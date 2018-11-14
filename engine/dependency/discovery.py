import re
import time

import numpy as np
import psycopg2

from engine.loader import sql


class Discovery():
    def __init__(self, database, owner):
        self.database = database
        self.owner = owner
        self.sql_params = sql.get_sql_config('prod_database')
        self.id_like_columns_tables = {}
        self.table_length = {}
        self.table_store = {}
        self.table_column_id_store = {}

    def retrieve_table(self, table, connection):
        if table not in self.table_store:
            query = 'SELECT * FROM {} LIMIT 1000;'.format(table)
            results = sql.run(query, connection)
            results = np.array(results)
            self.table_store[table] = results

        return self.table_store[table]

    def retrieve_length(self, table, connection):
        if table not in self.table_length:
            self.table_length[table] = sql.get_length(table, connection)

        return self.table_length[table]

    def is_id_like_column(self, column):
        id_like = False
        if all(isinstance(el, int) for el in column):
            id_like = True
        elif all(isinstance(el, str) for el in column):
            if all(re.search(r'^\w+$', el) is not None for el in column):
                unique_values = list(set(column))
                if len(unique_values) < 40:  # TODO: what value?
                    id_like = True

        return id_like

    def find_id_like_columns(self, table, connection):
        """
        Given a table, return all columns that could be id columns for a join
        """
        if table in self.id_like_columns_tables:
            return self.id_like_columns_tables[table]

        id_like_columns = []

        column_infos = sql.get_columns(table, connection, include_data_type=True)
        column_names = []
        column_types = []
        for column_name, column_type in column_infos:
            column_names.append(column_name)
            column_types.append(column_type)

        table_rows = self.retrieve_table(table, connection)
        columns = table_rows.T
        for column_name, column_type, column in zip(column_names, column_types, columns):
            if self.is_id_like_column(column):
                id_like_columns.append((column_name, column_type))

        self.id_like_columns_tables[table] = id_like_columns
        return id_like_columns

    def is_table_compatible(self, left_table, left_column, right_table, join_datatype, connection):
        """
        State whether right_table could be join with left_table on left_table.id_column
        """
        right_columns = self.find_id_like_columns(right_table, connection)

        left_len = self.retrieve_length(left_table, connection)
        right_len = self.retrieve_length(right_table, connection)

        for right_column, column_type in right_columns:
            if column_type == join_datatype:
                left_table_name = left_table
                right_table_name = right_table
                with_statements = []
                if left_len > 10000:
                    limit = np.round(left_len**(2/3))
                    q = " short_left_table AS (" \
                        "SELECT {} " \
                        "FROM {} " \
                        "ORDER BY RANDOM() LIMIT 10000 " \
                        ")".format(left_column, left_table, limit)
                    left_table_name = 'short_left_table'
                    with_statements.append(q)

                if right_len > 10000:
                    limit = np.round(right_len**(2/3))
                    q = " right_table_name AS (" \
                        "SELECT {} " \
                        "FROM {} " \
                        "ORDER BY RANDOM() LIMIT 10000 " \
                        ")".format(right_column, right_table, limit)
                    right_table_name = 'right_table_name'
                    with_statements.append(q)

                if len(with_statements) > 0:
                    with_statement = 'WITH' + ', '.join(with_statements) + ' '
                else:
                    with_statement = ''

                args = (
                    left_table_name, right_table_name,
                    left_table_name, left_column,
                    right_table_name, right_column
                )

                inner_join_query = with_statement + \
                                   "SELECT COUNT(*) " \
                                   "FROM {} " \
                                   "INNER JOIN {} " \
                                   "ON {}.{} = {}.{};".format(*args)

                result = sql.run(inner_join_query, connection)
                join_size = result[0][0]
                if join_size > 0:
                    # print(left_table, left_column, right_table, right_column, join_size)
                    return True
        return False

    def find_compatible_tables(self, table, id_column, id_column_type, connection):
        """
        Return the names of the tables which could be joined on table.id_column
        """
        compatible_tables = []
        tables = [t for t in sql.get_tables(connection) if t != table]
        for right_table in tables:
            print('\t{}'.format(right_table))
            if self.is_table_compatible(table, id_column, right_table, id_column_type, connection):
                compatible_tables.append(right_table)

        return compatible_tables

    def find_joinable_tables(self, table, connection):
        """
        Return the names of the table which could be in a join with the given table
        """
        id_columns = self.find_id_like_columns(table, connection)
        joinable_tables = []
        for id_column, id_column_type in id_columns:
            tables = self.find_compatible_tables(table, id_column, id_column_type, connection)
            joinable_tables += tables

        return sorted(list(set(joinable_tables)))

    def build_dependency_graph(self):
        with psycopg2.connect(**self.sql_params) as connection:
            owner = self.owner
            tables = sql.get_tables(connection)
            graph = {}
            start_time = time.time()
            for table in tables:
                table = str(table)
                print('*********************  {}   {}\n'.format(table, time.time() - start_time))
                table_id = self.table_id(owner, table)
                joinable_tables = self.find_joinable_tables(table, connection)
                graph[table_id] = [self.table_id(owner, table) for table in joinable_tables]

        return graph

    @staticmethod
    def table_id(owner, table):
        return '{}:{}'.format(owner, table)



