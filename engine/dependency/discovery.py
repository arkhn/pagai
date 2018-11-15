import re
import time

import psycopg2

from engine.loader import sql


class Discovery:
    def __init__(self, database, owner):
        self.database = database
        self.owner = owner
        self.sql_params = sql.get_sql_config('prod_database')
        self.id_like_columns_tables = {}

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

        table_rows = sql.get_table(table, connection)
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

        left_len = sql.get_length(left_table, connection)
        right_len = sql.get_length(right_table, connection)

        left_column_data = sql.get_column(left_table, left_column, connection)

        for right_column, column_type in right_columns:
            if column_type == join_datatype:
                right_column_data = sql.get_column(right_table, right_column, connection)

                for left_el in left_column_data:
                    if left_el in right_column_data:
                        return True

        return False

    def find_compatible_tables(self, table, id_column, id_column_type, connection):
        """
        Return the names of the tables which could be joined on table.id_column
        """
        compatible_tables = []
        tables = [t for t in sql.get_tables(connection) if t != table]
        for right_table in tables:
            # print('\t{}'.format(right_table))
            if self.is_table_compatible(table, id_column, right_table, id_column_type, connection):
                compatible_tables.append(right_table)

        return compatible_tables

    def find_joinable_tables(self, table, connection):
        """
        Return the names of the table which could be in a join with the given table
        """
        id_columns = self.find_id_like_columns(table, connection)
        print([id_column for id_column, id_column_type in id_columns])
        joinable_tables = {}
        for id_column, id_column_type in id_columns:
            print('#COL_ID ', id_column)
            print(list(sql.get_column(table, id_column, connection))[:10])
            compatible_tables = self.find_compatible_tables(table, id_column, id_column_type, connection)
            for compatible_table in compatible_tables:
                if compatible_table not in joinable_tables:
                    joinable_tables[compatible_table] = []
                joinable_tables[compatible_table].append(id_column)

        return joinable_tables

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
                graph[table_id] = joinable_tables  # [self.table_id(owner, table) for table in joinable_tables]

        return graph

    @staticmethod
    def table_id(owner, table):
        return '{}:{}'.format(owner, table)



