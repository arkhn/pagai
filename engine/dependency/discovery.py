import re

import numpy as np

from engine.loader import sql


class Discovery():
    def __init__(self, database, owner):
        self.database = database
        self.owner = owner
        # self.connection = sql.get_connection(database)

    def find_id_like_columns(self, table):
        """
        Given a table, return all columns that could be id columns for a join
        """
        id_like_columns = []

        column_infos = sql.get_columns(table, include_data_type=True)
        column_names = []
        column_types = []
        for column_name, column_type in column_infos:
            column_names.append(column_name)
            column_types.append(column_type)

        query = 'SELECT * FROM {} LIMIT 1000;'.format(table)
        results = sql.run(query)
        columns = np.array(results).T
        for column_name, column_type, column in zip(column_names, column_types, columns):
            id_like = False
            if all(isinstance(el, int) for el in column):
                id_like = True
            elif all(isinstance(el, str) for el in column):
                if all(re.search(r'^\w+$', el) is not None for el in column):
                    id_like = True

            if id_like:
                id_like_columns.append((column_name, column_type))

        return id_like_columns

    def is_table_compatible(self, left_table, left_column, right_table, join_datatype):
        """
        State whether right_table could be join with left_table on left_table.id_column
        """
        right_columns = sql.get_columns(right_table, include_data_type=True)
        for right_column, column_type in right_columns:
            if column_type == join_datatype:
                args = (
                    left_table, right_table,
                    left_table, left_column,
                    right_table, right_column
                )
                inner_join_query = "SELECT COUNT(*) " \
                                   "FROM {} " \
                                   "INNER JOIN {} " \
                                   "ON {}.{} = {}.{};".format(*args)
                result = sql.run(inner_join_query)
                join_size = result[0][0]
                if join_size > 0:
                    print(left_table, left_column,
                    right_table, right_column, join_size
                    )
                    return True
        return False

    def find_compatible_tables(self, table, id_column, id_column_type):
        """
        Return the names of the tables which could be joined on table.id_column
        """
        compatible_tables = []
        tables = [t for t in sql.get_tables() if t != table]
        for right_table in tables:
            if self.is_table_compatible(table, id_column, right_table, id_column_type):
                compatible_tables.append(right_table)

        return compatible_tables

    def find_joinable_tables(self, table):
        """
        Return the names of the table which could be in a join with the given table
        """
        id_columns = self.find_id_like_columns(table)
        joinable_tables = []
        for id_column, id_column_type in id_columns:
            tables = self.find_compatible_tables(table, id_column, id_column_type)
            joinable_tables += tables

        return sorted(list(set(joinable_tables)))

    def build_dependency_graph(self):
        owner = self.owner
        tables = sql.get_tables()
        graph = {}
        for table in tables:
            table_id = self.table_id(owner, table)
            joinable_tables = self.find_joinable_tables(table)
            graph[table_id] = [self.table_id(owner, table) for table in joinable_tables]

        return graph

    @staticmethod
    def table_id(owner, table):
        return '{}:{}'.format(owner, table)



