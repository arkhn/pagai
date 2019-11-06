from tqdm import tqdm
import os
import re

from pagai.engine.structure import Graph
from pagai.services.postgres import (
    get_table_names,
    get_column_names,
    get_table,
    get_column,
)


EXCLUDE_COLUMNS = ["id", "row_id"]


class DependencyGraphBuilder:
    def __init__(self):
        self.id_like_columns_tables = {}

    def is_id_like_column(self, column):
        """
        Determines whether a given column looks
        like a primary key.
        """

        # Check if all values are integers
        if all(isinstance(el, int) for el in column):
            # Check if all numbers are not in [0, 1]
            if not all(el in [0, 1] for el in set(column)):
                return True
        # Check if all values are strings
        elif all(isinstance(el, str) for el in column):
            # Check if strings are "letters and numbers only"
            if all(re.search(r"^\w+$", el) is not None for el in column):
                # TODO: Check ratio of unique values
                unique_values = list(set(column))
                if len(unique_values) < int(os.getenv("STR_ID_MAX_UNIQUE_VALUES")):
                    return True

        return False

    def find_id_like_columns(self, table, connection):
        """
        Given a table, return all columns that could be id columns for a join
        """
        if table in self.id_like_columns_tables:
            return self.id_like_columns_tables[table]

        id_like_columns = []

        column_infos = get_column_names(
            table, connection=connection, include_data_type=True
        )
        column_names = []
        column_types = []
        for column_name, column_type in column_infos:
            column_names.append(column_name)
            column_types.append(column_type)

        table_rows = get_table(table, connection)
        columns = table_rows.T
        for column_name, column_type, column in zip(
            column_names, column_types, columns
        ):
            # Column should be "like" a primary key or id column, but not in some forbidden columns
            if self.is_id_like_column(column) and column_name not in EXCLUDE_COLUMNS:
                id_like_columns.append((column_name, column_type))

        self.id_like_columns_tables[table] = id_like_columns

        return id_like_columns

    def table_compatibility(
        self, left_table, left_column, right_table, join_datatype, connection
    ):
        """
        State whether right_table could be join with left_table on left_table.left_column
        """

        include_threshold = float(os.getenv("INCLUDE_THRESHOLD"))

        right_columns = self.find_id_like_columns(right_table, connection)

        left_column_data = get_column(connection, left_table, left_column)

        acceptable_right_columns = []
        for right_column, column_type in right_columns:
            # Check left_column and right_column have same type
            if column_type == join_datatype:
                right_column_data = get_column(right_table, right_column, connection)

                n_included_el = 0
                for left_el in left_column_data:
                    if left_el in right_column_data:
                        n_included_el += 1
                        # Possibly break out of loop
                        # if include_threshold is reached
                        # (for performance)
                        include_ratio = n_included_el / len(left_column_data)
                        if include_ratio >= include_threshold:
                            break

                include_ratio = n_included_el / len(left_column_data)
                if include_ratio >= include_threshold:
                    acceptable_right_columns.append(right_column)

        return acceptable_right_columns

    def find_compatible_tables(self, table, id_column, id_column_type, connection):
        """
        Return the names of the tables which could be joined on table.id_column
        """
        compatible_tables = []
        tables = [t for t in get_table_names(connection=connection) if t != table]

        for right_table in tqdm(tables):
            # print('\t{}'.format(right_table))
            acceptable_right_columns = self.table_compatibility(
                table, id_column, right_table, id_column_type, connection
            )
            if len(acceptable_right_columns) > 0:
                compatible_tables.append((right_table, acceptable_right_columns))

        return compatible_tables

    def find_joinable_tables(self, table, connection):
        """
        Return the names of the table which could be in a join with the given table
        """
        id_columns = self.find_id_like_columns(table, connection)
        joinable_tables = {}

        for id_column, id_column_type in tqdm(id_columns):
            compatible_tables = self.find_compatible_tables(
                table, id_column, id_column_type, connection
            )
            if len(compatible_tables) > 0:
                joinable_tables[id_column] = compatible_tables

        return joinable_tables

    def build_dependency_graph(self, connection):
        graph = Graph()
        tables = get_table_names(connection=connection)
        for table in tables:
            graph.add_table(table)

        for table in tqdm(tables):
            tqdm.write(f"Computing joinable tables for {table}...")
            joinable_tables = self.find_joinable_tables(table, connection)

            tqdm.write("Add results to graph")
            for id_column, join_data in joinable_tables.items():
                for joinable_table, joinable_columns in join_data:
                    for joinable_column in joinable_columns:
                        join_info = (id_column, joinable_column)
                        graph.add_join(table, joinable_table, join_info)

        return graph
