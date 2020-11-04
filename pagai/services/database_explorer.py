from typing import Dict, Optional, Callable

from sqlalchemy import (
    and_,
    Column,
    create_engine,
    MetaData,
    Table,
    text,
)
from sqlalchemy.exc import InvalidRequestError, NoSuchColumnError, NoSuchTableError
from collections import defaultdict

from pagai.errors import OperationOutcome


def handle_between_filter(col, value):
    values = value.split(",")
    if len(values) != 2:
        raise ValueError("BETWEEN filter expects 2 values separated by a comma.")
    min_val = values[0].strip()
    max_val = values[1].strip()
    return and_(col.__ge__(min_val), col.__le__(max_val))


SQL_RELATIONS_TO_METHOD: Dict[str, Callable[[Column, str], Callable]] = {
    "<": lambda col, value: col.__lt__(value),
    "<=": lambda col, value: col.__le__(value),
    "<>": lambda col, value: col.__ne__(value),
    "=": lambda col, value: col.__eq__(value),
    ">": lambda col, value: col.__gt__(value),
    ">=": lambda col, value: col.__ge__(value),
    "BETWEEN": handle_between_filter,
    "IN": lambda col, value: col.in_(value.split(",")),
    "LIKE": lambda col, value: col.like(value),
}

MSSQL = "MSSQL"
ORACLE = "ORACLE"
POSTGRES = "POSTGRES"
DB_DRIVERS = {POSTGRES: "postgresql", ORACLE: "oracle+cx_oracle", MSSQL: "mssql+pyodbc"}
URL_SUFFIXES = {
    POSTGRES: "",
    ORACLE: "",
    # the param MARS_Connection=Yes solves the following issue:
    # https://github.com/catherinedevlin/ipython-sql/issues/54
    MSSQL: "?driver=ODBC+Driver+17+for+SQL+Server&MARS_Connection=Yes",
}


def get_sql_url(db_model: str, sql_config: dict) -> str:
    return (
        f"{DB_DRIVERS[db_model]}://{sql_config['login']}:{sql_config['password']}"
        f"@{sql_config['host']}:{sql_config['port']}"
        f"/{sql_config['database']}{URL_SUFFIXES[db_model]}"
    )


def table_exists(sql_engine, table_name):
    try:
        # Don't use Sqlalchemy Inspector as it uses reflection and
        # it takes a very long time on Oracle.
        metadata = MetaData(bind=sql_engine)
        return True, Table(table_name, metadata, autoload=True)
    except NoSuchTableError:
        return False, None


def get_col_from_row_result(row, col):
    try:
        return row[col]
    except NoSuchColumnError:
        # If column is not found it may be because the column names are case
        # insensitive. If so, col can be in upper case (what oracle considers
        # as case insensitive) but the keys in row are in lower case
        # (what sqlalchemy considers as case insensitive).
        return row[col.lower()]


class DatabaseExplorer:
    def __init__(self, db_config: Optional[dict] = None):
        self.db_schema = None
        self._sql_engine = None
        self._prev_db_config = None

        if db_config:
            self.update_connection(db_config)

    def update_connection(self, db_config: dict):
        # If the config hasn't been changed, don't do anything
        if db_config == self._prev_db_config:
            return

        # Otherwise we update the DatabaseExplorer attributes
        self._prev_db_config = db_config

        self._db_model = db_config.get("model")
        if self._db_model not in DB_DRIVERS:
            raise OperationOutcome(f"Database type {self._db_model} is unknown")

        self._db_config = db_config
        self._sql_engine = create_engine(get_sql_url(self._db_model, db_config))
        self._metadata = MetaData(bind=self._sql_engine)

    def check_connection_exists(self):
        if not self._sql_engine:
            raise OperationOutcome("DatabaseExplorer was not provided with any credentials.")

    def get_sql_alchemy_table(self, table: str, schema: Optional[str] = None):
        if not schema:
            # Pyrog passes an empty string when there is no schema
            schema = None
        return Table(table.strip(), self._metadata, schema=schema, autoload=True)

    def get_sql_alchemy_column(self, column: str, table: str, schema: str = None):
        """
        Return column names of a table
        """
        table = self.get_sql_alchemy_table(table, schema)
        try:
            return table.c[column]
        except KeyError:
            # If column is not in table.c it may be because the column names
            # are case insensitive. If so, the schema can be in upper case
            # (what oracle considers as case insensitive) but the keys
            # in table.c are in lower case (what sqlalchemy considers
            # as case insensitive).
            return table.c[column.lower()]

    def get_table_rows(self, table_name: str, schema=None, limit=100, filters=[]):
        """
        Return content of a table with a limit
        """
        table = self.get_sql_alchemy_table(table_name, schema)
        select = table.select()

        # Add filtering if any
        for filter_ in filters:
            col = self.get_sql_alchemy_column(
                filter_["sqlColumn"]["column"], filter_["sqlColumn"]["table"], schema
            )
            filter_clause = SQL_RELATIONS_TO_METHOD[filter_["relation"]](col, filter_["value"])
            select = select.where(filter_clause)

        # Get column names with the right casing
        if self.db_schema is None:
            self.get_db_schema(schema)
        columns_names = self.db_schema[table_name]

        # Return as JSON serializable object
        return {
            "fields": columns_names,
            "rows": [
                [get_col_from_row_result(row, col) for col in columns_names]
                for row in self._sql_engine.execute(select.limit(limit))
            ],
        }

    def explore(self, table_name: str, schema: str, limit: int, filters=[]):
        """
        Returns the first rows of a table alongside the column names.
        """
        self.check_connection_exists()

        try:
            return self.get_table_rows(
                table_name=table_name, schema=schema, limit=limit, filters=filters
            )
        except InvalidRequestError as e:
            if "requested table(s) not available" in str(e):
                raise OperationOutcome(f"Table {table_name} does not exist in database")
            else:
                raise OperationOutcome(e)
        except Exception as e:
            raise OperationOutcome(e)

    def get_owners(self):
        """
        Returns all owners of a database.
        """
        self.check_connection_exists()

        if self._db_model == ORACLE:
            sql_query = text("select username as owners from all_users")
        else:  # POSTGRES AND MSSQL
            sql_query = text("select schema_name as owners from information_schema.schemata;")

        with self._sql_engine.connect() as connection:
            result = connection.execute(sql_query).fetchall()
        return [r["owners"] for r in result]

    def get_db_schema(self, owner: str):
        """
        Returns the database schema for one owner of a database,
        as required by Pyrog.
        """
        self.check_connection_exists()
        self.db_schema = defaultdict(list)

        if self._db_model == ORACLE:
            sql_query = text(
                f"select table_name, column_name from all_tab_columns where owner='{owner}'"
            )
        else:  # POSTGRES AND MSSQL
            sql_query = text(
                f"select table_name, column_name from information_schema.columns "
                f"where table_schema='{owner}';"
            )

        with self._sql_engine.connect() as connection:
            result = connection.execute(sql_query).fetchall()
            for row in result:
                self.db_schema[row["table_name"]].append(row["column_name"])

        return self.db_schema
