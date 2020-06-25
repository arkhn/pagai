from typing import Dict
import time
from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import InvalidRequestError, NoSuchTableError
from collections import defaultdict

from pagai.errors import OperationOutcome

POSTGRES = "postgresql"
ORACLE = "oracle+cx_oracle"

SQL_RELATIONS_TO_METHOD = {
    "<": "__lt__",
    "<=": "__le__",
    "<>": "__ne__",
    "=": "__eq__",
    ">": "__gt__",
    ">=": "__ge__",
    # not handled yet
    # "BETWEEN": "",
    "IN": "in_",
    "LIKE": "like",
}


def get_sql_url(db_handler: str, sql_config: Dict) -> str:
    return (
        f'{db_handler}://{sql_config["login"]}:{sql_config["password"]}'
        f'@{sql_config["host"]}:{sql_config["port"]}'
        f'/{sql_config["database"]}'
    )


def table_exists(sql_engine, table_name):
    try:
        # Don't use Sqlalchemy Inspector as it uses reflection and it takes a very long time on Oracle.
        metadata = MetaData(bind=sql_engine)
        _ = Table(table_name, metadata, autoload=True)
        return True
    except NoSuchTableError:
        return False


class DatabaseExplorer:
    def __init__(self, driver_name: str, db_config: Dict):
        self._db_config = db_config
        self._sql_engine = create_engine(get_sql_url(driver_name, db_config))
        self._metadata = MetaData(bind=self._sql_engine)

    def _get_sql_table(self, table: str, schema: str = None):
        if not schema:
            schema = None
        table = table.strip()
        return Table(table, self._metadata, schema=schema, autoload=True)

    def get_column_names(self, table: str, schema: str = None):
        """
        Return column names of a table
        """
        table = self._get_sql_table(table, schema)
        return [column.name for column in table.c]

    def get_column(self, column: str, table: str, schema: str = None):
        """
        Return column names of a table
        """
        table = self._get_sql_table(table, schema)
        return table.c[column]

    def get_table(self, table: str, schema=None, limit=1000, filters=[]):
        """
        Return content of a table with a limit
        """
        table = self._get_sql_table(table, schema)
        select = table.select()

        # Add filtering if any
        for filter_ in filters:
            col = self.get_column(
                filter_["sqlColumn"]["column"], filter_["sqlColumn"]["table"], schema
            )
            rel_method = SQL_RELATIONS_TO_METHOD[filter_["relation"]]
            select = select.where(getattr(col, rel_method)(filter_["value"]))

        # Return as JSON serializable object
        return [[col for col in row] for row in self._sql_engine.execute(select.limit(limit))]

    def explore(self, table: str, limit, schema=None, filters=[]):
        """
        Returns the first rows of a table alongside the column names.
        """
        try:
            return {
                "fields": self.get_column_names(table, schema=schema),
                "rows": self.get_table(table, schema=schema, limit=limit, filters=filters),
            }
        except InvalidRequestError as e:
            if "requested table(s) not available" in str(e):
                raise OperationOutcome(f"Table {table} does not exist in database")
            else:
                raise OperationOutcome(e)
        except Exception as e:
            raise OperationOutcome(e)

    def get_owners(self, driver=POSTGRES):
        """
        Returns all owners of a database.
        """
        if driver == POSTGRES:
            sql_query = text(f"select schema_name as owners from information_schema.schemata;")
        else:
            sql_query = text(f"select username as owners from all_users")

        with self._sql_engine.connect() as connection:
            result = connection.execute(sql_query).fetchall()
        return [r["owners"] for r in result]

    def get_db_schema(self, owner: str, driver=POSTGRES):
        """
        Returns the database schema for one owner of a database, as required by Pyrog
        """
        db_schema = defaultdict(lambda: [])

        if driver == POSTGRES:
            sql_query = text(
                f"select table_name, column_name from information_schema.columns where table_schema='{owner}';"
            )
        else:
            sql_query = text(
                f"select table_name, column_name from all_tab_columns where owner='{owner}'"
            )

        with self._sql_engine.connect() as connection:
            result = connection.execute(sql_query).fetchall()
            for row in result:
                db_schema[row["table_name"]].append(row["column_name"])
        return db_schema
