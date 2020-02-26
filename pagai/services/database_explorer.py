from typing import Dict

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import InvalidRequestError, NoSuchTableError

from pagai.errors import OperationOutcome

POSTGRES = "postgresql"
ORACLE = "oracle+cx_oracle"


def get_sql_url(db_handler: str, sql_config: Dict) -> str:
    return (
        f'{db_handler}://{sql_config["login"]}:{sql_config["password"]}@{sql_config["host"]}:{sql_config["port"]}'
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
        return Table(table, self._metadata, schema=schema, autoload=True)

    def get_column_names(self, table: str, schema: str = None):
        """
        Return column names of a table
        """
        table = self._get_sql_table(table, schema)
        return [column.name for column in table.c]

    def get_table(self, table: str, limit=1000, schema=None):
        """
        Return content of a table with a limit
        """
        table = self._get_sql_table(table, schema)
        return list(self._sql_engine.execute(table.select().limit(limit)))

    def explore(self, table: str, limit, schema=None):
        """
        Returns the first rows of a table alongside the column names.
        """
        try:
            return {
                "fields": self.get_column_names(table, schema=schema),
                "rows": self.get_table(table, schema=schema, limit=limit),
            }
        except InvalidRequestError as e:
            if 'requested table(s) not available' in str(e):
                raise OperationOutcome(f"Table {table} does not exist in database")
            else:
                raise OperationOutcome(e)
        except Exception as e:
            raise OperationOutcome(e)
