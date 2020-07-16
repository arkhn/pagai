from os import getenv

from pagai.services.database_explorer import POSTGRES, ORACLE, DatabaseExplorer
from pagai.errors import OperationOutcome
from sqlalchemy.exc import OperationalError
import json

def get_col_completion(owner, table_list):
    """
    TODO: add function description
    """

    # switch on the possible db models
    # if the db model is not supported, an error is raised.
    db_drivers = {"POSTGRES": POSTGRES, "ORACLE": ORACLE}


    credentials = {
        'model': 'ORACLE',
        'host': getenv('DB_HOST'),
        'port': int(getenv('DB_PORT', 1531)),
        'database': getenv('DB_NAME'),
        'login': getenv('DB_USER'),
        'password': getenv('DB_PASSWORD'),
    }
    print(credentials)

    db_model = "ORACLE"
    if db_model not in db_drivers:
        raise OperationOutcome(f"Database type {credentials.get('model')} is unknown")


    try:
        # Explore the Database
        explorer = DatabaseExplorer(db_drivers[db_model], credentials)
        # This returns the database schema, a dict of the form {"table_A": ["col1", "col2"], "table_B": ["col3", "col4"]}
        schema = explorer.get_db_schema(owner=owner, driver=ORACLE)
        col_completion = explorer.get_column_completion(db_schema=schema, tables=table_list)
        return col_completion
    except OperationalError as e:
        if "could not connect to server" in str(e):
            raise OperationOutcome(f"Could not connect to the database: {e}")
        else:
            raise OperationOutcome(e)
    except Exception as e:
        raise OperationOutcome(e)

test = get_col_completion(owner='V500', table_list=["UF", "MALADE"])

print(test)