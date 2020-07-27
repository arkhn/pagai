from os import getenv

from pagai.services.database_explorer import POSTGRES, ORACLE, DatabaseExplorer
from pagai.errors import OperationOutcome
from sqlalchemy.exc import OperationalError
import json

def get_col_completion(owner, table_name, sorted):
    """
    Returns the percentage of completion for all columns in the given table
    """

    # switch on the possible db models
    # if the db model is not supported, an error is raised.
    db_drivers = {"POSTGRES": POSTGRES, "ORACLE": ORACLE}

    with open("/home/arkhn/git/pagai/schema.json") as f:
        schema=json.load(f)
    credentials = {
        'model': 'ORACLE',
        'host': getenv('DB_HOST'),
        'port': int(getenv('DB_PORT', 1531)),
        'database': getenv('DB_NAME'),
        'login': getenv('DB_USER'),
        'password': getenv('DB_PASSWORD'),
    }

    db_model = "ORACLE"
    if db_model not in db_drivers:
        raise OperationOutcome(f"Database type {credentials.get('model')} is unknown")

    result_display = ""
    try:
        explorer = DatabaseExplorer(db_drivers[db_model], credentials)
        col_completion = explorer.get_column_completion(db_schema=schema, table=table_name, sort=sorted)
        
        # Format CSV friendly
        for item in col_completion: 
            result_display += f"{item[0]}, {item[1]} \n" 
        return result_display
    except OperationalError as e:
        if "could not connect to server" in str(e):
            raise OperationOutcome(f"Could not connect to the database: {e}")
        else:
            raise OperationOutcome(e)
    except Exception as e:
        raise OperationOutcome(e)


test = get_col_completion(owner='V500', table_name="UF", sorted= True)