from flask import Blueprint, jsonify, request, g
from flask_cors import CORS
from sqlalchemy.exc import OperationalError

from pagai.errors import OperationOutcome
from pagai.services import pyrog
from pagai.services.database_explorer import DatabaseExplorer

api = Blueprint("api", __name__)
# enable Cross-Origin Resource Sharing
# "Allow-Control-Allow-Origin" HTTP header
CORS(api)

engines = dict()


def get_pyrog_client():
    if "pyrog_client" not in g:
        g.pyrog_client = pyrog.PyrogClient()
    return g.pyrog_client


@api.route("/explore/<resource_id>/<table>", methods=["GET"])
def explore(resource_id, table):
    """
    Database exploration: returns the first rows of
    a database table. The db credentials are retrieved from
    Pyrog. The number of returned rows may be specified using
    query params (eg: /explore/<resource_id>/<table>?first=10).
    """
    limit = request.args.get("first", 10, type=int)

    resource = get_pyrog_client().get_resource(resource_id)

    # Get credentials
    if not resource["source"]["credential"]:
        raise OperationOutcome("credentialId is required to explore the DB.")

    credentials = resource["source"]["credential"]

    # Get filters
    filters = resource["filters"]

    try:
        explorer = DatabaseExplorer(credentials)
        return jsonify(
            explorer.explore(table, limit=limit, schema=credentials.get("owner"), filters=filters)
        )
    except OperationalError as e:
        if "could not connect to server" in str(e):
            raise OperationOutcome(f"Could not connect to the database: {e}")
        else:
            raise OperationOutcome(e)
    except Exception as e:
        raise OperationOutcome(e)


@api.route("/get_owners", methods=["POST"])
def get_owners():
    credentials = request.get_json()

    try:
        explorer = DatabaseExplorer(credentials)
        db_owners = explorer.get_owners()
        return jsonify(db_owners)
    except OperationalError as e:
        if "could not connect to server" in str(e):
            raise OperationOutcome(f"Could not connect to the database: {e}")
        else:
            raise OperationOutcome(e)
    except Exception as e:
        raise OperationOutcome(e)


@api.route("/get_db_schema", methods=["POST"])
def get_db_schema():

    credentials = request.get_json()
    owner = credentials.get("owner")
    if not owner:
        raise OperationOutcome("Database owner is required")

    try:
        explorer = DatabaseExplorer(credentials)
        db_schema = explorer.get_db_schema(owner)
        return jsonify(db_schema)
    except OperationalError as e:
        if "could not connect to server" in str(e):
            raise OperationOutcome(f"Could not connect to the database: {e}")
        else:
            raise OperationOutcome(e)
    except Exception as e:
        raise OperationOutcome(e)


@api.errorhandler(OperationOutcome)
def handle_bad_request(e):
    return jsonify({"error": str(e)}), 400
