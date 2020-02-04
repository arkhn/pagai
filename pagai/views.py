from flask import Blueprint, jsonify, request
from flask_cors import CORS
from pathlib import Path
import psycopg2

from pagai.errors import OperationOutcome
from pagai.engine import Engine
from pagai.engine.models import SAVE_PATH
from pagai.services import postgres
from pagai.services import pyrog

api = Blueprint("api", __name__)
# enable Cross-Origin Resource Sharing
# "Allow-Control-Allow-Origin" HTTP header
CORS(api)

engines = dict()


@api.route("/init/<database_name>", methods=["GET"])
@api.route("/init/<database_name>/<force_retrain>", methods=["GET"])
def init(database_name, force_retrain=False):
    """
    Endpoint for analysing database.
    Builds dependency graph, trains model and predicts classes.
    """

    try:
        engine = Engine(database_name)
        engines[database_name] = engine
        engine.initialise(force_retrain=force_retrain)
    except MemoryError as e:
        print(e)
        print("engine.init crashed beautifully")
        return "error", 500

    return jsonify({"response": "success"})


@api.route("/retrain/<database_name>", methods=["GET"])
def retrain(database_name):
    """
    Force build dependency graph, train model and predict classes.
    """

    try:
        engine = engines[database_name]
        engine.initialise(force_retrain=True)
    except Exception:
        print("retraining crashed beautifully")
        return "error", 500

    return jsonify({"response": "success"})


@api.route("/search/<database_name>/<resource_type>", methods=["GET"])
def search(database_name, resource_type):
    """
    Handle search calls by resource_type, keywords, etc.
    """
    engine = engines[database_name]
    columns = engine.score(resource_type)

    return jsonify(columns)


@api.route("/beta/search/<database_name>/<resource_type>", methods=["GET"])
@api.route("/beta/search/<database_name>/<resource_type>/<head_table>", methods=["GET"])
@api.route(
    "/beta/search/<database_name>/<resource_type>/<head_table>/<column_name>", methods=["GET"],
)
def betasearch(database_name, resource_type, head_table=None, column_name=None):
    """
    Return columns which have the desired resource type.
    """
    engine = engines[database_name]
    columns = engine.score(resource_type, parent_table=head_table, column_name=column_name)

    return jsonify(columns)


@api.route("/state/<database_name>", methods=["GET"])
def state(database_name):
    """
    Endpoint for retrieving the state of the data-base.
    Returns untrained or trained.
    """
    pickle_path = f"{SAVE_PATH}/{database_name}.pickle"
    pickle_file = Path(pickle_path)

    # Check if model is already trained
    if pickle_file.is_file():
        return jsonify({"status": "trained"})
    else:
        return jsonify({"status": "unknown or training"})


@api.route("/explore/<credential_id>/<table>", methods=["GET"])
def explore(credential_id, table):
    """
    Database exploration: returns the first rows of
    a database table. The db credentials are retrieved from
    Pyrog. The number of returned rows may be specified using
    query params (eg: /explore/<db>/<table>?first=10).
    """
    credentials = pyrog.get_credentials(credential_id)
    limit = request.args.get("first", 10, type=int)
    schema = request.args.get("schema")

    def explore_postgres():
        try:
            with postgres.from_pyrog_credentials(credentials) as connection:
                return postgres.explore(table, connection=connection, limit=limit, schema=schema)
        except psycopg2.OperationalError as e:
            raise OperationOutcome(f"Could not connect to the postgres database: {e}")

    # switch on the possible db models and process the exploration query in a dedicated handler
    # if the db model is not supported, an error is raised.
    db_handlers = {"POSTGRES": explore_postgres}
    try:
        handler = db_handlers[credentials.get("model")]
        results = handler()
    except KeyError:
        raise OperationOutcome(f"Database type {credentials.get('model')} is unknown")

    return jsonify(results)


@api.errorhandler(OperationOutcome)
def handle_bad_request(e):
    return jsonify({"error": str(e)}), 400
