import os

from dotenv import load_dotenv
from flask import Flask, Blueprint, jsonify
from pathlib import Path

from api.errors.operation_outcome import OperationOutcome
from engine import Engine
from engine.models import SAVE_PATH

api = Blueprint("api", __name__)
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
    except:
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
    "/beta/search/<database_name>/<resource_type>/<head_table>/<column_name>",
    methods=["GET"],
)
def betasearch(database_name, resource_type, head_table=None, column_name=None):
    """
    Return columns which have the desired resource type.
    """
    engine = engines[database_name]
    columns = engine.score(
        resource_type, parent_table=head_table, column_name=column_name
    )

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


@api.errorhandler(OperationOutcome)
def handle_bad_request(e):
    return str(e), 400


app = Flask(__name__)
app.register_blueprint(api)


if __name__ == "__main__":
    # Load .env config file for entire environement
    configFileName = (
        ".env.dev.custom" if os.path.exists(".env.dev.custom") else ".env.dev.default"
    )
    load_dotenv(dotenv_path=configFileName)

    # Start application
    app.run(debug=True, host="0.0.0.0")
