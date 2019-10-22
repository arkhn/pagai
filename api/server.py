from dotenv import load_dotenv
from flask import Flask, Blueprint, request, jsonify
import json
import os
from pathlib import Path
import pickle
import yaml

from api.errors.operation_outcome import OperationOutcome
import engine


api = Blueprint('api', __name__)
SAVE_PATH = "build"

@api.route("/init/<database_name>/<force_retrain>", methods=['GET'])
def init(database_name, force_retrain=False):
    """
    Endpoint for analysing database.
    Builds dependency graph, trains model and predicts classes.
    """

    try:
        engine.init(database_name, force_retrain=force_retrain)
    except:
        print("engine.init crashed beautifully")
        return "error", 500

    return "success", 200


@api.route("/retrain/<database_name>", methods=['GET'])
def retrain(database_name, force_retrain=False):
    """
    Force build dependency graph, train model and predict classes.
    """

    try:
        engine.init(database_name, force_retrain=True)
    except:
        print("retraining crashed beautifully")
        return "error", 500

    return "success", 200


@api.route("/state/<database_name>",methods=['GET'])
def state(database_name):
    """
    Endpoint for retrieving the state of the data-base.
    Returns untrained or trained.
    """
    pickle_path = f"{SAVE_PATH}/{database_name}.pickle"
    pickle_file = Path(pickle_path)

    # Check if model is already trained
    if pickle_file.is_file():
        print("trained model")
        return "trained"
    else:
        print("untrained model or unknown database")
        return "untrained or unknown database name"



@api.errorhandler(OperationOutcome)
def handle_bad_request(e):
    return str(e), 400


app = Flask(__name__)
app.register_blueprint(api)


if __name__ == '__main__':
    # Load .env config file for entire environement
    configFileName = './.env.dev.custom' if os.path.exists('./.env.dev.custom') else './.env.dev.default'
    load_dotenv(dotenv_path=configFileName)

    # Start application
    app.run(debug=True, host='0.0.0.0')
