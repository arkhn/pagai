from dotenv import load_dotenv
from flask import Flask, Blueprint, request, jsonify
import json
import os
import yaml

from api.errors.operation_outcome import OperationOutcome
import engine


api = Blueprint('api', __name__)


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
def retrain(database_name):
    """
    Force build dependency graph, train model and predict classes.
    """

    try:
        engine.init(database_name, force_retrain=True)
    except:
        print("retraining crashed beautifully")
        return "error", 500

    return "success", 200


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
