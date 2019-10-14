from flask_restful import Resource, reqparse
import logging
import os
import pickle
import psycopg2
import requests
import yaml
from pathlib import Path

from api.common.utils import file_response
from api.db.client import connect, closeConnection
from engine.dependency.discovery import Discovery
from engine.query import Query
from engine.models.train import build_training_set, train, spec_from_source
from engine.models.predict import classify
from engine.models.train import train
from engine import models
from engine.structure import Column, Graph
import api.loader as loader


ENCODING = 'utf-8'
SAVE_PATH = "build/"


# database = 'mimic' # 'CW'
# owner = 'ICSF'
query = Query()
# query.load()

train_db_params = None
prod_db_params = None
configFileName = './conf.custom.yml' if os.path.exists('./conf.custom.yml') else './conf.default.yml'

with open(configFileName) as configFile:
    try:
        # Load config
        config = yaml.safe_load(configFile)
        print(config)
        train_db_params = config["staging_db"]
        prod_db_params = config["staging_db"]
        print(train_db_params)
    except yaml.YAMLError as error:
        print(error)


class Init(Resource):
    @staticmethod
    def build_datasets(connection):
        # Get all table.column names
        table_names = loader.get_table_names(connection=connection)
        table_column_names = [
            "{}.{}".format(table_name, column_name)
            for table_name in table_names
            for column_name in loader.get_column_names(table_name, connection=connection)
        ]

        # Format to fit the transform and predict model pipeline
        # TODO: have a specific canal
        sources = [
            ("unknown", table_column, 1) for table_column in table_column_names
        ]
        datasets, _labels = spec_from_source(sources)

        return datasets, _labels

    @staticmethod
    def get(database_name, force_retrain=False):
        query_path = f"{SAVE_PATH}{database_name}.pickle"
        query_pickle = Path(query_path)

        # Check model isn't already trained
        if query_pickle.is_file() and not force_retrain:
            logging.warning("Model ready!")

            return "trained"
        else:
            with psycopg2.connect(**prod_db_params) as connection:
                logging.warning("Building engine...")
                # Load discovery module to build dependency graph
                discovery = Discovery()
                logging.warning("Building the dependency graph...")
                dependency_graph = discovery.build_dependency_graph(connection)
                # dependency_graph = Graph()

                # Train models
                model_types = ["ngram"]
                models = dict()
                classifications = dict()

                logging.warning("Load training sets...")
                columns, labels = None, None

                # Build training set
                with psycopg2.connect(**train_db_params) as connection:
                    columns, labels = build_training_set(connection)

                # Fetch all rows to be classified
                datasets, _labels = Init.build_datasets(connection)
                columns = loader.fetch_columns(connection, datasets, dataset_size=100)

                # Train and classify each model
                for model_type in model_types:
                    # Train
                    logging.warning(f"Training {model_type}...")
                    model = train(columns, labels, model_type)
                    models[model_type] = model
                    # Classify
                    classification = classify(model, columns, _labels)
                    classifications[model_type] = classification

                # Store dependency graph, trained models, and predictions
                if not os.path.exists(os.path.dirname(query_path)):
                    os.makedirs(os.path.dirname(query_path))
                with open(query_path, "wb") as file:
                    pickle.dump({
                        "dependency_graph": dependency_graph,
                        "models": models,
                        "classifications": classifications
                    }, file)

            return "success"


class BetaSearch(Resource):
    @staticmethod
    def get(resource_type='all', head_table=None, column_name=None):
        """Return columns which have the desired resource type"""

        columns = query.find(resource_type, parent_table=head_table, column_name=column_name)

        return columns

      
class Search(Resource):
    """
    Handle search calls by resource_type, keywords, etc.
    """
    @staticmethod
    def post():
        # TODO: todo
        parser = reqparse.RequestParser()
        parser.add_argument('resource_type', required=False, type=str)
        parser.add_argument('owner', required=True, type=str)
        args = parser.parse_args()

    @staticmethod
    def get(resource_type):
        """Return columns which have the desired resource type"""

        columns = query.find(resource_type)

        return columns