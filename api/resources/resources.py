from flask_restful import Resource, reqparse
import os
import psycopg2
import requests
import yaml

from api.common.utils import file_response
from api.db.client import connect, closeConnection
from engine.query import Query
from engine.models.train import build_training_set, train, spec_from_source
from engine.models.predict import classify
from engine.structure import Column
import api.loader as loader


ENCODING = 'utf-8'

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


class Analysis(Resource):
    @staticmethod
    def build_datasets(connection):
        # Get all table.column names
        table_names = loader.get_table_names(connection)
        table_column_names = [
            "{}.{}".format(table_name, column_name)
            for table_name in table_names
            for column_name in loader.get_column_names(connection, table_name)
        ]

        # Format to fit the transform and predict model pipeline
        # TODO: have a specific canal
        sources = [
            ("unknown", table_column, 1) for table_column in table_column_names
        ]
        datasets, _labels = spec_from_source(sources)

        return datasets, _labels

    @staticmethod
    def get():
        # TODO: check model isn't already trained
        # is_trained = False

        # if is_trained:
        #     return "already trained"
        # else:
        #     columns, labels = None, None
        #     with psycopg2.connect(**train_db_params) as connection:
        #         columns, labels = build_training_set(connection)

        #     # Train model...
        #     model = train(columns, labels, model_type="ngram")

        #     # TODO: Store trained model

        with psycopg2.connect(**prod_db_params) as connection:
            # Compute dependency graph
            query.load(connection)

            # Fetch all distant columns
            # datasets, _labels = Analysis.build_datasets(connection)
            # columns = loader.fetch_columns(connection, datasets, dataset_size=100)

            # Classify all columns
            # classification = classify(model, columns, _labels)
            # print(len(classification))

            # TODO: Store classification

            # TODO
            # Compute dependency graph
            # Store dependency graph

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