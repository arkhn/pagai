from pathlib import Path
import logging
import numpy as np
import os
import pickle
import psycopg2

from pagai.engine.dependency import DependencyGraphBuilder
from pagai.engine.models import train, predict
from pagai.engine.structure import Graph
from pagai.engine.models import SAVE_PATH

from pagai.services.postgres import fetch_columns, get_table_names, get_column_names


prod_db_params = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

train_db_params = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}


class Engine:
    def __init__(self, database_name):
        self.database_name = database_name
        self.dependency_graph = None
        self.model_type = "ngram"
        self.models = {}
        self.classifications = {}

    @staticmethod
    def build_datasets(connection):
        # Get all table.column names
        table_names = get_table_names(connection=connection)
        table_column_names = [
            "{}.{}".format(table_name, column_name)
            for table_name in table_names
            for column_name in get_column_names(table_name, connection=connection)
        ]

        # Format to fit the transform and predict model pipeline
        # TODO: have a specific canal
        sources = [("unknown", table_column, 1) for table_column in table_column_names]
        datasets, _labels = train.spec_from_source(sources)

        return datasets, _labels

    def initialise(self, force_retrain=False):
        """
        Receives a database name and performs the following tasks:
        * compute dependency graph if need be
        * train models if need be
        * predict classes for database if need be
        * store all results
        """

        pickle_path = f"{SAVE_PATH}/{self.database_name}.pickle"
        pickle_path = Path(pickle_path)

        # Check model isn't already trained
        if pickle_path.is_file() and not force_retrain:
            logging.warning("Engine already initiated. Loading engine...")
            with open(pickle_path, "rb") as pickle_file:
                engine = pickle.load(pickle_file)
            self.dependency_graph = engine["dependency_graph"]
            self.models = engine["models"]
            self.classifications = engine["classifications"]

            return "trained"
        else:
            with psycopg2.connect(**prod_db_params) as connection:
                # Load discovery module to build dependency graph
                logging.warning("Building the dependency graph...")
                dependency_graph_builder = DependencyGraphBuilder()
                # TODO build the dependency graph
                # self.dependency_graph = dependency_graph_builder.build_dependency_graph(connection)
                self.dependency_graph = Graph()

                # Train models
                model_types = ["ngram"]
                logging.warning("Build / load training sets...")
                columns, labels = None, None

                # Build training set
                with psycopg2.connect(**train_db_params) as train_db_connection:
                    columns, labels = train.build_training_set(train_db_connection)

                # Fetch all rows to be classified
                logging.warning("Build datasets...")
                datasets, _ = Engine.build_datasets(connection)
                logging.warning("Fetch columns...")
                test_columns = fetch_columns(datasets, dataset_size=100, connection=connection)

                # Train and classify each model
                for model_type in model_types:
                    # Train
                    logging.warning(f"Training {model_type}...")
                    model = train.train(columns, labels, model_type)
                    self.models[model_type] = model
                    # Classify
                    classification = predict.classify(model, test_columns)
                    model.classification = classification
                    self.classifications[model_type] = classification

                # Store dependency graph, trained models, and predictions
                ## Create path
                if not os.path.exists(os.path.dirname(pickle_path)):
                    os.makedirs(os.path.dirname(pickle_path))
                ## Store pickle file
                pickle_file = "pickle/model.pickle"
                logging.warning("Saving results...")
                with open(pickle_file, "wb") as file:
                    pickle.dump(
                        {
                            "dependency_graph": self.dependency_graph,
                            "models": self.models,
                            "classifications": self.classifications,
                        },
                        file,
                    )

                with open(pickle_file, "rb") as file:
                    _ = pickle.load(file)

    def score(self, resource_type, parent_table=None, column_name=None, max_results=10):
        """
        Return columns sorted according to several criteria provided as arguments. Criteria are blended in a single score used to sort the columns in decreasing order.

        Args:
            resource_type: the type of column, which depends on how the labels selected when training the model. Example: Firstname, City, Code, Id, etc
            parent_table (optional): the tables close to the parent_table are favoured
            column_name (optional): the column name (or an inclusion of it) that we expect. Example: "Pat" perfectly matches "PATIENT".
        """
        # Send request to the model
        key_word = resource_type.upper()
        columns = self.models[self.model_type].find_all(key_word)

        # If a parent table is provided, down vote the tables that are "far" from the parent table
        if parent_table is not None:
            for column in columns:
                distance = self.dependency_graph.get_distance(parent_table, column.table)
                column.score *= 2 ** (-distance)

        # If column_name is provided, up vote columns that fit with it
        if column_name is not None:
            query = column_name
            for column in columns:
                distance = column.match_name_score(query)
                column.score *= 1 / np.sqrt(1 + distance)

        # Sort to have the column with the biggest score first
        columns.sort(key=lambda x: x.score, reverse=True)

        # Don't display all results
        columns = columns[:max_results]

        return [col.serialize() for col in columns]
