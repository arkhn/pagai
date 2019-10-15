from pathlib import Path
import logging
import os
import pickle
import psycopg2

from engine.dependency import DependencyGraphBuilder
from engine.models import train, predict
from engine.structure import Graph
from queries.postgres import fetch_columns, get_table_names, get_column_names

SAVE_PATH = "build"

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
        sources = [
            ("unknown", table_column, 1) for table_column in table_column_names
        ]
        datasets, _labels = train.spec_from_source(sources)

        return datasets, _labels


def init(database_name, force_retrain=False):
    """
    Receives a database name and performs the following tasks:
    * compute dependency graph if need be
    * train models if need be
    * predict classes for database if need be
    * store all results
    """

    pickle_path = f"{SAVE_PATH}/{database_name}.pickle"
    pickle_file = Path(pickle_path)

    # Check model isn't already trained
    if pickle_file.is_file() and not force_retrain:
        logging.warning("Model ready!")
        return "trained"
    else:
        with psycopg2.connect(**prod_db_params) as connection:
            # Load discovery module to build dependency graph
            logging.warning("Building the dependency graph...")
            dependency_graph_builder = DependencyGraphBuilder()
            # dependency_graph = dependency_graph_builder.build_dependency_graph(connection)
            dependency_graph = Graph()

            # Train models
            model_types = ["ngram"]
            models = dict()
            classifications = dict()

            logging.warning("Build/load training sets...")
            columns, labels = None, None

            # Build training set
            with psycopg2.connect(**train_db_params) as connection:
                columns, labels = train.build_training_set(connection)

            # Fetch all rows to be classified
            logging.warning("Build datasets...")
            datasets, _labels = build_datasets(connection)
            logging.warning("Fetch columns...")
            columns = fetch_columns(datasets, dataset_size=1, connection=connection)

            # Train and classify each model
            for model_type in model_types:
                # Train
                logging.warning(f"Training {model_type}...")
                model = train.train(columns, labels, model_type)
                models[model_type] = model
                # Classify
                classification = predict.classify(model, columns, _labels)
                classifications[model_type] = classification

            # Store dependency graph, trained models, and predictions
            ## Create path
            if not os.path.exists(os.path.dirname(pickle_path)):
                os.makedirs(os.path.dirname(pickle_path))
            ## Store pickle file
            logging.warning("Saving results...")
            with open(pickle_file, "wb") as file:
                pickle.dump({
                    "dependency_graph": dependency_graph,
                    "models": models,
                    "classifications": classifications
                }, file)