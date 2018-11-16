import os
import pickle
import logging
from pathlib import Path
import numpy as np

from engine.dependency import Discovery as Discovery
from engine import models

SAVE_PATH_FILE = 'build/query.pickle'

class Query:
    def __init__(self, database, owner):
        self.database = database
        self.owner = owner
        self.dependency_graph = None
        self.model = None

    def find(self, resource_type, parent_table=None, column_name=None, max_results=10):
        """
        Find all columns with a given resource_type and more criteria:
        - parent_table (Optional): the tables close to the parent_table are favoured
        - more to come
        """
        # Send request to the model
        key_word = resource_type.upper()
        columns = self.model.find_all(key_word)

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

        return self.api_response(columns)

    def load(self, force_retrain=False):
        database, owner = self.database, self.owner
        query_pickle = Path(SAVE_PATH_FILE)
        if query_pickle.is_file() and not force_retrain:
            logging.warning('Model ready!')
            with open(SAVE_PATH_FILE, 'rb') as pickle_file:
                query = pickle.load(pickle_file)
                self.dependency_graph = query.dependency_graph
                self.model = query.model
                return self
        else:
            logging.warning('Training model...')
            # Load the discovery module to build the dependency graph
            discovery = Discovery(database, owner)
            dependency_graph = discovery.build_dependency_graph()

            # Load and train the model
            model = models.train.train(owner, database, 'ngram')

            self.model = model
            self.dependency_graph = dependency_graph

            if not os.path.exists(os.path.dirname(SAVE_PATH_FILE)):
                os.makedirs(os.path.dirname(SAVE_PATH_FILE))
            with open(SAVE_PATH_FILE, 'wb') as file:
                pickle.dump(self, file)

            return self

    @staticmethod
    def api_response(results):
        response = []
        for res in results:
            response.append(res.ser())
        return response

