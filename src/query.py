import os
import pickle
import logging
from pathlib import Path

from src.dependency import Discovery as Discovery
from src import models

SAVE_PATH_FILE = 'build/query.pickle'


class Query:
    def __init__(self, database, owner):
        self.database = database
        self.owner = owner
        self.dependency_graph = None
        self.model = None

    def find(self, resource_type):
        key_word = resource_type.upper()
        columns = self.model.find_all(key_word)
        return columns

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
            #discovery = Discovery(database, owner)
            dependency_graph = None #discovery.build_dependency_graph()

            # Load and train the model
            model = models.train.train(owner, database, 'ngram')

            self.model = model
            self.dependency_graph = dependency_graph

            if not os.path.exists(os.path.dirname(SAVE_PATH_FILE)):
                os.makedirs(os.path.dirname(SAVE_PATH_FILE))
            with open(SAVE_PATH_FILE, 'wb') as file:
                pickle.dump(self, file)

            return self

