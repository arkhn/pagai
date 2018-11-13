from flask_restful import Resource, reqparse
import requests

from api.common.utils import file_response
from src.query import Query


ENCODING = 'utf-8'

database = 'mimic' # 'CW'
owner = 'ICSF'
query = Query(owner, database)
query.load()


class BasicSearch(Resource):
    @staticmethod
    def post():
        parser = reqparse.RequestParser()
        parser.add_argument('resource_type', required=False, type=str)
        parser.add_argument('owner', required=True, type=str)


        args = parser.parse_args()

    @staticmethod
    def get(resource_type):
        """Return columns which have the desired resource type"""

        columns = query.find(resource_type)

        return columns

class Search(Resource):
    @staticmethod
    def get(resource_type):
        return


class Schemas(Resource):
    @staticmethod
    def get():
        """Returns CSV list of available database schemas."""

        content = requests.get('{}/databases.json'.format(
            SCHEMA_URL
        )).content.decode(ENCODING)

        return file_response(content, 'json')


class Schema(Resource):
    @staticmethod
    def get(database_name, extension):
        """Fetches distant file and parses it according to its extension."""

        content = requests.get('{}/{}.{}'.format(
            SCHEMA_URL,
            database_name,
            extension
        )).content.decode(ENCODING)

        return file_response(content, extension)


class Store(Resource):
    @staticmethod
    def get(resource_name, extension):
        """Fetches distant file from Store and parses it according to its extension."""

        content = requests.get('{}/{}.{}'.format(
            STORE_URL,
            resource_name,
            extension
        )).content.decode(ENCODING)

        return file_response(content, extension)
