from flask_restful import Resource, reqparse
import requests

from api.common.utils import file_response
from engine.query import Query


ENCODING = 'utf-8'

database = 'mimic' # 'CW'
owner = 'ICSF'
query = Query(owner, database)
query.load()

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

