from flask import Flask
from flask_restful import Api

from api.resources import Search, BetaSearch, Init


app = Flask(__name__)
api = Api(app)

# TODO add current table also to reduce join
# api.add_resource(Search, '/search/<owner_name>/<table_name>/<column_name>/<resource_type>/<query>')
api.add_resource(Search, '/search/<resource_type>')
api.add_resource(
    BetaSearch,
    '/beta/search',
    '/beta/search/<resource_type>',
    '/beta/search/<resource_type>/<head_table>',
    '/beta/search/<resource_type>/<head_table>/<column_name>',
)
api.add_resource(
    Init,
    '/init/<database_name>',
    '/init/<database_name>/<force_retrain>'
)


if __name__ == '__main__':
    app.run(debug=True)
