from flask import Flask
from flask_restful import Api

from api.resources import Search, BasicSearch


app = Flask(__name__)
api = Api(app)

# TODO add current table also to reduce join
api.add_resource(Search, '/search/<owner_name>/<table_name>/<column_name>/<resource_type>/<query>')
api.add_resource(BasicSearch, '/basic/search/<resource_type>')


if __name__ == '__main__':
    app.run(debug=True)
