from flask import Flask

from pagai.views import api
from pagai.json_encoder import MyJSONEncoder


def create_app():
    app = Flask(__name__)
    app.register_blueprint(api)
    app.json_encoder = MyJSONEncoder

    return app


app = create_app()
if __name__ == "__main__":
    # Start application
    app.run(debug=True, port=4000, host="0.0.0.0", load_dotenv=True)
