from flask import Flask

from pagai.views import api


def create_app():
    app = Flask(__name__)
    app.register_blueprint(api)

    return app


app = create_app()
if __name__ == "__main__":
    # Start application
    app.run(debug=True, port=4000, host="0.0.0.0", load_dotenv=True)
