import os

from dotenv import load_dotenv
from flask import Flask
from pagai.views import api

app = Flask(__name__)
app.register_blueprint(api)


if __name__ == "__main__":
    # Load .env config file for entire environment
    configFileName = os.getenv("CONFIG_PATH", ".env.dev.default")
    load_dotenv(dotenv_path=configFileName)

    # Start application
    app.run(debug=True, host="0.0.0.0")
