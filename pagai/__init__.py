from flask import Flask

from pagai.views import api

app = Flask(__name__)
app.register_blueprint(api)

if __name__ == "__main__":
    # Start application
    app.run(debug=True, host="0.0.0.0", load_dotenv=True)
