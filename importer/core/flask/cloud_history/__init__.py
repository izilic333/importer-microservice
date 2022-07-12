from common.urls.urls import flask_config
from core.flask.common_flask.common import SECRET_KEY
from flask import Flask
from flask_compress import Compress
from flask_cors import CORS

app = Flask(__name__)
app.debug = flask_config['debug']
app.config['SECRET_KEY'] = SECRET_KEY
CONFIG = {'AMQP_URI': flask_config['amqp_uri']}
CORS(app)

COMPRESS_MIMETYPES = ['text/html', 'text/css', 'text/xml', 'application/json', 'application/javascript']
COMPRESS_LEVEL = 6
COMPRESS_MIN_SIZE = 500

Compress(app)
from . import views
