from common.urls.urls import flask_config
from core.flask.common_flask.common import SECRET_KEY
from flask import Flask
from flask_compress import Compress
from flask_cors import CORS
from flask_limiter.util import get_remote_address
from flask_limiter import Limiter

app = Flask(__name__)


limiter = Limiter(
    app,
    key_func=get_remote_address,
)

app.debug = flask_config['debug']
app.config['SECRET_KEY'] = SECRET_KEY
CONFIG = {'AMQP_URI': flask_config['amqp_uri']}
CORS(app)

COMPRESS_MIMETYPES = ['text/html', 'text/css', 'text/xml', 'application/json', 'application/javascript']
COMPRESS_LEVEL = 6
COMPRESS_MIN_SIZE = 500

Compress(app)

from . import views
