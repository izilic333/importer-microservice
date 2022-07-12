from common.urls.urls import flask_config

SECRET_KEY = '{}'.format(flask_config['token_generator'])
TOKEN_TIME = 6600

