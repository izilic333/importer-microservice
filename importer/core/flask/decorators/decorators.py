from common.mixin.mixin import server_response
from core.flask.sessions.session import AuthorizeUser

from functools import wraps
from flask import request


def check_token():
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            header = request.headers.get('Authorization')
            if not header:
                return server_response(
                    [], 403, 'Authorization failed. Please provide token.', False)

            validate_token = AuthorizeUser.verify_user_token(header)
            if not validate_token['success']:
                return server_response([], 403, validate_token['message'], False)

            return f(*args, **kwargs)
        return wrapped
    return decorator

