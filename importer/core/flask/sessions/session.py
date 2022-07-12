from core.flask.common_flask.common import SECRET_KEY
from passlib.hash import django_pbkdf2_sha256
from itsdangerous import (BadSignature,
                          SignatureExpired, JSONWebSignatureSerializer)


class AuthorizeUser(object):
    @classmethod
    def check_password(cls, user_password, requested_password):
        return django_pbkdf2_sha256.verify(requested_password, user_password)

    @classmethod
    def set_session(cls, user):
        pass

    @classmethod
    def generate_jwt_token(cls, user):
        s = JSONWebSignatureSerializer(secret_key=SECRET_KEY, algorithm_name='HS512')
        return s.dumps(
            {
                'user_id': user[0]['id'],
                'username': user[0]['username'],
                'email': user[0]['email'],
                'full_name': user[0]['full_name'],
                'company_id': user[0]['company_id'],
                'language': user[0]['language']

            }
        )

    @classmethod
    def verify_user_token(cls, token):
        s = JSONWebSignatureSerializer(secret_key=SECRET_KEY, algorithm_name='HS512')
        try:
            data = s.loads(token)
        except SignatureExpired:
            return {'success': False, 'message': 'Token expired.', 'response': []}
        except BadSignature:
            return {'success': False, 'message': 'Invalid token.', 'response': []}
        return {'success': True, 'message': 'Token valid.', 'response': data}
