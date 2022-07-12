from flask import request

from common.mixin.mixin import server_response
from common.mixin.validation_const import return_import_type_status_and_import
from core.flask.decorators.decorators import check_token
from core.flask.sessions.session import AuthorizeUser
from database.company_database.core.client_history import CloudHistory
from database.company_database.core.query_company import GetCompanyFromDatabase
from elasticsearch_component.core.cloud_elastic import CloudElasticQuery
from . import app


@app.route('/all', methods=['GET', ])
@check_token()
def get_company_history():
    header = request.headers.get('Authorization')
    token_data = AuthorizeUser.verify_user_token(header)['response']

    company_id = token_data['company_id']

    cmp = GetCompanyFromDatabase.get_company_by_id(company_id)
    if not cmp['success']:
        return server_response([], 404, 'Company not exists.', False)

    request_all_history = CloudHistory.retrieve_all_history_fail_and_success(company_id)

    if not request_all_history['status']:
        return server_response([], 404, request_all_history['message'], False)

    return server_response(
        request_all_history['results'], 200, 'Data found.',
        request_all_history['status']
    )


@app.route('/<import_type>', methods=['GET', ])
@check_token()
def get_company_history_by_type(import_type):
    header = request.headers.get('Authorization')
    token_data = AuthorizeUser.verify_user_token(header)['response']

    company_id = token_data['company_id']

    cmp = GetCompanyFromDatabase.get_company_by_id(company_id)
    if not cmp['success']:
        return server_response([], 404, 'Company not exists.', False)

    import_type_check = return_import_type_status_and_import(import_type)
    if not import_type_check['success']:
        return server_response(
            [], 404, import_type_check['response'], import_type_check['success']
        )

    request_all_history = CloudHistory.retrieve_all_history_fail_and_success(company_id, import_type)

    if not request_all_history['status']:
        return server_response([], 404, request_all_history['message'], False)

    return server_response(
        request_all_history['results'], 200, 'Data found',
        request_all_history['status']
    )


@app.route('/hash/<hash>', methods=['GET', ])
@check_token()
def get_company_hash(hash):
    header = request.headers.get('Authorization')
    token_data = AuthorizeUser.verify_user_token(header)['response']

    company_id = token_data['company_id']

    cmp = GetCompanyFromDatabase.get_company_by_id(company_id)
    if not cmp['success']:
        return server_response([], 404, 'Company not exists.', False)

    elastic_query = CloudElasticQuery.get_process_hash_full_object(company_id, hash)

    if not elastic_query['status']:
        return server_response([], 404, elastic_query['message'], elastic_query['status'])

    return server_response(
        elastic_query['results'], 200, 'Data found',
        elastic_query['status']
    )

