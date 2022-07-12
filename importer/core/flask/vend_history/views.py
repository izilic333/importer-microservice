from flask import request

from common.mixin.mixin import server_response
from core.flask.decorators.decorators import check_token
from core.flask.sessions.session import AuthorizeUser
from database.company_database.core.query_company import GetCompanyFromDatabase
from elasticsearch_component.core.cloud_vend_elastic import CloudVendImportQuery
from common.mixin.mixin import validate_date_format_flask


from . import app


@app.route('/hash/<process_hash>', methods=['GET'])
@check_token()
def vend_import_history_by_index_hash(process_hash):
    header = request.headers.get('Authorization')
    token_data = AuthorizeUser.verify_user_token(header)['response']

    company_id = token_data['company_id']

    cmp = GetCompanyFromDatabase.get_company_by_id(company_id)
    if not cmp['success']:
        return server_response([], 404, 'Company does not exist.', False)

    elastic_query = CloudVendImportQuery.get_process_hash_index(process_hash, company_id)

    if not elastic_query['status']:
        return server_response([], 404, elastic_query['message'], elastic_query['status'])

    return server_response(
        elastic_query['results'], 200, 'Data found',
        elastic_query['status']
    )


@app.route('/company/all', methods=['GET'])
@check_token()
def vend_import_history_by_company_all():
    header = request.headers.get('Authorization')
    token_data = AuthorizeUser.verify_user_token(header)['response']

    company_id = token_data['company_id']

    cmp = GetCompanyFromDatabase.get_company_by_id(company_id)
    if not cmp['success']:
        return server_response([], 404, 'Company does not exist.', False)

    elastic_query = CloudVendImportQuery.get_process_company_id(company_id)

    if not elastic_query['status']:
        return server_response([], 404, elastic_query['message'], elastic_query['status'])

    return server_response(
        elastic_query['results'], 200, 'Data found',
        elastic_query['status']
    )


@app.route('/type/<vend_type>', methods=['GET'])
@check_token()
def vend_import_history_by_type(vend_type):
    header = request.headers.get('Authorization')
    token_data = AuthorizeUser.verify_user_token(header)['response']

    company_id = token_data['company_id']

    cmp = GetCompanyFromDatabase.get_company_by_id(company_id)
    if not cmp['success']:
        return server_response([], 404, 'Company does not exist.', False)

    elastic_query = CloudVendImportQuery.get_process_company_id_vend_type(company_id, vend_type)

    if not elastic_query['status']:
        return server_response([], 404, elastic_query['message'], elastic_query['status'])

    return server_response(
        elastic_query['results'], 200, 'Data found',
        elastic_query['status']
    )


@app.route('/query', methods=['GET'])
@check_token()
def vend_import_history_by_query():
    header = request.headers.get('Authorization')
    token_data = AuthorizeUser.verify_user_token(header)['response']

    company_id = token_data['company_id']

    cmp = GetCompanyFromDatabase.get_company_by_id(company_id)
    if not cmp['success']:
        return server_response([], 404, 'Company does not exist.', False)

    date_start = request.args.get('start')
    date_end = request.args.get('end')
    import_type = request.args.get('type')

    if not date_start and not date_end and not import_type:
        return server_response([], 403, 'Please populate query string.', False)

    date_start_validate = validate_date_format_flask(date_start)
    date_end_validate = validate_date_format_flask(date_start)

    if not date_start_validate['success']:
        return server_response([], 403, 'Start date: ' + date_start_validate['message'], False)
    elif not date_end_validate['success']:
        return server_response([], 403, 'End date: ' + date_start_validate['message'], False)

    if not import_type:
        import_type = ''

    elastic_query = CloudVendImportQuery.get_process_date_range(company_id, import_type, date_start, date_end)

    if not elastic_query['status']:
        return server_response([], 404, elastic_query['message'], elastic_query['status'])

    return server_response(
        elastic_query['results'], 200, 'Data found',
        elastic_query['status']
    )
