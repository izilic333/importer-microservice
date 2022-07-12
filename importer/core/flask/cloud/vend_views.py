from flask import request
from common.mixin.mixin import server_response, validate_date_format_flask
from core.flask.decorators.decorators import check_token
from database.company_database.core.query_company import GetCompanyFromDatabase
from elasticsearch_component.core.query_vends import GetVendImportProcessLog

from . import app


"""

    ElasticSearch Vend Import Logs

"""

@check_token()
@app.route('/vend_import/log/all/<company_id>', methods=['GET'])
def get_vend_import_logs_by_company(company_id):
    cmp = GetCompanyFromDatabase.get_company_by_id(company_id)
    if not cmp['success']:
        return server_response([], 404, 'Company does not exist.', False)

    logs = GetVendImportProcessLog.get_process_by_company_id(company_id)
    if not logs['status']:
        return server_response(
            [], 404, logs['message'], logs['status']
        )

    return server_response(logs['results'], 200, 'All vend import data for company : %s' % company_id, True)

@check_token()
@app.route('/vend_import/log/type/<company_id>/<type>', methods=['GET'])
def get_vend_import_logs_by_company_type(company_id, vend_type):
    cmp = GetCompanyFromDatabase.get_company_by_id(company_id)
    if not cmp['success']:
        return server_response([], 404, 'Company does not exist.', False)

    logs = GetVendImportProcessLog.get_process_by_vend_type(company_id, vend_type)
    if not logs['status']:
        return server_response(
            [], 404, logs['message'], logs['status']
        )

    return server_response(logs['results'], 200,
                           'All vend import data for company : %s with import_type ' % (company_id, vend_type), True)

@check_token()
@app.route('/vend_import/log/query/<company_id>', methods=['GET'])
def get_vend_import_logs_by_date_range(company_id):
    cmp = GetCompanyFromDatabase.get_company_by_id(company_id)
    if not cmp['success']:
        return server_response([], 404, 'Company not exists.', False)

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

    log = (
            GetVendImportProcessLog
            .get_process_by_date_range(company_id, import_type, date_start, date_end)
        )
    if not log['status']:
        return server_response(
            [], 404, log['message'], log['status']
        )
    return server_response(
        log['results'], 200, 'All data for company : %s' % company_id, True
    )

@check_token()
@app.route('/vend_import/log/hash/<company_id>/<elastic_hash>', methods=['GET'])
def get_vend_import_logs_by_hash(company_id, elastic_hash):
    # Validate company
    cmp = GetCompanyFromDatabase.get_company_by_id(company_id)
    if not cmp['success']:
        return server_response([], 404, 'Company not exists.', False)

    # Validate elastic hash
    process = GetVendImportProcessLog.get_process_by_hash(company_id, elastic_hash)

    if not process['status']:
        return server_response(
            [], 404, 'Company query with hash not found: %s' % elastic_hash, process['status']
        )

    return server_response(
        process['results'], 200, 'Company query with hash found: %s' % elastic_hash,
        process['status']
    )

