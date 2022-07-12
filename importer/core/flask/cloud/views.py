import json
import time

from common.mixin.enum_errors import return_enum_error
from common.rabbit_mq.export_publisher.export_publisher import PublishExportQ
from core.flask.redis_store.redis_managment import RedisManagement
from core.flask.sessions.session import AuthorizeUser
from database.company_database.core.query_export import ExportHistoryQuery
from database.company_database.core.query_history import (
    CompanyFailHistory, CompanyHistory,
    CloudRequestHistory,
    CloudVendRequestHistory)
from flask import request, send_file
from common.mixin.mixin import server_response, validate_date_format_flask, custom_hash, custom_hash_vends
from common.mixin.validation_const import (return_import_type_status_and_import,
                                           return_import_type_id, return_active_type,
                                           return_file_example)

from core.flask.decorators.decorators import check_token
from database.company_database.core.query_company import GetCompanyFromDatabase
from elasticsearch_component.core.query_company import GetCompanyProcessLog
from elasticsearch_component.core.query_vends import GetVendImportProcessLog

from . import app

"""

    Elastic search log status

"""

@app.route('/import/log/hash/<company_id>/<elastic_hash>', methods=['GET', ])
@check_token()
def api_get_log_by_hash(company_id, elastic_hash):
    # Validate company
    cmp = GetCompanyFromDatabase.get_company_by_id(company_id)
    if not cmp['success']:
        return server_response([], 404, 'Company not exists.', False)

    # Validate elastic hash
    process = GetCompanyProcessLog.get_process_by_hash(company_id, elastic_hash)

    if not process['status']:
        return server_response(
            [], 404, 'Company query with hash not found: %s' % elastic_hash, process['status']
        )

    return server_response(
        process['results'], 200, 'Company query with hash found: %s' % elastic_hash,
        process['status']
    )

@app.route('/import/log/type/<company_id>/<import_type>', methods=['GET', ])
@check_token()
def api_get_all_logs_by_type(company_id, import_type):
    cmp = GetCompanyFromDatabase.get_company_by_id(company_id)
    if not cmp['success']:
        return server_response([], 404, 'Company not exists.', False)

    # Check import type
    import_type_check = return_import_type_status_and_import(import_type)
    if not import_type_check['success']:
        return server_response([], 404, import_type_check['response'], import_type_check['success'])

    logs = GetCompanyProcessLog.get_logs_based_on_type_and_company(company_id, import_type.upper())
    if not logs['status']:
        return server_response(
            [], 404, logs['message'], logs['status']
        )

    return server_response(logs['results'], 200, 'All data for company : %s' % company_id, True)


@app.route('/import/log/all/<company_id>', methods=['GET', ])
@check_token()
def api_get_all_logs_all(company_id):
    cmp = GetCompanyFromDatabase.get_company_by_id(company_id)
    if not cmp['success']:
        return server_response([], 404, 'Company not exists.', False)

    logs = GetCompanyProcessLog.get_all_logs_based_on_company_id(company_id)
    if not logs['status']:
        return server_response(
            [], 404, logs['message'], logs['status']
        )

    return server_response(logs['results'], 200, 'All data for company : %s' % company_id, True)


@app.route('/import/log/query/<company_id>', methods=['GET', ])
@check_token()
def api_get_all_logs_query(company_id):
    cmp = GetCompanyFromDatabase.get_company_by_id(company_id)
    if not cmp['success']:
        return server_response([], 404, 'Company not exists.', False)

    # Query params
    date_start = request.args.get('start')
    date_end = request.args.get('end')
    search_type = request.args.get('type')

    if not date_start and not date_end and not search_type:
        return server_response([], 403, 'Please populate query string.', False)

    # Validate date
    date_start_validate = validate_date_format_flask(date_start)
    date_end_validate = validate_date_format_flask(date_start)

    if not date_start_validate['success']:
        return server_response([], 403, 'Start date: '+date_start_validate['message'], False)
    elif not date_end_validate['success']:
        return server_response([], 403, 'End date: '+date_start_validate['message'], False)

    if search_type:
        import_type_check = return_import_type_status_and_import(search_type)
        if not import_type_check['success']:
            return server_response(
                [], 404, import_type_check['response'], import_type_check['success']
            )

    # Start Query Elastic search
    if date_start and date_end and not search_type:
        process = (
            GetCompanyProcessLog
            .get_logs_based_on_company_id_and_date(company_id, date_start, date_end, '')
        )

        if not process['status']:
            return server_response(
                [], 404, process['message'], process['status']
            )
        return server_response(
            process['results'], 200, 'All data for company : %s' % company_id, True
        )

    elif date_start and date_end and search_type:
        process = (
            GetCompanyProcessLog
                .get_logs_based_on_company_id_and_date(
                company_id, date_start, date_end, search_type.upper()
            )
        )
        if not process['status']:
            return server_response(
                [], 404, process['message'], process['status']
            )
        return server_response(
            process['results'], 200, 'All data for company : %s' % company_id, True
        )


"""

    Local Database Import History

"""


@app.route('/import/history/fail/<company_id>', methods=['GET', ])
@check_token()
def api_history_fail_log(company_id):
    cmp = GetCompanyFromDatabase.get_company_by_id(company_id)
    if not cmp['success']:
        return server_response([], 404, 'Company not exists.', False)

    date_from = request.args.get('date_from')
    type_import = request.args.get('type')

    # Date from
    if date_from:
        date_from_validate = validate_date_format_flask(date_from)
        if not date_from_validate['success']:
            return server_response([], 403, 'Date from: '+date_from_validate['message'], False)

    # Type of import
    if type_import:
        import_type_check = return_import_type_status_and_import(type_import)
        if not import_type_check['success']:
            return server_response(
                [], 404, import_type_check['response'], import_type_check['success']
            )

    # Lets make some API request's

    if not date_from and not type_import:
        # Get all fail history only with company ID
        company_fail_history = CompanyFailHistory.get_fail_history_by_company(company_id)
        if not company_fail_history['status']:
            return server_response([], 404, company_fail_history['message'], False)

        return server_response(
            company_fail_history['results'], 200, 'All data for company : %s' % company_id, True
        )

    elif date_from and not type_import:
        company_fail_history = (
            CompanyFailHistory
            .get_fail_history_company_id_and_date_from(company_id, date_from)
        )
        if not company_fail_history['status']:
            return server_response([], 404, company_fail_history['message'], False)

        return server_response(
            company_fail_history['results'], 200, 'All data for company : %s' % company_id, True
        )

    elif date_from and type_import:
        import_type_id = return_import_type_id(type_import)
        company_fail_history = (
            CompanyFailHistory
            .get_fail_history_by_datetime(company_id, import_type_id, date_from)
        )
        if not company_fail_history['status']:
            return server_response([], 404, company_fail_history['message'], False)

        return server_response(
            company_fail_history['results'], 200, 'All data for company : %s' % company_id, True
        )


@app.route('/import/history/dashboard/all/<company_id>', methods=['GET'])
@check_token()
def company_dashboard_history_all(company_id):
    # Check company
    cmp = GetCompanyFromDatabase.get_company_by_id(company_id)
    if not cmp['success']:
        return server_response([], 404, 'Company not exists.', False)

    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    type_import = request.args.get('type')

    # Date from
    if date_from or date_to:
        date_from_validate = validate_date_format_flask(date_from)
        if not date_from_validate['success']:
            return server_response([], 403, 'Date from: ' + date_from_validate['message'], False)

        date_to_validate = validate_date_format_flask(date_to)
        if not date_to_validate['success']:
            return server_response([], 403, 'Date to: ' + date_from_validate['message'], False)

    # Type of import
    if type_import:
        import_type_check = return_import_type_status_and_import(type_import)
        if not import_type_check['success']:
            return server_response(
                [], 404, import_type_check['response'], import_type_check['success']
            )

    if not type_import:

        redis_key_masterdata = custom_hash(company_id+str(date_from)+str(date_to))
        redis_key_vends = custom_hash_vends(company_id+str(date_from)+str(date_to))
        # Check redis for key
        query_data_masterdata = RedisManagement.get_data_from_redis(redis_key_masterdata)
        query_data_vends = RedisManagement.get_data_from_redis(redis_key_vends)

        if not query_data_masterdata:
            # Get all fail history only with company ID
            company_process_history = CloudRequestHistory.retrieve_all_history_fail_and_success(company_id, date_from=date_from, date_to=date_to)

            if not company_process_history['status']:
                return server_response([], 404, company_process_history['message'], False)

            # Redis data
            query_data_masterdata = RedisManagement.get_data_from_redis(redis_key_masterdata)

        if not query_data_vends:
            # Get all fail history only with company ID
            vend_process_history = CloudVendRequestHistory.retrieve_all_vends_history_fail_and_success(company_id, date_from=date_from, date_to=date_to)

            if not vend_process_history['status']:
                return server_response([], 404, vend_process_history['message'], False)

            # Redis data
            query_data_vends = RedisManagement.get_data_from_redis(redis_key_vends)

        query_data_all = []

        if query_data_masterdata:
            query_data_all.extend(json.loads(query_data_masterdata))
        if query_data_vends:
            query_data_all.extend(json.loads(query_data_vends))
        return server_response(
            query_data_all, 200, 'All data for company : %s' % company_id, True
        )


@app.route('/import/history/dashboard/<company_id>', methods=['GET', ])
@check_token()
def company_dashboard(company_id):
    cmp = GetCompanyFromDatabase.get_company_by_id(company_id)
    if not cmp['success']:
        return server_response([], 404, 'Company not exists.', False)

    company_users = CompanyHistory.get_all_users(company_id)
    include_history_only = request.args.get('includeHistoryOnly')

    return server_response(
        {
            'import_type': return_active_type(include_history_only),
            'error_type': return_enum_error(),
            'users': company_users['results']
        }, 200, 'All data for company : %s' % company_id, True
    )


@app.route('/import/history/elastic/<company_id>/<elastic_hash>', methods=['GET', ])
@check_token()
def company_socket_elastic(company_id, elastic_hash):
    # Check company
    cmp = GetCompanyFromDatabase.get_company_by_id(company_id)
    if not cmp['success']:
        return server_response([], 404, 'Company not exists.', False)

    # Validate elastic hash
    process = GetCompanyProcessLog.get_process_by_hash(company_id, elastic_hash)

    if not process['status']:
        return server_response(
            [], 404, 'Company query with hash not found: %s' % elastic_hash, process['status']
        )


@app.route('/export/download-file/<id>', methods=['GET', 'POST', ])
def download_file(id):
    file = ExportHistoryQuery.return_file_path(id)
    if not file['status']:
        return server_response(
            [], 404, '%s' % file['message'], file['status']
        )

    return send_file('{}'.format(file['results']['file_path']), as_attachment=True)


@app.route('/export/history/all/<company_id>', methods=['GET', ])
@check_token()
def export_history_view(company_id):
    # Check company
    cmp = GetCompanyFromDatabase.get_company_by_id(company_id)
    if not cmp['success']:
        return server_response([], 404, 'Company not exists.', False)

    query_all_history = ExportHistoryQuery.get_all_history_export_log(company_id)

    if not query_all_history['status']:
        return server_response(
            [], 200, query_all_history['message'], query_all_history['status']
        )

    return server_response(
        query_all_history['results'], 200, 'All data for company : %s' % company_id,
        query_all_history['status']
    )


@app.route('/export/history/<company_id>', methods=['GET', ])
@check_token()
def export_history_set(company_id):

    # Check company
    cmp = GetCompanyFromDatabase.get_company_by_id(company_id)
    if not cmp['success']:
        return server_response([], 404, 'Company not exists.', False)

    type_import = request.args.get('type')

    # Type of import
    if type_import and type_import != 'ALL':
        import_type_check = return_import_type_status_and_import(type_import)
        if not import_type_check['success']:
            return server_response(
                [], 404, import_type_check['response'], import_type_check['success']
            )

    if not type_import:
        return server_response(
            [], 403, 'Please provide all requirements for export data.', False
        )

    header = request.headers.get('Authorization')
    token_data = AuthorizeUser.verify_user_token(header)['response']

    # Generate Query hash
    date_now = time.strftime("%Y-%m-%d")
    hash_string = str(company_id)+str(type_import)+str(date_now)
    query_hash = custom_hash(hash_string)

    # Query history before start new request
    history_query = ExportHistoryQuery.get_history_by_hash(query_hash)
    if not history_query['status']:
        return server_response(
            [], 403, 'Export is currently exporting results. It will be visible in table.', False
        )

    create_export_json = {
        'user_id': token_data['user_id'],
        'user_name': token_data['full_name'],
        'email': token_data['email'],
        'company_id': company_id,
        'export_type': type_import,
        'export_hash': query_hash
    }

    # Call Q publisher and start export
    rabbit_mq_publisher = PublishExportQ.publish_new_export(create_export_json)

    if not rabbit_mq_publisher['success']:
        return server_response(
            [], 403, rabbit_mq_publisher['message'], False
        )

    return server_response(
        [{'track_hash': query_hash}], 201,
        'Export will be visible in interface soon. You will receive notification on your email' , True
    )


@app.route('/file_example/<type_name>', methods=['GET', 'POST', ])
def download_file_example(type_name):
    import os

    if type_name:
        import_type_check = return_import_type_status_and_import(type_name)
        if not import_type_check['success']:
            return server_response(
                [], 404, import_type_check['response'], import_type_check['success']
            )

    file_name = return_file_example(type_name)
    basepath = os.path.dirname(__file__)
    filepath_example = os.path.abspath(
        os.path.join(basepath, "..", "..", "..", "file_examples/{}".format(file_name))
    )

    return send_file(filepath_example,
                     mimetype='text/csv',
                     attachment_filename='{}'.format(file_name),
                     as_attachment=True)


@app.route('/download_history_file/<path:file_url>', methods=['GET', 'POST',])
def download_file_path(file_url):
    file_full_path = file_url.replace('!', '/')
    file_name_split = file_full_path.split('/')
    file_name = file_name_split[len(file_name_split)-1]
    file_al = file_name.split('$')

    return send_file(file_full_path,
                     mimetype='text/csv',
                     attachment_filename='{}'.format(file_al[1]),
                     as_attachment=True)
