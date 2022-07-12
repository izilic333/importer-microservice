import json
import os

from operator import itemgetter
import jsonschema

from common.mixin.elastic_login import ElasticCloudLoginFunctions
from common.mixin.handle_file import ImportProcessHandler
from common.mixin.vends_mixin import MainVendProcessLogger
from common.rabbit_mq.validator_api_q.pubisher import PublishFileToValidationMessageQ
from common.synchronize.request import ParseRabbitRequests
from core.flask.redis_store.redis_managment import RedisManagement
from database.company_database.core.company_statistic import CompanyMonthlyAPIUsage
from database.company_database.core.query_company import GetCompanyFromDatabase
from database.company_database.core.query_export import ExportHistory
from database.company_database.core.query_history import CloudRequestHistory, CompanyHistory, CloudVendRequestHistory
from elasticsearch_component.core.query_vends import VendImportProcessLogger
from jsonschema import validate
from flask import request
from werkzeug.utils import secure_filename
from common.logging.setup import save_flask_files, vend_logger

from common.mixin.mixin import (server_response, validate_file_extensions,
                                function_check_elastic_status, generate_elastic_process,
                                return_errors_from_json_schema, HandleValidationOfAPI)

from core.flask.sessions.session import AuthorizeUser

from database.cloud_database.core.query import CustomUserQueryOnCloud
from common.mixin.validation_const import (ImportType,
                                           return_import_type_status,
                                           return_import_type_based_on_parser,
                                           return_import_object_type,
                                           return_import_type_status_and_import,
                                           return_import_type_id)

from common.mixin.enum_errors import (elastic_not_allowed_status, EnumProcessType, EnumErrorType,
                                      EnumValidationMessage, EnumAPIType)
from common.rabbit_mq.validator_file_q.validator_publisher import publish_file_validation

from . import app, limiter
from common.mixin.enum_errors import EnumValidationMessage as Const
from common.mixin.enum_errors import enum_message_on_specific_language


@app.route('/login', methods=['POST'])
@limiter.limit("1 per second")
def login():
    data = request.json
    email = data.get('email')
    password = data.get('password')

    if not email or not password:
        return server_response([], 403, 'Please send email and password in request post body.', False)

    logger.info("User requested login: Email: {} Password: {}".format(email, password))
    check_user = CustomUserQueryOnCloud.check_if_user_exists_on_database(email)
    if not check_user['status']:
        return server_response([], 404, check_user['message'], False)
    # Get password hash
    password_check = AuthorizeUser.check_password(check_user['results'][0]['password'], password)

    # Return if user is not auth
    if not password_check:
        return server_response([], 403, 'User not authorized.', False)

    token = AuthorizeUser.generate_jwt_token(check_user['results'])
    return server_response(
        {'token': str(token.decode('ascii'))}, 200, 'Token successfully generated.', True
    )



@app.route('/import/api/export/<import_type>/<company_id>', methods=['GET'])
@limiter.limit("1 per second")
def export_history(import_type, company_id):
    header = request.headers.get('Authorization')
    if not header:
        return server_response([], 403, 'Authorization failed. Please provide token.', False)

    # Validate TOKEN
    validate_token = AuthorizeUser.verify_user_token(header)
    if not validate_token['success']:
        return server_response([], 403, validate_token['message'], False)

    cmp_id = company_id

    check_company_assgment = GetCompanyFromDatabase.check_company_assignment(
        validate_token['response']['email'], company_id
    )

    logger.info(
        '>>> Check_company_assgment: {}'
            .format(check_company_assgment)
    )

    if not check_company_assgment['success']:
        return server_response([], 403, "You don't have permissions for this operation.", False)

    # Validate company
    cmp = GetCompanyFromDatabase.get_company_by_id(cmp_id)
    if not cmp['success']:
        return server_response([], 404, 'Company does not exist.', False)

    # Check import Type
    import_type_check = return_import_type_status_and_import(import_type)
    if not import_type_check['success']:
        return server_response(
            [], 404, import_type_check['response'], import_type_check['success']
        )

    # Token data
    import_id = return_import_type_id(import_type.upper())

    type_api = EnumAPIType.GET.name
    api_name = import_type.upper()

    stat = CompanyMonthlyAPIUsage.insert_or_update_company_api_usage(
        company_id, type_api, api_name
    )
    if not stat['success']:
        return server_response(
            [], 403, stat['message'], False)

    logger.info(
        '>>> API is starting to fetch results for export. Company: {} import type {}'
            .format(company_id, import_type)
    )

    # Start export history from cloud
    export, name = ExportHistory.select_and_return_type_export(cmp_id, import_id)

    for res in export:
        for key, value in res.items():
            if type(value) != str:
                res[key] = '{}'.format(value)
            if value is None:
                res[key] = None

    return server_response(
        export, 200,
        'Export {} successfully generated.'.format(import_type.upper()), True
    )


@app.route('/import/api/<import_type>/<company_id>', methods=['POST', ])
@limiter.limit("1 per second")
def api_rest_import(import_type, company_id):
    header = request.headers.get('Authorization')
    if not header:
        return server_response([], 403, 'Authorization failed. Please provide token.', False)

    validate_token = AuthorizeUser.verify_user_token(header)
    if not validate_token['success']:
        return server_response([], 403, validate_token['message'], False)

    company_id = company_id

    # Validate company
    cmp = GetCompanyFromDatabase.get_company_by_id(company_id)
    if not cmp['success']:
        return server_response([], 404, 'Company does not exist.', False)

    logger.info(
        '>>> API is starting to fetch results from company: {} import type {}'
            .format(company_id, import_type)
    )

    check_company_assgment = GetCompanyFromDatabase.check_company_assignment(
        validate_token['response']['email'], company_id
    )

    logger.info(
        '>>> Check_company_assgment: {}'
            .format(check_company_assgment)
    )

    if not check_company_assgment['success']:
        return server_response([], 403, "You don't have permissions for this operation.", False)

    type_of_request = [x.name for x in ImportType]

    if import_type.upper() not in type_of_request:
        return server_response([], 404, 'Import type not supported: %s' % import_type, False)

    active_import = return_import_type_status(import_type)
    if not active_import:
        return server_response(
            [], 403, 'Import type not supported for now: %s' % import_type, False
        )

    # All allowed status
    logger.info(
        '>>> API is starting to fetch results from company: {} import type {}'
        .format(company_id, import_type)
    )

    elastic_results = (
        function_check_elastic_status(
            company_id, import_type.upper(), EnumProcessType.API.name
        )
    )
    logger.info(
        "Elastic response: {}".format(elastic_results)
    )

    # Get body
    body = request.json
    if not len(body):
        return server_response(
            [], 403, 'Please send data for import type %s to API.' % import_type, False
        )
    # Lets roll with schema
    get_default_parser = return_import_type_based_on_parser(import_type.upper())


    for res in body:
        for key, value in list(res.items()):
            if value is None:
                res.pop(key)

    # See where errors are
    output_response = []
    count = 0
    language = validate_token['response']['language']

    for row in body:
        count += 1
        try:
            validate(row, get_default_parser)
        except jsonschema.exceptions.ValidationError as e:
            output_error = return_errors_from_json_schema(
                row, e, get_default_parser, language, count
            )
            for list_item in output_error:
                output_response.append(list_item)

    # If errors return response to API
    if len(output_response) > 0:
        unique_len = sorted(output_response, key=itemgetter('record'))
        new_d = []
        for x in unique_len:
            if x not in new_d:
                new_d.append(x)
        if len(new_d):
            return server_response(
                new_d, 403, 'Please fix your data.', False
            )

    def generate_new_process():

        # Transform data to specific fields
        prepare_data_for_process = HandleValidationOfAPI.handle_api_data_with_cloud_fields(
            get_default_parser, 'cloud_validator', body
        )

        logger.info('Converted data for cloud: {}'.format(prepare_data_for_process))

        generate_token = (
            generate_elastic_process(
                company_id, import_type.upper(), 'API')
        )

        if generate_token:
            elastic_hash = generate_token['id']

            ElasticCloudLoginFunctions.create_process_flow(
                elastic_hash, EnumErrorType.IN_PROGRESS.name,
                EnumValidationMessage.API_VALIDATION.value['en']
            )

            # Send to API for statistic
            type_api = EnumAPIType.POST.name
            api_name = import_type.upper()

            stat = CompanyMonthlyAPIUsage.insert_or_update_company_api_usage(
                company_id, type_api, api_name
            )
            if not stat['success']:
                return server_response(
                    [], 403, stat['message'], False)
            else:
                publish_file_validation(
                    company_id, elastic_hash, prepare_data_for_process,
                    import_type.upper(), '', 'JWT '+header, ''
                )

                return elastic_hash, True

    hash, stat = generate_new_process()
    if stat:
        return server_response(
            {
                'elastic': hash
            }, 201,
            'Import started, you can watch progress with hash: %s' % (
                hash),
            True
        )


@app.route('/import/file/<import_type>/<company_id>', methods=['POST', ])
def api_rest_file_import(import_type, company_id):
    agent = request.user_agent.browser
    header = request.headers.get('Authorization')
    if not header:
        return server_response([], 403, 'Authorization failed. Please provide token.', False)

    validate_token = AuthorizeUser.verify_user_token(header)
    if not validate_token['success']:
        return server_response([], 403, validate_token['message'], False)

    company_id = company_id

    # Validate company
    cmp = GetCompanyFromDatabase.get_company_by_id(company_id)
    if not cmp['success']:
        return server_response([], 404, 'Company not exists.', False)

    check_company_assgment = GetCompanyFromDatabase.check_company_assignment(
        validate_token['response']['email'], company_id
    )

    if not check_company_assgment['success']:
        return server_response([], 403, "You don't have permissions for this operation.", False)

    type_of_request = [x.name for x in ImportType]

    if import_type.upper() not in type_of_request:
        return server_response([], 403, 'Import type not supported: %s' % import_type, False)

    active_import = return_import_type_status(import_type)
    if not active_import:
        return server_response(
            [], 403, 'Import type not supported for now: %s' % import_type, False
        )

    ema = request.form.get('email', '')
    dels = request.form.get('delimiter', None)

    if dels is None:
        return server_response(
            [], 403, 'Please provide delimiter in request.', False
        )

    if len(dels) > 1:
        return server_response(
            [], 403, 'Wrong delimiters only supported one.', False
        )

    if 'file' not in request.files:
        return server_response([], 403, 'Please provide file in request.', False)

    file = request.files['file']
    if file.filename == '':
        return server_response([], 403, 'Please provide filename for file.', False)

    if not validate_file_extensions(file.filename):
        return server_response(
            [], 403, 'Wrong file extension, not supported. File: %s' % file.filename, False
        )

    # All allowed status
    elastic_allowed_status = elastic_not_allowed_status()

    logger.info(
        'File API is starting to fetch results from company: {} import type {}'
        .format(company_id, import_type)
    )

    elastic_results = (
        function_check_elastic_status(
            company_id, import_type.upper(), EnumProcessType.FILE.name
        )
    )
    logger.info(
        "Elastic response: {}".format(elastic_results)
    )
    # Save file to folder
    filename = secure_filename(file.filename)
    file.save(os.path.join(save_flask_files, filename))
    logger.info("Type of process: {}".format(agent))
    def generate_new_process():
        generate_token = (
            generate_elastic_process(
                company_id, import_type.upper(), 'API' if not agent else 'CLOUD')
        )

        if generate_token:
            elastic_hash = generate_token['id']
            data = {
                'company': company_id,
                'import_type': import_type.upper(),
                'email': ema,
                'delimiters': dels,
                'process_type': 'API' if not agent else 'CLOUD',
                'language': validate_token['response']['language']
            }

            PublishFileToValidationMessageQ.publish_new_data_to_validation(
                data, filename, elastic_hash, 'JWT ' + header)

            return elastic_hash

    if not elastic_results['status']:
        if len(elastic_results['results']) > 0:
            last_status = elastic_results['results'][0]['status']
            if last_status not in elastic_allowed_status:
                hash = generate_new_process()
                return server_response(
                    {
                        'elastic': hash
                    }, 201,
                    'Import started, you can watch progress in cloud interface with hash: %s' % (
                        hash),
                    True
                )
            else:
                return server_response([], 200, 'Please wait to process finish.', True)
        else:
            hash = generate_new_process()
            return server_response(
                {
                    'elastic': hash
                }, 201,
                'Import started, you can watch progress in cloud interface with hash: %s' % (
                    hash),
                True
            )
    else:
        hash = generate_new_process()
        return server_response(
            {
                'elastic': hash
            }, 201,
            'Import started, you can watch progress in cloud interface with hash: %s' % (
                hash),
            True
        )


@app.route('/import/history/all/<company_id>', methods=['GET', ])
def api_client_history(company_id):
    header = request.headers.get('Authorization')
    if not header:
        return server_response([], 403, 'Authorization failed. Please provide token.', False)

    validate_token = AuthorizeUser.verify_user_token(header)
    if not validate_token['success']:
        return server_response([], 403, validate_token['message'], False)

    # Validate company
    cmp = GetCompanyFromDatabase.get_company_by_id(company_id)
    if not cmp['success']:
        return server_response([], 404, 'Company not exists.', False)

    query_all_history = CloudRequestHistory.retrieve_all_history_fail_and_success(company_id)
    query_all_history_vends = CloudVendRequestHistory.retrieve_all_vends_history_fail_and_success(company_id)

    if not query_all_history['status']:
        return server_response(
            [], 200, query_all_history['message'], query_all_history['status']
        )
    elif not query_all_history_vends['status']:
        return server_response(
            [], 200, query_all_history_vends['message'], query_all_history_vends['status']
        )

    return server_response(
        query_all_history['results'], 200, 'All data for company : %s' % company_id,
        query_all_history['status']
    )


from common.logging.setup import logger


@app.route('/import/update-cloud-process', methods=['POST',])
def api_cloud_response():
    from common.mixin.enum_errors import EnumValidationMessage as Const
    from common.mixin.enum_errors import enum_message_on_specific_language

    cloud_data = request.json
    logger.info('<<< Cloud is sending data: {}'.format(cloud_data))
    company_id = cloud_data['request_data'].get('company_id')
    elastic_hash = cloud_data['request_data'].get('elastic_hash')
    import_type = cloud_data['request_data'].get('type')
    email = cloud_data['request_data'].get('email')  # Email from cloud
    elastic_prepare = cloud_data.get('request_data')

    def insert_into_elastic(data, hash, progress):
        ElasticCloudLoginFunctions.create_cloud_process_flow(
            hash, progress, json.dumps(data))

        ElasticCloudLoginFunctions.create_process_flow(
            hash, progress, json.dumps(data))
        return

    def insert_partial(data, hash, progress):
        ElasticCloudLoginFunctions.create_process_flow(
            hash, progress, json.dumps(data))
        return

    error_name = ''
    msg_nok = cloud_data['messages_nok']
    msg_ok = cloud_data['messages_ok']
    statistics = cloud_data['statistics']
    ultimate_error = cloud_data['ultimate_error']
    message = ''
    partial = False
    if len(msg_nok) and not len(ultimate_error):
        partial = True
        if msg_ok:
            error_name += EnumErrorType.WARNING.name
            message += 'Data was partially imported into cloud database {}'.format(import_type)
        else:
            error_name += EnumErrorType.ERROR.name
            message += 'Data was not imported into cloud database {}'.format(import_type)
        insert_into_elastic(msg_nok, elastic_hash, error_name)
        insert_msg = enum_message_on_specific_language(
            Const.DATABASE_STATISTICS_RESULT.value, 'en', statistics['inserted_count'], statistics['updated_count'],
            statistics['deleted_count'], statistics.get('reused_count', 0), len(msg_nok)
        )
        insert_into_elastic(insert_msg, elastic_hash, error_name)
    elif len(ultimate_error) and len(msg_nok):
        partial = True
        error_name += EnumErrorType.ERROR.name
        if msg_ok:
            message += 'Data was partially imported into cloud database {}'.format(import_type)
        else:
            message += 'Data was not imported into cloud database {}'.format(import_type)
        insert_into_elastic(msg_nok, elastic_hash, error_name)
        insert_msg = enum_message_on_specific_language(
            Const.DATABASE_STATISTICS_RESULT.value, 'en', statistics['inserted_count'], statistics['updated_count'],
            statistics['deleted_count'], statistics.get('reused_count', 0), len(msg_nok)
        )
        insert_into_elastic(insert_msg, elastic_hash, error_name)
        insert_into_elastic(ultimate_error, elastic_hash, error_name)
    elif len(ultimate_error) and not len(msg_nok):
        partial = True
        error_name += EnumErrorType.ERROR.name
        insert_into_elastic(ultimate_error, elastic_hash, error_name)
    else:
        error_name += EnumErrorType.SUCCESS.name
        message += 'Data imported into cloud database {}'.format(import_type)
        insert_msg = enum_message_on_specific_language(
            Const.DATABASE_STATISTICS_RESULT.value, 'en', statistics['inserted_count'], statistics['updated_count'],
            statistics['deleted_count'], statistics.get('reused_count', 0), len(msg_nok)
        )
        insert_into_elastic(insert_msg, elastic_hash, error_name)

    # Start elastic process
    ParseRabbitRequests.save_logging_message(elastic_prepare, error_name, message)

    prepare_insert = {
        'inserted': statistics.get('inserted_count', 0),
        'updated': statistics.get('updated_count', 0),
        'deleted': statistics.get('deleted_count', 0),
        'reused': statistics.get('reused_count', 0),
        'errors': len(msg_nok)
    }

    update_method = (
        CompanyHistory.update_local_finish_process(
            elastic_hash, statistics, prepare_insert, partial)
    )
    if not update_method:
        logger.info('Error update local process: {} {}'.format(import_type, cloud_data))
    logger.info('Updated local process: {} {}'.format(import_type, cloud_data))

    # Cloud update redis key, process for this company finished, because this is important for next import!
    redis_import_process_handler = ImportProcessHandler(
        company_id=company_id,
        elastic_hash=elastic_hash,
        file_path='',
        import_type=import_type,
    )
    redis_import_process_handler.finish_import_process_redis(finished_by='importer api', reason='api_cloud_response')

    if email:
        import threading
        prepared_data = {
            'company_id': int(company_id),
            'hash': elastic_hash,
            'email': email,
            'import_type': return_import_object_type(import_type)
        }
        logger.info('<<< Sending email: {}'.format(prepared_data))
        threading.Thread(
            target=ExportHistory.export_specific_hash, args=(prepared_data,), daemon=True
        ).start()

    return server_response(
        [], 200, 'Inserted into flask.',
        True
    )


@app.route('/import/vend_update-cloud-process', methods=['POST', ])
def api_vend_cloud_response():

    cloud_data = request.json
    
    request_data = cloud_data['request_data']
    
    vend_logger.info('<<< Cloud is sending data: {}'.format(cloud_data))
    email =request_data.get('email')
    elastic_hash = request_data.get('elastic_hash')
    token = request_data.get('token')
    import_type = request_data.get('import_type')
    company_id = request_data.get('company')
    success_path = request_data.get('success_file_path', '')
    elastic_prepare = cloud_data['request_data']
    ultimate_error = cloud_data['ultimate_error']
    reimport_error = cloud_data['reimport_error']
    statistics = cloud_data['statistics']
    msg_nok = cloud_data['messages_nok']
    msg_ok = cloud_data['messages_ok']
    partial = False
    status = ""
    message = ""

    def insert_into_elastic_general(elastic_message, elastic, status):
        VendImportProcessLogger.create_cloud_validation_process_flow(
            status=status,
            process_hash=elastic,
            message=elastic_message
        )

        VendImportProcessLogger.create_importer_validation_process_flow(
            status=status,
            process_hash=elastic,
            message=elastic_message
        )
        return

    if len(reimport_error):
        status = EnumErrorType.WARNING.name
        reimport_filename = ''.join([str(d['reimport_filename']) for d in reimport_error if 'reimport_filename' in d])
        reimport_file_vends = int(
            ''.join([str(d['reimport_file_vends']) for d in reimport_error if 'reimport_file_vends' in d])
        )
        import_file_vends = int(
            ''.join([str(d['import_file_vends']) for d in reimport_error if 'import_file_vends' in d])
        )

        insert_msg = enum_message_on_specific_language(Const.ERROR_ON_REIMPORT.value, 'en', reimport_filename,
                                                       reimport_file_vends, import_file_vends)

        insert_into_elastic_general(insert_msg, elastic_hash, status)

    if len(msg_nok) and not len(ultimate_error):
        partial = True
        if msg_ok:
            status += EnumErrorType.WARNING.name
            message += enum_message_on_specific_language(Const.PARTIALLY_VEND_IMPORT.value, 'en', import_type)
        else:
            status += EnumErrorType.ERROR.name
            message += enum_message_on_specific_language(Const.NO_VEND_CLOUD_IMPORT.value, 'en', import_type)

        vend_logger.error(message)

        cloud_nok_msg = enum_message_on_specific_language(Const.CLOUD_IMPORT_ERROR_LIST.value, 'en', msg_nok)

        insert_into_elastic_general(cloud_nok_msg, elastic_hash, EnumErrorType.WARNING.name)

        insert_msg = enum_message_on_specific_language(Const.NEW_VEND_CLOUD_MESSAGE.value, 'en', statistics['inserted_count'])

        insert_into_elastic_general(insert_msg, elastic_hash, status)

    elif len(ultimate_error) and len(msg_nok):
        partial = True
        status += EnumErrorType.ERROR.name
        if msg_ok:
            message += enum_message_on_specific_language(Const.PARTIALLY_VEND_IMPORT.value, 'en', import_type)
        else:
            message += enum_message_on_specific_language(Const.NO_VEND_CLOUD_IMPORT.value, 'en', import_type)

        vend_logger.error(message)

        cloud_nok_msg = enum_message_on_specific_language(Const.CLOUD_IMPORT_ERROR_LIST.value, 'en', msg_nok)

        insert_into_elastic_general(cloud_nok_msg, elastic_hash, EnumErrorType.WARNING.name)

        insert_msg = enum_message_on_specific_language(
            Const.NEW_VEND_CLOUD_MESSAGE.value, 'en', statistics['inserted_count']
        )

        insert_into_elastic_general(insert_msg, elastic_hash, status)
        vend_logger.error(ultimate_error)

    elif len(ultimate_error) and not len(msg_nok):
        partial = True
        status += EnumErrorType.ERROR.name
        message += enum_message_on_specific_language(Const.CLOUD_IMPORT_GENERAL_FAIL.value, 'en')
        insert_into_elastic_general(message, elastic_hash, status)
        vend_logger.error(ultimate_error)

    else:
        status += EnumErrorType.SUCCESS.name
        message += enum_message_on_specific_language(Const.DATA_IMPORT_INTO_CLOUD.value, 'en', import_type)
        insert_msg = enum_message_on_specific_language(
            Const.NEW_VEND_CLOUD_MESSAGE.value, 'en', statistics['inserted_count']
        )
        insert_into_elastic_general(insert_msg, elastic_hash, status)

    ParseRabbitRequests.vend_save_logging_message(elastic_prepare, status, message)

    insert_method = CompanyHistory.vend_insert_history(
        company_id=company_id,
        import_type=import_type,
        elastic_hash=elastic_hash,
        file_path=success_path,
        cloud_result=statistics,
        statistics=statistics,
        token=token,
        cloud_inserted=True,
        vend_import_json=cloud_data,
        partial=partial
    )

    if not insert_method:
        vend_logger.info('Error update local process: {} {}'.format(import_type, cloud_data))
    vend_logger.info('Updated local process: {} {}'.format(import_type, cloud_data))

    if email:
        import threading
        prepared_data = {
            'company_id': int(company_id),
            'hash': elastic_hash,
            'email': email,
            'import_type': return_import_object_type(import_type)
        }
        vend_logger.info('<<< Sending email: {}'.format(prepared_data))
        threading.Thread(
            target=ExportHistory.vend_export_specific_hash, args=(prepared_data,), daemon=True
        ).start()

    return server_response(
        [], 200, 'Inserted into flask.',
        True
    )
