import codecs
import csv
import json
import traceback
from operator import itemgetter
import ftputil
import os
import shutil
import re
import time
import jsonschema
from jsonschema import validate, FormatChecker
import pandas as pd
from common.mixin.enum_errors import EnumValidationMessage as enum_msg
from common.mixin.enum_errors import enum_message_on_specific_language
from common.mixin.enum_errors import EnumErrorType
from common.mixin.ftp import MySession, sftp_download_file
from common.mixin.handle_file import CsvValidatorHandleFIle as handle_file, ImportProcessHandler
from common.mixin.mixin import (generate_hash_for_json, return_errors_from_json_schema, delete_processed_file,
                                import_file_content_to_json, mandatory_geo_location)
from common.mixin.validation_const import (
    return_import_type_name, ALLOWED_EXTENSIONS,
    return_import_type_based_on_parser,
    return_all_fields_and_mandatory, ImportType, Enum_validation_error, return_import_type_id_custom_validation)
from common.mixin.validator_import import (
    escape_list, WORKING_DIR, HISTORY_FAIL_DIR, STORE_DIR, HISTORY_SUCCESS_DIR)
from common.rabbit_mq.validator_file_q.validator_publisher import publish_file_validation
from core.flask.sessions.session import AuthorizeUser
from database.cloud_database.core.query import CustomUserQueryOnCloud
from database.company_database.core.query_export import ExportHistory
from database.company_database.core.query_history import CompanyFailHistory
from common.validators.cloud_db.cloud_validator import CompanyHistory
from common.mixin.elastic_login import ElasticCloudLoginFunctions
from common.mixin.mixin import generate_uid
from common.logging.setup import logger
from common.email.send_email import send_email_on_general_error
from elasticsearch_component.core.logger import CompanyProcessLogger

logger_api = logger


class ParseFileBasedOnExtension(object):
    """
    This class make convert xls/xlsx file to csv using pandas, and make elastic and system  logging
    """

    def __init__(self, file, file_delimiter, process_logger):
        self.file_parse = file
        self.delimiter = file_delimiter
        self.process_logger= process_logger


    def return_file_extension(self):
        """

        :return: file extension
        """
        file_extension = os.path.splitext(self.file_parse)[1]
        if file_extension in ALLOWED_EXTENSIONS:
            return file_extension.replace('.', ''), os.path.splitext(self.file_parse)[0]
        return False, None

    def convert_xlsx_with_csv_reader(self):
        """
        Attempt to read file, using something other than pandas
        """
        try:
            values_data = []
            with codecs.open(self.file_parse, "r", encoding='utf-8',
                             errors='ignore') as in_file:
                csv_reader = csv.reader(in_file, delimiter=self.delimiter)
                headers = next(csv_reader)
                for line in csv_reader:
                    values_entry = dict()
                    for i in range(len(headers)):
                        try:
                            values_entry[headers[i]] = line[i]
                        except IndexError as e:
                            values_entry[headers[i]] = ''
                            logger_api.info(self.process_logger.update_system_log_flow(
                                i,e,key_enum=enum_msg.VALIDATION_SYSTEM_LOG_CONTENT_INFO.value)
                            )

                    values_data.append(values_entry)
            new_decoded_file_content = os.path.splitext(self.file_parse)[0] + '.csv'
            logger_api.info(self.process_logger.update_system_log_flow(
                new_decoded_file_content,
                key_enum=enum_msg.VALIDATION_SYSTEM_LOG_INFO_CONVERTING_FILE.value)
            )

            with open(new_decoded_file_content, 'w') as new_decoded_content:
                json.dump(values_data, new_decoded_content)
            return {'success': True, 'file_path': new_decoded_file_content}

        except Exception:
            logger_api.info(self.process_logger.update_system_log_flow(
                self.file_parse,
                key_enum=enum_msg.VALIDATION_SYSTEM_LOG_ERROR_CONVERTING_FILE.value)
            )
            return {'success': False, 'file_path': self.file_parse}

    def open_xlsx_file_and_convert_to_csv(self, new_file_path):
        """
        Try to open excel file with pandas library

        Keyword arguments:
        new_file_path -- path to the excel file
        """
        # converting xls/xlsx file to csv using Pandas reader
        try:
            new_file_path_save = new_file_path + '.csv'
            work_book = pd.read_excel(self.file_parse)
            work_book.to_csv(new_file_path_save, encoding='utf-8')

            logger_api.info(self.process_logger.update_system_log_flow(
                new_file_path_save,key_enum=enum_msg.VALIDATION_SYSTEM_LOG_INFO_CONVERTED_FILE.value)
            )
            return {'success': True, 'file_path': new_file_path_save}
        except Exception:
            logger_api.error(self.process_logger.update_system_log_flow(
                traceback.format_exc(), key_enum=enum_msg.VALIDATION_SYSTEM_LOG_PANDAS_ERROR.value)
            )
            convert_status = self.convert_xlsx_with_csv_reader()
            return convert_status

    def start_checking_file(self):
        """
        Based on file extension, process file and return appropriate messages.
        """
        # Check file extension
        try:
            file_extension, path = self.return_file_extension()
            if not file_extension:
                return None
            if file_extension == 'xlsx' or file_extension == 'xls':
                # if file was OK converted, return full file path
                if self.open_xlsx_file_and_convert_to_csv(path)['success']:
                    ok_file_convert = self.open_xlsx_file_and_convert_to_csv(path)['file_path']
                    return {'success': True, 'file_path': ok_file_convert}
                else:
                    # if we cant convert file to csv return old/non converted file!
                    nok_file_convert = self.open_xlsx_file_and_convert_to_csv(path)['file_path']
                    return {'success': False, 'file_path': nok_file_convert}
            elif file_extension == 'csv':
                return {'success': True, 'file_path': path + '.csv'}
        except Exception as e:
            logger_api.info(self.process_logger.update_system_log_flow(
                self.file_parse, e,
                key_enum=enum_msg.VALIDATION_FILE_ERROR.value)
            )
            return {'success': False, 'file_path': ''}


class ProcessLogger(object):
    """

    Main logging for elastic (cloud, importer), system log and insert into fail history database.
    """

    def __init__(self, company_id, import_type, elastic_hash, token):
        self.company_id = company_id
        self.import_type = import_type
        self.elastic_hash = elastic_hash
        self.token = token
        self.data_hash = ''

    def set_data_hash(self, data_hash):
        self.data_hash = data_hash

    def create_process_and_cloud_flow_and_main(self, *args, error=None, file_path=None, email=None,
                                               language=None,  key_enum=None, message=None):
        """

        :param args: format error arguments for enum message
        :param error: validation status error/fail
        :param file_path: path to the file
        :param email: email
        :param language: selected language for elastic message
        :param key_enum: error enum key
        :return: update main process with fail/error status, insert into fail history database,
        generated elastic message on selected language for cloud and importer
        """

        self.insert_into_fail_history(error.value, self.data_hash, file_path, email)
        self.update_process_and_cloud_flow(*args, error=error.name, language=language,
                                           key_enum=key_enum, message=message)
        ElasticCloudLoginFunctions.update_main_process(
            hash=self.elastic_hash, error=error.name
        )

    def update_process_and_cloud_flow(self, *args, error=None, language=None, key_enum=None, message=None):
        """

        :param args: format error arguments for enum message
        :param error: validation status error/fail
        :param language: selected language for elastic message
        :param key_enum: error enum key
        :return: generated elastic message on selected language for cloud and importer.
        """
        if not message:
            message = enum_message_on_specific_language(key_enum, language, *args)

        ElasticCloudLoginFunctions.create_cloud_process_flow(
            hash=self.elastic_hash, error=error, message=message
        )

        self.update_process_flow(error=error, message=message, key_enum=key_enum, *args)

    def update_process_flow(self, *args, error=None, language='en', message=None, key_enum=None):
        """

        :param args: format for error elastic message
        :param error: validation status error/fail
        :param language: selected language for elastic message
        :param message: already generated elastic message
        :param key_enum: key error for elastic message
        :return: generated elastic message for importer
        """

        if not message:
            message = enum_message_on_specific_language(key_enum, language, *args)
        ElasticCloudLoginFunctions.create_process_flow(
            hash=self.elastic_hash, error=error, message=message
        )

    def update_system_log_flow(self,*args, language='en',  key_enum=None):
        """

        :param args: format arguments for error message
        :param key_enum: main enum for error message
        :param language: default eng language for system log
        :return: message on selected language (default eng)
        """

        message = enum_message_on_specific_language(key_enum, language, *args)
        return message

    def insert_into_fail_history(self, error, data_hash, file_path, email):
        """

        :param error: validation status
        :param data_hash: json hash generated from input file
        :param file_path: path to the file
        :param email: email
        :return: insert all specific data into fail database with fail/error status, and send mail
        """
        file_name = os.path.split(file_path)[1]
        file_extension = os.path.splitext(file_path)[1]

        if not data_hash:
            uid_generator = generate_uid()
        else:
            uid_generator = data_hash
        if os.path.isfile(file_path):
            try:
                shutil.copy2(file_path, os.path.join(HISTORY_FAIL_DIR, file_name))
                os.rename(
                    os.path.join(HISTORY_FAIL_DIR, file_name),
                    os.path.join(HISTORY_FAIL_DIR,
                                 '{}_{}${}'.format(self.company_id, uid_generator, file_name))
                )
                os.remove(file_path)
                if os.path.isfile(os.path.join(STORE_DIR, file_name)):
                    os.remove(os.path.join(STORE_DIR, file_name))
                if file_extension != '.csv':
                    file_path_remove = str(os.path.splitext(file_name)[0]) + '.csv'
                    full_path_to_file = os.path.join(WORKING_DIR, file_path_remove)
                    if os.path.isfile(full_path_to_file):
                        os.remove(os.path.join(WORKING_DIR, file_path_remove))
            except Exception:
                logger_api.error(self.update_system_log_flow(
                    traceback.print_exc(),
                    key_enum=enum_msg.VALIDATION_SYSTEM_LOG_FAIL_HISTORY.value)
                )
        CompanyFailHistory.insert_fail_history(
            company_id=self.company_id,
            import_type=self.import_type,
            elastic_hash=self.elastic_hash,
            data_hash=data_hash,
            file_path=os.path.join(HISTORY_FAIL_DIR, '{}_{}${}'.format(self.company_id, uid_generator, file_name)),
            import_error_type=error,
            token=self.token
        )

        # Send email if email exists
        if email:
            import threading
            prepared_data = {
                'company_id': self.company_id,
                'hash': self.elastic_hash,
                'email': email,
                'import_type': self.import_type
            }
            threading.Thread(
                target=ExportHistory.export_specific_hash, args=(prepared_data,), daemon=True
            ).start()


def generate_elastic_process(company_id, import_type, process_request_type):
    """
    :return: created main elastic process
    """
    process = ElasticCloudLoginFunctions.create_process(
        company_id, import_type, process_request_type
    )
    return process


class CsvFileValidatorRemote(object):
    """
    Download file from FTP, make basic validation on file format and create elastic process and
    delete file from FTP server
    """

    def __init__(self, input_params):
        self.company_id = input_params['company']
        self.host = input_params['url']
        self.username = input_params['username']
        self.password = input_params['password']
        self.port = int(input_params['port']) if input_params['port'] else 21
        self.path = input_params['ftp_path']
        self.delimiter = input_params['file_delimiters']
        self.emails = input_params['email']
        self.import_type_execute = int(input_params['category_import'])
        self.downloaded_files = []
        self.user_token = CustomUserQueryOnCloud.get_auth_token_from_user_id(
            int(input_params['user_id'])
        )
        if not self.user_token or self.user_token['status'] is False:
            logger_api.error('Error getting user {} -> {} '.format(input_params.get('user_id', '-'), str(self.user_token)))
            raise Exception('Error getting user {} -> {}' .format(input_params.get('user_id', '-'), str(self.user_token)))

        self.el_hash = None

        self.elastic_hash = ElasticCloudLoginFunctions.create_process(
            company_id=self.company_id,
            import_type=self.import_type_execute,
            process_request_type='FILE',
        )
        self.process_logger = ProcessLogger(self.company_id, self.import_type_execute,
                                            self.elastic_hash, self.user_token['token'])
        self.sorted_files_list = []

    def get_remote_file(self):
        """

        :return: download file from FTP
        """
        from datetime import datetime
        # If token is not defined, make elastic, global and cloud logging then update main process!
        if not self.user_token['status']:
            self.process_logger.create_process_and_cloud_flow_and_main(
                error=EnumErrorType.FAIL,
                file_path='',
                email=self.emails,
                language='en',
                key_enum=enum_msg.USER_ERROR.value
            )

            return {"success": False}

        # If can't connect to FTP server, make logging and update main process.
        if self.port != 22:
            try:
                ftp_connect = ftputil.FTPHost(self.host, self.username,
                                              self.password, port=self.port,
                                              session_factory=MySession)
                logger_api.info(self.process_logger.update_system_log_flow(
                    self.username, self.host, self.port,
                    key_enum=enum_msg.FTP_CONNECTED.value)
                )
            except Exception as e:
                self.process_logger.create_process_and_cloud_flow_and_main(
                    error=EnumErrorType.ERROR,
                    file_path='',
                    email=self.emails,
                    language='en',
                    key_enum=enum_msg.FTP_CONNECTION_ERROR.value
                )
                return {"success": False}

            # If can't change directory on remote FTP server make logging and update main process.
            with ftp_connect as ftp_host:
                try:
                    ftp_host.chdir(self.path)
                    os.chdir(STORE_DIR)
                    logger_api.info(self.process_logger.update_system_log_flow(
                        self.path,
                        key_enum=enum_msg.FILE_RIGHT_PATH.value)
                    )
                except Exception:
                    self.process_logger.create_process_and_cloud_flow_and_main(
                        traceback.print_exc(),
                        error=EnumErrorType.ERROR,
                        file_path='',
                        email=self.emails,
                        language='en',
                        key_enum=enum_msg.FTP_PATH_ERROR.value
                    )
                    return {"success": False}

                file_names = ftp_host.listdir(ftp_host.curdir)

                logger_api.info(self.process_logger.update_system_log_flow(
                    file_names,
                    key_enum=enum_msg.FTP_FILE_LIST.value)
                )

                # Try to download file from FTP server.
                for file_name in file_names:
                    try:
                        if ftp_host.path.isfile(file_name) and file_name not in escape_list:
                            extension = os.path.splitext(os.path.join(STORE_DIR, file_name))[1]
                            time_file = datetime.fromtimestamp(
                                float(ftp_host.path.getmtime(file_name))
                            ).strftime('%Y-%m-%d %H:%M:%S')

                            logger_api.info(self.process_logger.update_system_log_flow(
                                file_name,
                                key_enum=enum_msg.FTP_START_DOWNLOAD_FILE.value)
                            )

                            ftp_host.download(file_name, os.path.join(STORE_DIR, file_name))

                            new_filename_without_space = re.sub('\s+', '_', file_name).strip()
                            os.rename(os.path.join(STORE_DIR, file_name),
                                      new_filename_without_space)

                            logger_api.info(self.process_logger.update_system_log_flow(
                                file_name, new_filename_without_space,
                                key_enum=enum_msg.FTP_DOWNLOADED_AS.value)
                            )

                            # Decide if file has wrong format and remove file with wrong format!
                            try:
                                if extension not in ALLOWED_EXTENSIONS:
                                    self.process_logger.update_process_and_cloud_flow(
                                        file_name,
                                        error=EnumErrorType.FAIL.name,
                                        language='en',
                                        key_enum=enum_msg.FILE_WRONG_FORMAT.value
                                    )

                                    if os.path.isfile(os.path.join(
                                            STORE_DIR, new_filename_without_space)):
                                        os.remove(os.path.join(
                                            STORE_DIR, new_filename_without_space))
                                else:
                                    self.downloaded_files.append(
                                        {
                                            'name': new_filename_without_space,
                                            'time': time_file
                                        }
                                    )
                            except Exception:
                                logger_api.info(self.process_logger.update_system_log_flow(
                                    traceback.print_exc(), self.host, new_filename_without_space,
                                    key_enum=enum_msg.VALIDATION_SYSTEM_LOG_WRONG_FORMAT_FTP.value)
                                )
                                pass

                    except Exception as e:

                        self.process_logger.update_process_and_cloud_flow(
                            file_name,
                            error=EnumErrorType.FAIL.name,
                            language='en',
                            key_enum=enum_msg.FTP_DOWNLOAD_ERROR.value,
                        )
                    try:
                        ftp_host.remove(file_name)
                        logger_api.info(self.process_logger.update_system_log_flow(
                            file_name,
                            key_enum=enum_msg.FTP_DELETED_FILE.value)
                        )
                    except Exception:
                        logger_api.error(self.process_logger.update_system_log_flow(
                            traceback.print_exc(),
                            key_enum=enum_msg.VALIDATION_SYSTEM_LOG_ERROR_REMOVING_FTP_FILE.value)
                        )

                        pass
        elif self.port == 22:
            downloaded_files_resultset = sftp_download_file(
                self.host, self.username,
                22, self.password,
                self.path, STORE_DIR,
                self.emails,
                self.process_logger,
            )

            if downloaded_files_resultset["success"]:
                self.downloaded_files = downloaded_files_resultset["file_list"]
            else:
                return

        if len(self.downloaded_files) == 0:
            logger_api.error(self.process_logger.update_system_log_flow(
                self.company_id, self.elastic_hash, self.host, self.port, self.path,
                key_enum=enum_msg.VALIDATION_SYSTEM_LOG_FTP_ERROR.value)
            )

            ElasticCloudLoginFunctions.update_main_process(
                hash=self.elastic_hash, error=EnumErrorType.FAIL.name)
            self.process_logger.update_process_and_cloud_flow(
                self.host,
                error=EnumErrorType.FAIL.name,
                language='en',
                key_enum=enum_msg.VALIDATION_MAIN_PROCESS_FTP.value,
            )
            return
        self.sorted_files_list = sorted(self.downloaded_files, key=lambda k: k["time"])
        return self.sorted_files_list


    def process_ftp_files(self):
        """

        :return: send file to analyze, and make logging
        """

        generate_hash = generate_elastic_process(self.company_id, self.import_type_execute, 'FILE')

        for file in self.sorted_files_list:

            initial_process = ProcessLogger(
                self.company_id, self.import_type_execute, generate_hash, self.user_token['token'])

            initial_process.update_process_flow(
                file['name'],
                error=EnumErrorType.IN_PROGRESS.name,
                language='en',
                key_enum=enum_msg.FTP_SUCCESSFULLY_DOWNLOAD_FILE.value,
            )

            parameters = {
                'company': self.company_id,
                'delimiters': self.delimiter,
                'email': self.emails,
                'import_type': self.import_type_execute,
                'process_request_type': 'FILE'
            }

            CsvFileValidatorLocal(parameters, file['name'], generate_hash, self.user_token['token'],
                                  api_request=False).validation_for_specific_file()
            time.sleep(2)


class CsvFileValidatorLocal(object):
    """

    Make main validation on file format, handle zip file and send file on validation content
    """

    def __init__(self, input_params, filename, elastic_hash, token, api_request=True):
        self.api_request = api_request
        self.company_id = input_params['company']
        self.delimiter = input_params['delimiters']
        self.email = input_params['email']
        self.import_type = input_params['import_type']
        self.process_request_type = input_params.get('process_request_type', None)
        self.filename = filename
        self.language = 'en'
        token_check = AuthorizeUser.verify_user_token(token.replace('JWT ', ''))
        self.token_check = (
            token_check['response']['language'] if token_check['response']['language'] else 'en'
        )
        self.elastic_hash = elastic_hash
        self.token = token
        self.email = input_params['email']
        self.process_logger = ProcessLogger(self.company_id, self.import_type,
                                            self.elastic_hash, self.token)

        self.helper_methods_main = handle_file(self.filename, self.import_type, self.company_id,
                                         self.process_logger, self.delimiter, self.token)
        self.fields_in_document = []


    def validation_for_specific_file(self):
        """

        :return: processing specific file, that was uploaded in "working_dir"!
        """
        extension = self.helper_methods_main.check_file_extension()
        zip_path = os.path.join(STORE_DIR, self.filename)
        zip_extension = ['.zip']
        process_request_type = self.process_request_type if self.process_request_type else 'CLOUD'

        if extension in zip_extension:
            zip_file_data = self.helper_methods_main.handle_zip_file(zip_path)
            zip_responses = []
            if len(zip_file_data):
                # Update zip main process, with status success if we get list of unzip files!
                ElasticCloudLoginFunctions.update_main_process(
                    hash=self.elastic_hash, error=EnumErrorType.SUCCESS.name)
                self.process_logger.update_process_and_cloud_flow(
                    self.filename,
                    error=EnumErrorType.SUCCESS.name,
                    language=self.language,
                    key_enum=enum_msg.FILE_ZIP_PROCESSING.value,
                )
                for working_file_path in zip_file_data:
                    work_filename = os.path.basename(working_file_path)
                    # check file extension for every zip file from list
                    zip_elastic_hash = generate_elastic_process(
                        self.company_id, self.import_type, process_request_type)
                    zip_process_logger = ProcessLogger(self.company_id, self.import_type,
                                                       zip_elastic_hash,
                                                       self.token)

                    self.helper_methods_zip = handle_file(
                        working_file_path, self.import_type, self.company_id, zip_process_logger,
                        self.delimiter, self.token
                    )

                    extension = self.helper_methods_zip.check_file_extension()

                    # if wrong format delete file from working dir and make only system logging
                    if extension not in ALLOWED_EXTENSIONS:
                        delete_processed_file(working_file_path, zip_process_logger)

                        logger_api.error(self.process_logger.update_system_log_flow(
                            work_filename,
                            key_enum=enum_msg.FILE_WRONG_FORMAT.value))

                        ElasticCloudLoginFunctions.update_main_process(
                            hash=zip_elastic_hash, error=EnumErrorType.ERROR.name
                        )
                    else:
                        # generate elastic hash for every valid file format and make  logging

                        zip_process_logger.update_process_flow(
                            work_filename,
                            error=EnumErrorType.IN_PROGRESS.name,
                            language=self.language,
                            key_enum=enum_msg.FILE_START_VALIDATION_FORMAT.value,
                        )

                        if os.path.getsize(working_file_path) == 0:
                            zip_process_logger.create_process_and_cloud_flow_and_main(
                                self.import_type,
                                error=EnumErrorType.ERROR,
                                file_path=working_file_path,
                                email=self.email,
                                language=self.language,
                                key_enum=enum_msg.FILE_EMPTY.value
                            )

                            return {'file': work_filename, "success": False}

                        zip_process_logger.update_process_flow(
                            work_filename,
                            error=EnumErrorType.IN_PROGRESS.name,
                            language=self.language,
                            key_enum=enum_msg.FILE_WITH_OK_FORMAT.value,
                        )
                        zip_process_logger.update_process_flow(
                            work_filename,
                            error=EnumErrorType.IN_PROGRESS.name,
                            language=self.language,
                            key_enum=enum_msg.FILE_START_VALIDATION_FIELDS.value,
                        )

                        call_validation_for_zip_file = FieldValidation(
                                self.language, zip_process_logger, self.email, self.company_id,
                                self.import_type, self.token, working_file_path, process_request_type,
                                self.delimiter, zip_elastic_hash, api_request=self.api_request
                            )

                        zip_responses.append(call_validation_for_zip_file.get_validation_fields())
                    time.sleep(2)
        else:
            if extension not in ALLOWED_EXTENSIONS:
                self.process_logger.create_process_and_cloud_flow_and_main(
                    self.import_type,
                    error=EnumErrorType.ERROR,
                    file_path=os.path.join(WORKING_DIR, self.filename),
                    email=self.email,
                    language=self.language,
                    key_enum=enum_msg.FILE_WRONG_FORMAT.value
                )

                return {'file': self.filename, "success": False}

            validation_start_args= os.path.basename(self.filename)
            self.process_logger.update_process_flow(
                validation_start_args,
                error=EnumErrorType.IN_PROGRESS.name,
                language=self.language,
                key_enum=enum_msg.FILE_START_VALIDATION_FORMAT.value,
            )
            responses = []
            file_path = os.path.join(STORE_DIR, self.filename)
            try:
                if os.path.isfile(file_path):
                    shutil.copy2(file_path, WORKING_DIR)
                    real_file_path= os.path.join(WORKING_DIR, os.path.basename(self.filename))
                    call_validation_for_other_file = FieldValidation(
                            self.language, self.process_logger, self.email, self.company_id,
                            self.import_type, self.token, real_file_path, process_request_type,
                            self.delimiter, self.elastic_hash, api_request=self.api_request)
                    if os.path.getsize(real_file_path) == 0:
                        self.process_logger.create_process_and_cloud_flow_and_main(
                            self.import_type,
                            error=EnumErrorType.ERROR,
                            file_path=real_file_path,
                            email=self.email,
                            language=self.language,
                            key_enum=enum_msg.FILE_EMPTY.value
                        )
                        return {'file': self.filename, "success": False}

                    self.process_logger.update_process_flow(
                        os.path.basename(self.filename),
                        error=EnumErrorType.IN_PROGRESS.name,
                        language=self.language,
                        key_enum=enum_msg.FILE_WITH_OK_FORMAT.value
                    )
                    self.process_logger.update_process_flow(
                        os.path.basename(self.filename),
                        error=EnumErrorType.IN_PROGRESS.name,
                        language=self.language,
                        key_enum=enum_msg.FILE_START_VALIDATION_FIELDS.value
                    )

                    responses.append(call_validation_for_other_file.get_validation_fields())
            except Exception:
                logger_api.error(self.process_logger.update_system_log_flow(
                    traceback.format_exc(), key_enum=enum_msg.RENAME_LOCAL_FILE_ERROR.value))


class FieldValidation(object):
    """
    This class basically make main validation of file content, also in this part we checking fail
    history database (if file with same json hash has imported three times brake operation),
    also we check success history database, if file was successfully pass validation in this part
    we send json data to the rabbitMQ.

    """
    def __init__(self, language, process_logger, email, company_id, import_type, token,
                 filename_path, process_request_type, delimiter, elastic_hash, api_request=True):
        self.api_request = api_request
        self.process_logger = process_logger
        self.email = email
        self.company_id = company_id
        self.import_type = import_type
        self.token = token
        self.filename_path = filename_path
        self.process_request_type = process_request_type
        self.delimiter = delimiter
        self.elastic_hash = elastic_hash
        self.language = language
        self.helper_methods_validations = handle_file(
            self.filename_path, self.import_type, self.company_id, self.process_logger,
            self.delimiter, self.token
        )

    def parse_dictionary_for_cloud(self, working_file, parser):
        """

        :param working_file: file for validation
        :param parser: parser type (location, machine, machine_type, regions)
        :return: main list of data content type for cloud
        """
        cloud_data = self.helper_methods_validations.serializer_data(working_file, parser,
                                                                     'cloud_validator')
        return cloud_data


    def get_output_response(self, working_file, parser):
        """

        :param working_file: file for validation
        :param parser: parser type (location, machine, machine_type, regions)
        :return: main list of data content type for importer and json schema validation
        """
        importer_data = self.helper_methods_validations.serializer_data(working_file, parser,
                                                                        'csv_validator')
        return importer_data

    @staticmethod
    def clean_meter_readings(row):
        """
        This cleans meter readings from data

        :param row: import row
        :return:
        """
        row.pop('meter_readings_list')

    def publish_to_cloud_validator(self, output_values_for_cloud, cloud_success_csv_file_path,
                                   working_validation_filename):
        publish_file_validation(
            self.company_id, self.elastic_hash, output_values_for_cloud,
            return_import_type_name(self.import_type), self.email, self.token,
            cloud_success_csv_file_path, self.language
        )
        self.process_logger.update_process_flow(
            working_validation_filename,
            error=EnumErrorType.IN_PROGRESS.name,
            language=self.language,
            key_enum=enum_msg.FILE_SEND_TO_rabbitMQ.value,
        )
        return {'file': working_validation_filename, "success": True}

    def get_validation_fields(self):
        """

        :return: main part of file validation, if file was successfully pass validation send json
        data to the rabbitMQ, else insert into fail database history.
        """
        working_validation_filename= os.path.basename(self.filename_path)
        call_class_on_execute = ParseFileBasedOnExtension(
            os.path.join(WORKING_DIR, working_validation_filename), self.delimiter,
            self.process_logger
        )
        real_path_for_processing = call_class_on_execute.start_checking_file()
        error_header_message = enum_msg.FILE_WITH_WRONG_HEADER_NAME.value

        if not real_path_for_processing['success']:
            self.process_logger.create_process_and_cloud_flow_and_main(
                os.path.basename(self.filename_path),
                error=EnumErrorType.FAIL,
                file_path=self.filename_path,
                email=self.email,
                language=self.language,
                key_enum=enum_msg.FILE_WRONG_FORMAT.value
            )
            return {'file': working_validation_filename, "success": False}
        if real_path_for_processing['success']:
            response_of_file = real_path_for_processing['file_path']

            logger_api.info(self.process_logger.update_system_log_flow(
                working_validation_filename,
                key_enum=enum_msg.FILE_WITH_OK_FORMAT.value)
            )

            working_file = os.path.join(WORKING_DIR, response_of_file)
            import_type_name = return_import_type_name(self.import_type)

            csv_import_data = import_file_content_to_json(working_file=working_file, delimiter=self.delimiter)
            if import_type_name == ImportType.PLANOGRAMS.name:
                for import_header in csv_import_data:
                    if not import_header.get('multiple_pricelists'):
                        self.process_logger.create_process_and_cloud_flow_and_main(
                            error=EnumErrorType.FAIL,
                            file_path=self.filename_path,
                            email=self.email,
                            language=self.language,
                            key_enum=enum_msg.PLANOGRAM_IMPORT_HEADER_GENERATOR_ERROR.value,
                        )
                        return
            if import_type_name == ImportType.LOCATIONS.name:
                for x in csv_import_data:
                    location_address_value = x.get('location_address')
                    longitude_value = x.get('longitude')
                    latitude_value = x.get('latitude')
                    location_name = x.get('location_name')

                    if location_address_value:
                        if longitude_value and not latitude_value:
                            self.process_logger.update_process_and_cloud_flow(
                                location_address_value, longitude_value,
                                error=EnumErrorType.WARNING.name,
                                language=self.language,
                                key_enum=enum_msg.GEO_LOCATION_LONGITUDE.value,
                            )

                        elif latitude_value and not longitude_value:
                            self.process_logger.update_process_and_cloud_flow(
                                location_address_value, latitude_value,
                                error=EnumErrorType.WARNING.name,
                                language=self.language,
                                key_enum=enum_msg.GEO_LOCATION_LATITUDE.value,
                            )

                    geo_location_mandatory = mandatory_geo_location(
                        address=location_address_value,
                        latitude=latitude_value,
                        longitude=longitude_value
                    )
                    geo_location_validation_status = geo_location_mandatory['valid']
                    mandatory_fields_info = geo_location_mandatory['mandatory_fields']

                    if not geo_location_validation_status:
                        self.process_logger.update_process_and_cloud_flow(
                            working_validation_filename, self.import_type,
                            error=EnumErrorType.FAIL.name,
                            language=self.language,
                            key_enum=enum_msg.FILE_WITHOUT_MANDATORY_FIELDS.value,
                        )

                        self.process_logger.update_process_and_cloud_flow(
                            working_validation_filename, self.import_type,
                            error=EnumErrorType.FAIL.name,
                            language=self.language,
                            message=Enum_validation_error.LONGITUDE_LATITUDE_LOCATION_ADDRESS_ERROR.value[self.language],
                        )
                        if location_name:
                            self.process_logger.update_process_and_cloud_flow(
                                location_name,
                                error=EnumErrorType.FAIL.name,
                                language=self.language,
                                key_enum=enum_msg.GEO_LOCATION.value,
                            )

                        output_string = ' '.join((str(x) for x in mandatory_fields_info))
                        wrong_headers_args = '<br>' + re.sub('[^a-zA-Z0-9 _]', '<br>', output_string)
                        self.process_logger.create_process_and_cloud_flow_and_main(
                            wrong_headers_args,
                            error=EnumErrorType.FAIL,
                            file_path=self.filename_path,
                            email=self.email,
                            language=self.language,
                            key_enum=error_header_message,
                        )
                        return

                    geo_location_validation_message = geo_location_mandatory['message']
                    logger_api.info(geo_location_validation_message)

            if import_type_name == ImportType.PLANOGRAMS.name:
                get_default_parser = return_import_type_based_on_parser(import_type_name, data=csv_import_data)
                if not get_default_parser:
                    self.process_logger.create_process_and_cloud_flow_and_main(
                        error=EnumErrorType.FAIL,
                        file_path='',
                        email='',
                        language='en',
                        key_enum=enum_msg.PLANOGRAM_IMPORT_HEADER_GENERATOR_ERROR.value
                    )
                    return

                # Business logic only for planogram, if sent column that is not defined in multi price column
                try:
                    for import_item_data in csv_import_data:
                        import_price = dict(
                            [(x, y) for x, y in import_item_data.items() if x.startswith('price_') or x.startswith('Price_')]
                        )
                        allowed_price = dict(
                            [(x, y) for x, y in get_default_parser['custom_valid_fields'].items() if x.startswith('price_')]
                        )
                        import_price_list = []
                        allowed_price_list = []
                        for k, v in import_price.items():
                            if k not in import_price_list:
                                import_price_list.append(k)

                        for k, v in allowed_price.items():
                            if k not in allowed_price_list:
                                allowed_price_list.append(k)
                        wrong_column_message = []
                        for import_price_item in import_price_list:
                            if import_price_item not in allowed_price_list:
                                if import_price_item not in wrong_column_message:
                                    wrong_column_message.append(import_price_item)
                        if len(wrong_column_message):
                            self.process_logger.create_process_and_cloud_flow_and_main(
                                ', '.join(allowed_price_list), ', '.join(wrong_column_message),
                                error=EnumErrorType.FAIL,
                                file_path=self.filename_path,
                                email=self.email,
                                language=self.language,
                                key_enum=enum_msg.PLANOGRAM_IMPORT_PRICE_WRONG_COLUMN.value,
                            )
                            return None
                except Exception as e:
                    logger_api.error(self.process_logger.update_system_log_flow(
                        e, key_enum=enum_msg.PLANOGRAM_IMPORT_PRICE_WRONG_COLUMN_EXCEPTION.value))
            else:
                get_default_parser = return_import_type_based_on_parser(self.import_type, api_request=self.api_request)

            field_names, mandatory_fields = return_all_fields_and_mandatory(get_default_parser)

            missing_mandatory_fields = False
            missing_header = False

            list_missing_header = []
            with codecs.open(working_file, 'r', encoding='utf-8', errors='ignore') as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=self.delimiter)
                header_row = next(csv_reader)
                self.fields_in_document = [h.lower() if h is not None else h for h in header_row]
                for field in field_names:
                    if field not in self.fields_in_document:
                        missing_header = True
                file_content = []
                for content_rows in csv_reader:
                    for row in content_rows:
                        file_content.append(row)
                if not file_content:
                    self.process_logger.create_process_and_cloud_flow_and_main(
                        self.import_type,
                        error=EnumErrorType.FAIL,
                        file_path= self.filename_path,
                        email=self.email,
                        language=self.language,
                        key_enum=enum_msg.FILE_EMPTY.value
                    )

                    return {'file': working_validation_filename, "success": False}
                real_header = [x for x in header_row if x]
                check_header=list((filter((lambda x: re.search('\w+', x)), real_header)))
                if not len(check_header):
                    real_header= check_header
                if not real_header:
                    self.process_logger.create_process_and_cloud_flow_and_main(
                        os.path.basename(working_file), self.import_type,
                        error=EnumErrorType.FAIL,
                        file_path=self.filename_path,
                        email=self.email,
                        language=self.language,
                        key_enum=enum_msg.FILE_WITHOUT_HEADER.value
                    )
                    return

                # check mandatory fields!!
                if self.fields_in_document != field_names:
                    # Check mandatory fields in header!
                    for mandatory_field in mandatory_fields:
                        if mandatory_field not in self.fields_in_document:
                            missing_mandatory_fields = True
                            break
                if missing_header:
                    for item in self.fields_in_document:
                        real_item = ''.join(item.split())
                        if real_item not in field_names:
                            list_missing_header.append(real_item)

            # Check if json hash exists in database.
            output_dict_arr = self.get_output_response(response_of_file, get_default_parser)

            # Call function and convert dict for cloud
            output_values_for_cloud = self.parse_dictionary_for_cloud(response_of_file,
                                                                      get_default_parser)
            # if some not mandatory fields are missing, exclude them from validation
            excluded_fields = [n for n in field_names if n not in self.fields_in_document]
            missing_header = [x for x in list_missing_header if x]

            # add missing rows
            if excluded_fields:
                for row in output_values_for_cloud:
                    for field in excluded_fields:
                        row[field] = '<null>'

            if missing_header:
                if missing_mandatory_fields and missing_header:
                    self.process_logger.update_process_and_cloud_flow(
                        working_validation_filename, self.import_type,
                        error=EnumErrorType.FAIL.name,
                        language=self.language,
                        key_enum=enum_msg.FILE_WITHOUT_MANDATORY_FIELDS.value,
                    )
                    output_string = ' '.join((str(x) for x in missing_header))
                    wrong_headers_args='<br>' + re.sub('[^a-zA-Z0-9 _]', '<br>', output_string)
                    self.process_logger.create_process_and_cloud_flow_and_main(
                        wrong_headers_args,
                        error=EnumErrorType.FAIL,
                        file_path=self.filename_path,
                        email=self.email,
                        language=self.language,
                        key_enum=error_header_message,
                    )
                    return None
                elif missing_header and not missing_mandatory_fields:
                    output_string = ' '.join((str(x) for x in missing_header))
                    missing_header_args='<br>' + re.sub('[^a-zA-Z0-9 _]', '<br>', output_string)
                    self.process_logger.create_process_and_cloud_flow_and_main(
                        missing_header_args, self.import_type,
                        error=EnumErrorType.FAIL,
                        file_path=self.filename_path,
                        email=self.email,
                        language=self.language,
                        key_enum=enum_msg.FAIL_MISSING_HEADER.value,
                    )

                    return None

            json_hash = generate_hash_for_json(output_values_for_cloud)
            self.process_logger.set_data_hash(json_hash)
            success_history, history = CompanyHistory.exists_in_history(self.company_id, json_hash)
            fail_history = CompanyHistory.return_local_fail_history(self.company_id, json_hash)
            if success_history:
                self.process_logger.create_process_and_cloud_flow_and_main(
                    history.updated_at.strftime('%d-%m-%Y'),
                    error=EnumErrorType.FAIL,
                    file_path=self.filename_path,
                    email=self.email,
                    language=self.language,
                    key_enum=enum_msg.ALREADY_SUCCESS_PROCESSED_FILE.value
                )

                return {'file': working_validation_filename, "success": False}

            if fail_history['success']:
                self.process_logger.create_process_and_cloud_flow_and_main(
                    fail_history['res'].strftime('%d-%m-%Y %H:%M:%S'),
                    error=EnumErrorType.FAIL,
                    file_path=self.filename_path,
                    email=self.email,
                    language=self.language,
                    key_enum=enum_msg.ALREADY_FAIL_PROCESSED_FILE.value
                )

                return {'file': working_validation_filename, "success": False}

            output_response = []
            count_line = 1
            for row in output_dict_arr:
                count_line += 1
                try:
                    validate(row, get_default_parser, format_checker=FormatChecker())
                except jsonschema.exceptions.ValidationError as e:
                    output_error = return_errors_from_json_schema(
                        row, e, get_default_parser, self.language, count_line)
                    for list_item in output_error:
                        output_response.append(list_item)

            logger_api.info(self.process_logger.update_system_log_flow(
                output_response,
                key_enum=enum_msg.VALIDATION_SYSTEM_LOG_INFO.value)
            )

            if len(output_response) > 0:
                unique_len = sorted(output_response, key=itemgetter('record'))
                new_d = pd.DataFrame(unique_len).drop_duplicates().to_dict('records')
                total_error_message = len(new_d)
                if total_error_message:
                    self.process_logger.set_data_hash(json_hash)
                    messages = []

                    # translate json schema error message
                    for item in new_d:
                        messages.append({
                            'record': item['record'],
                            'message': enum_message_on_specific_language(
                                enum_msg.VALUE_INCORRECT_FORMAT.value, self.language, item['message'])
                        })

                    logger_api.info(self.process_logger.update_system_log_flow(
                        'csv_validator', total_error_message,
                        key_enum=enum_msg.VALIDATION_ELASTIC_INSERT_START_TIME.value)
                    )
                    self.process_logger.update_process_and_cloud_flow(
                        self.import_type,
                        error=EnumErrorType.ERROR.name,
                        language=self.language,
                        key_enum=enum_msg.CSV_VALIDATOR_ERROR.value,
                    )

                    self.process_logger.update_process_and_cloud_flow(
                        message=json.dumps(messages),
                        error=EnumErrorType.ERROR.name,
                        language=self.language,
                        key_enum=enum_msg.VALUE_INCORRECT_FORMAT.value,
                    )

                    logger_api.info(self.process_logger.update_system_log_flow(
                        'csv_validator', total_error_message,
                        key_enum=enum_msg.VALIDATION_ELASTIC_INSERT_END_TIME.value)
                    )
                    self.process_logger.create_process_and_cloud_flow_and_main(
                        self.import_type,
                        error=EnumErrorType.ERROR,
                        file_path=self.filename_path,
                        email=self.email,
                        language=self.language,
                        key_enum=enum_msg.FAIL_FILE_VALIDATION.value
                    )
                    return {'file': working_validation_filename, "success": False}

            input_file_no_ext = os.path.splitext(working_validation_filename)[0]
            hash_success_file = '{}_{}${}.{}'.format(self.company_id, json_hash,
                                                     input_file_no_ext, 'json')
            success_history_file = os.path.join(HISTORY_SUCCESS_DIR, hash_success_file)
            with open(success_history_file, 'w') as success_file:
                json.dump(output_dict_arr, success_file)

            cloud_success_csv_file_path = os.path.join(
                HISTORY_SUCCESS_DIR, '{}_{}${}'.format(
                    self.company_id, json_hash, working_validation_filename))
            try:
                my_extension = os.path.splitext(self.filename_path)[1]
                shutil.copy2(self.filename_path, HISTORY_SUCCESS_DIR)
                os.rename(os.path.join(HISTORY_SUCCESS_DIR, working_validation_filename),
                          cloud_success_csv_file_path)
                if os.path.isfile(os.path.join(STORE_DIR, working_validation_filename)):
                    os.remove(os.path.join(STORE_DIR, working_validation_filename))
                if os.path.isfile(self.filename_path):
                    os.remove(self.filename_path)
                if my_extension != '.csv':
                    file_path_remove = str(os.path.splitext(self.filename_path)[0]) + '.csv'
                    full_path_to_file = os.path.join(WORKING_DIR, file_path_remove)
                    if os.path.isfile(full_path_to_file):
                        os.remove(os.path.join(WORKING_DIR, file_path_remove))
            except Exception:
                logger_api.exception(self.process_logger.update_system_log_flow(
                    traceback.print_exc(),
                    key_enum=enum_msg.RENAME_LOCAL_FILE_ERROR.value)
                )
            self.process_logger.update_process_flow(
                cloud_success_csv_file_path,
                error=EnumErrorType.IN_PROGRESS.name,
                language=self.language,
                key_enum=enum_msg.SUCCESS_FILE_VALIDATION.value)

            self.process_logger.update_process_flow(
                os.path.basename(self.filename_path),
                error=EnumErrorType.IN_PROGRESS.name,
                language=self.language,
                key_enum=enum_msg.FILE_IN_QUEUE_FOR_DATABASE_VALIDATION.value,
            )

            CompanyHistory.insert_history(
                self.company_id, 'csv_validator', return_import_type_id_custom_validation(self.import_type),
                self.elastic_hash, json_hash, self.filename_path, self.token
            )

            # Initialize import process handler (allowed only one import process per company, in same time)!
            import_process_handler = ImportProcessHandler(
                company_id=self.company_id, elastic_hash=self.elastic_hash,
                file_path=self.filename_path, import_type=self.import_type)

            # Check if import process can be started (if not check every n sec, until previous process is finished)
            run_next_import_process, elastic_hash = import_process_handler.check_run_next_import_process()

            if not run_next_import_process:
                while not run_next_import_process:
                    time.sleep(60)
                    run_next_import_process, elastic_hash = import_process_handler.check_run_next_import_process()

                    logger_api.info(self.process_logger.update_system_log_flow(
                        self.elastic_hash,
                        elastic_hash,
                        self.company_id,
                        key_enum=enum_msg.IMPORT_PROCEDURE_PER_COMPANY.value))

                logger_api.info(self.process_logger.update_system_log_flow(
                    elastic_hash,
                    self.elastic_hash,
                    self.company_id,
                    key_enum=enum_msg.IMPORT_PROCEDURE_RUN_NEXT_PROCESS.value))

            else:
                # Import process can be published into cloud_validator queue, because there is no running import
                # process for current import company!
                logger_api.info(self.process_logger.update_system_log_flow(
                    self.company_id,
                    self.elastic_hash,
                    key_enum=enum_msg.IMPORT_PROCEDURE_NO_ACTIVE_PROCESS.value))

            if import_type_name not in [ImportType.PLANOGRAMS.name, ImportType.MACHINE_TYPES.name]:
                import_process_handler.redis_set_running_import_process()

            publish_status = self.publish_to_cloud_validator(
                output_values_for_cloud,
                cloud_success_csv_file_path,
                working_validation_filename)

            return publish_status
