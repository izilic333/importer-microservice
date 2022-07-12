import os
import re
import shutil
import zipfile
import codecs
import traceback
import csv
from datetime import datetime
from distutils.util import strtobool
from common.logging.setup import logger
from common.mixin.validation_const import ImportType
from common.mixin.validator_import import (WORKING_DIR, ZIP_WORKING_DIR, VENDS_ZIP_WORKING_DIR, VENDS_WORKING_DIR,
                                           create_if_doesnt_exist, VENDS_HISTORY_SUCCESS_DIR, VENDS_FAIL_DIR)
from common.mixin.enum_errors import EnumValidationMessage as enum_msg, EnumErrorType
from common.mixin.vends_mixin import VENDS_INITIAL_INFO, MainVendProcessLogger, guid1, create_elastic_hash, \
    CleanLocalHistory
from common.logging.setup import vend_logger
from common.urls.urls import import_type_redis_key_duration
from core.flask.redis_store.redis_managment import RedisManagement
from database.company_database.core.query_history import CompanyFailHistory

logger_api = logger


class CsvValidatorHandleFIle(object):
    """
    This is helper class for csv validator, basically handle file format, content, extension, data
    type, etc...
    """
    def __init__(self, filename, import_type, company_id, process_logger, delimiter, token):
        self.filename = filename
        self.import_type = import_type
        self.company_id = company_id
        self.process_logger = process_logger
        self.delimiter = delimiter
        self.token = token

    def get_import_type(self):
        """

        :return: import type name
        """

        for imports in ImportType:
            if type(self.import_type) is int and self.import_type == imports.value.get('id'):
                return imports.name
            elif imports.name == self.import_type:
                return imports.name
        return None

    def check_file_extension(self):
        """

        :return: extension of the file
        """

        try:
            extension = os.path.splitext(self.filename)[1]
            # Check if file sent without extension.
            if len(extension) == 0:
                logger_api.error(self.process_logger.update_system_log_flow(
                    self.filename,
                    key_enum=enum_msg.FILE_EXTENSION_ERROR.value)
                )
            return extension
        except Exception:
            logger_api.error(self.process_logger.update_system_log_flow(
                traceback.print_exc(),
                key_enum=enum_msg.FILE_WRONG_FORMAT.value)
            )

    def return_variable_type(self, input_data):
        """

        :return: determine/check file content type
        """

        if input_data is not None:
            value_type= input_data
            var_type = type(value_type)
            variable_changed = value_type
            if var_type.__name__ == 'int':
                return '{}'.format(int(variable_changed))
            elif var_type.__name__ == 'float':
                spl = str(variable_changed).split('.')
                if int(spl[1]) == 0:
                    return '{}'.format(round(float(variable_changed)))
                else:
                    return '{}'.format(float(variable_changed))
            elif var_type.__name__ == 'str':
                if variable_changed.lstrip('-').replace('.', '', 1).isdigit():
                    z = float(variable_changed)
                    c = int(variable_changed.replace('.', '', 1))
                    if z != c:
                        spl = str(variable_changed).split('.')
                        if int(spl[1]) == 0:
                            return '{}'.format(round(float(variable_changed)))
                        else:
                            return '{}'.format(float(variable_changed))
                    else:
                        return '{}'.format(str(variable_changed))
                else:
                    if len(variable_changed) > 0:
                        return '%s' % variable_changed.rstrip()
                    else:
                        return None
            elif variable_changed.lower() == 'true':
                return True
            elif variable_changed.lower() == 'false':
                return False
            else:
                return None
        else:
            return None

    def serializer_data(self, file_path, parser, validator_type):
        """

        :param parser: parser type (location, machine, machine_type, regions)
        :param validator_type: data type for cloud or importer (json schema validation)
        :return: main list of file content type
        """

        parser_json = parser['custom_valid_fields']
        values_data = []
        with codecs.open(file_path, "r", encoding='utf-8', errors='ignore') as in_file:
            csv_reader = csv.reader(in_file, delimiter=self.delimiter)
            headers = next(csv_reader)
            logger_api.info(self.process_logger.update_system_log_flow(
                headers, key_enum=enum_msg.VALIDATION_SYSTEM_LOG_INFO_HEADER_FILE.value)
            )
            for line in csv_reader:
                if not line:
                    continue
                values_entry = dict()
                for i in range(len(headers)):
                    try:
                        values_entry[headers[i]] = line[i]
                    except IndexError:
                        logger_api.info(self.process_logger.update_system_log_flow(
                            i, traceback.print_exc(),
                            key_enum=enum_msg.VALIDATION_SYSTEM_LOG_INFO_INDEX_MESSAGE.value)
                        )
                        values_entry[headers[i]] = ''
                values_data.append(values_entry)

        serializer_out = []
        for row in values_data:
            out_row = {}
            for key, val in row.items():
                # Find key
                if len(key):
                    try:
                        current_value = self.return_variable_type(val.rstrip().lstrip())
                        check_key = parser_json.get(key, None)
                    except Exception as e:
                        logger_api.error(self.process_logger.update_system_log_flow(
                            key, val, e, key_enum=enum_msg.VALIDATION_SYSTEM_LOG_FIELD_ERROR.value)
                        )
                    if check_key:
                        if current_value and check_key:
                            if check_key.__name__ == 'str':
                                out_row[key] = current_value
                            elif check_key.__name__ == 'int':
                                out_row[key] = current_value
                            elif check_key.__name__ == 'float':
                                out_row[key] = current_value
                        elif not current_value and check_key:
                            if check_key.__name__ == 'str':
                                if validator_type == 'cloud_validator':
                                    out_row[key] = ''
                                elif validator_type == 'csv_validator':
                                    out_row[key] = None
                            elif check_key.__name__ == 'int':
                                out_row[key] = None
                            elif check_key.__name__ == 'float':
                                out_row[key] = None
            if out_row:
                serializer_out.append(out_row)
            else:
                continue
        return serializer_out

    def handle_zip_file(self, zip_path):
        """

        :param zip_path:
        :return: list extracted zip path
        """
        extracted_uuid_dir = os.path.join(ZIP_WORKING_DIR, guid1())
        all_zip_files_path = []
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extracted_uuid_dir)

        working_zip_item = os.listdir(extracted_uuid_dir)

        for zip_item in working_zip_item:
            path_zip_working_dir = os.path.join(extracted_uuid_dir, zip_item)

            if os.path.isdir(path_zip_working_dir):
                zip_dir_content = os.listdir(path_zip_working_dir)
                for dir_content in zip_dir_content:
                    zip_dir_content_path = os.path.join(path_zip_working_dir, dir_content)
                    try:
                        path_working_dir2 = os.path.join(WORKING_DIR, dir_content)
                        shutil.copy2(zip_dir_content_path, WORKING_DIR)
                        all_zip_files_path.append(path_working_dir2)

                    except Exception:
                        logger_api.info(self.process_logger.update_system_log_flow(
                            traceback.print_exc(),
                            key_enum=enum_msg.RENAME_LOCAL_FILE_ERROR.value)
                        )
                try:
                    shutil.rmtree(path_zip_working_dir)
                except Exception:
                    logger_api.error(self.process_logger.update_system_log_flow(
                        traceback.print_exc(),
                        key_enum=enum_msg.DELETE_LOCAL_FILE_ERROR.value)
                    )

            else:
                try:
                    path_working_dir1 = os.path.join(WORKING_DIR, zip_item)
                    shutil.copy2(path_zip_working_dir, WORKING_DIR)
                except Exception:
                    logger_api.info(self.process_logger.update_system_log_flow(
                        traceback.print_exc(),
                        key_enum=enum_msg.RENAME_LOCAL_FILE_ERROR.value)
                    )
                # remove file, or remove dir, if it's a dir
                try:
                    if os.path.isfile(path_zip_working_dir):
                        os.remove(path_zip_working_dir)
                    else:
                        shutil.rmtree(path_zip_working_dir)
                except Exception:
                    logger_api.error(self.process_logger.update_system_log_flow(
                        traceback.print_exc(),
                        key_enum=enum_msg.DELETE_LOCAL_FILE_ERROR.value)
                    )

                if os.path.isfile(path_working_dir1):
                    all_zip_files_path.append(path_working_dir1)

        # remove the uuid folder and the .zip file
        shutil.rmtree(extracted_uuid_dir)
        if os.path.isfile(zip_path):
            os.remove(zip_path)
        return all_zip_files_path


def get_import_type(import_type):

    for imports in ImportType:
        if type(import_type) is int and import_type == imports.value.get('id'):
            return imports.name
        elif imports.name == import_type:
            return imports.name
    return None


class MainVendsHelpers(object):
    def __init__(self, import_type, company_id, token,  regex, datetime_format, process_logger, elastic_hash):
        self.import_type = import_type
        self.company_id = company_id
        self.token = token
        self.datetime_format = datetime_format
        self.filename_regex = regex
        self.process_logger = process_logger
        self.elastic_hash = elastic_hash
        self.working_dir = os.path.join(VENDS_WORKING_DIR, self.import_type, str(self.company_id))

    def zip_file_handler(self, zip_file_path):

        file_info = self.get_file_info(zip_file_path, self.import_type)
        device_pid = file_info['device_pid']
        file_year = file_info['file_year']
        file_month = file_info['file_month']
        file_day = file_info['file_day']
        file_hour = file_info['file_hour']
        file_minutes = file_info['file_minutes']
        file_seconds = file_info['file_seconds']

        # Make vends working directory with hash, this is security for files with same filename!
        extracted_uuid_dir = os.path.join(VENDS_ZIP_WORKING_DIR, guid1())
        all_zip_files_path = []

        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extracted_uuid_dir)

        working_zip_item = os.listdir(extracted_uuid_dir)

        for zip_item in working_zip_item:
            path_zip_working_dir = os.path.join(extracted_uuid_dir, zip_item)
            # Check if we have dir instead file
            if os.path.isdir(path_zip_working_dir):
                zip_dir_content = os.listdir(path_zip_working_dir)
                for dir_content in zip_dir_content:
                    try:
                        # Define new filename!
                        new_filename = '{}_{}.{}'.format(
                            device_pid, str(file_year) + str(file_month) + str(
                                file_day) + str('_') + str(file_hour) + str(file_minutes) + str(
                                file_seconds), 'eva'
                        )
                        new_filename_path = os.path.join(self.working_dir, new_filename)
                        old_filename_path = os.path.join(path_zip_working_dir, dir_content)
                        os.rename(old_filename_path, new_filename_path)
                        shutil.copy2(new_filename_path, self.working_dir)
                        all_zip_files_path.append(new_filename_path)

                    except Exception as e:
                        self.process_logger.update_system_log_flow(e, key_enum=enum_msg.RENAME_LOCAL_FILE_ERROR.value,
                                                                   logs_level=EnumErrorType.ERROR.name)
                try:
                    shutil.rmtree(path_zip_working_dir)
                except Exception as e:
                    self.process_logger.update_system_log_flow(e, key_enum=enum_msg.DELETE_LOCAL_FILE_ERROR.value,
                                                               logs_level=EnumErrorType.ERROR.name)
            else:
                new_filename = '{}_{}.{}'.format(
                    device_pid, str(file_year) + str(file_month) + str(
                        file_day) + str('_') + str(file_hour) + str(file_minutes) + str(
                        file_seconds), 'eva'
                )
                try:
                    ext = self.get_file_extension(path_zip_working_dir)
                    if ext == '.dex':

                        new_filename_path = os.path.join(self.working_dir, new_filename)
                        os.rename(path_zip_working_dir, new_filename_path)

                        if os.path.isfile(new_filename_path):
                            all_zip_files_path.append(new_filename_path)

                    if os.path.isdir(path_zip_working_dir):
                        shutil.rmtree(path_zip_working_dir)
                except Exception as e:
                    self.process_logger.update_system_log_flow(e, key_enum=enum_msg.RENAME_LOCAL_FILE_ERROR.value,
                                                               logs_level=EnumErrorType.ERROR.name)

        # remove the uuid folder and the .zip file
        shutil.rmtree(extracted_uuid_dir)
        if os.path.isfile(zip_file_path):
            os.remove(zip_file_path)
        return all_zip_files_path

    def get_file_info(self, zip_file_path, processor_type):

        try:
            zip_filename = (os.path.basename(zip_file_path))
            device_pid, file_time = re.match(self.filename_regex, zip_filename).groups()
            import_datetime = datetime.strptime(file_time, self.datetime_format)
            split_string = lambda x, n: [x[i:i + n] for i in range(0, len(x), n)]
            file_time_info = split_string(file_time, 2)
            file_month = file_time_info[0]
            file_day = file_time_info[1]
            file_year = file_time_info[2]+file_time_info[3]
            hours = file_time.split('_')[1]
            hours_info = split_string(hours, 2)

            # In this part create vends success/fail archive directory based on filename timestamp!
            cpi_vends = VENDS_INITIAL_INFO["vend_import_type"]
            if processor_type == 'CPI_VENDS':
                local_directory = VENDS_INITIAL_INFO["import_type_dir"]["cpi"]
            if processor_type == 'DEX_VENDS':
                local_directory = VENDS_INITIAL_INFO["import_type_dir"]["dex"]
            vends_dir = os.path.join(local_directory, cpi_vends)
            vends_success_dir = os.path.join(VENDS_HISTORY_SUCCESS_DIR, vends_dir)
            vends_fail_dir = os.path.join(VENDS_FAIL_DIR, vends_dir)
            create_if_doesnt_exist(vends_success_dir)
            create_if_doesnt_exist(vends_fail_dir)
            fail_dirs_based_on_datetime = os.path.join(vends_fail_dir, file_year, file_month, file_day)
            success_dirs_based_on_datetime = os.path.join(
                vends_success_dir, file_year, file_month, file_day
            )
            create_if_doesnt_exist(success_dirs_based_on_datetime)
            create_if_doesnt_exist(fail_dirs_based_on_datetime)
            file_response = {
                'device_pid': device_pid,
                'import_datetime_timestamp': import_datetime,
                'file_year': file_year,
                'file_month': file_month,
                'file_day': file_day,
                'file_hour': hours_info[0],
                'file_minutes': hours_info[1],
                'file_seconds': hours_info[2],
                'filename': zip_filename,
                'fail_dir': fail_dirs_based_on_datetime,
                'success_dir': success_dirs_based_on_datetime
            }
            return file_response

        except Exception as e:
            clean_local_archive = CleanLocalHistory(process_logger=self.process_logger)
            zip_filename = (os.path.basename(zip_file_path))

            sub_elastic_hash = create_elastic_hash(self.company_id, self.import_type, 'FILE')

            self.process_logger.update_system_log_flow(e, key_enum=enum_msg.VEND_INITIAL_FILE_ERROR.value,
                                                       logs_level=EnumErrorType.ERROR.name)
            self.process_logger.update_general_process_flow(
                zip_filename, self.elastic_hash,
                key_enum=enum_msg.VEND_INITIAL_FILE_ERROR_SUB_MESSAGE.value, elastic_hash=self.elastic_hash,
                status=EnumErrorType.WARNING.name)

            self.process_logger.update_importer_process_flow(
                zip_filename, status=EnumErrorType.ERROR.name,
                key_enum=enum_msg.VEND_INITIAL_FILE_ERROR.value,
                elastic_hash=sub_elastic_hash,
            )

            self.process_logger.update_main_elastic_process(
                file_path='', elastic_hash=sub_elastic_hash,
                data_hash='',
                main_elastic_hash=self.elastic_hash
            )
            clean_local_archive.delete_local_working_file(zip_file_path)
            return None

    @classmethod
    def get_file_extension(cls, file_path):
        try:
            extension = os.path.splitext(file_path)[1]
            # Check if file sent without extension.
            if len(extension) == 0:
                return None
            return extension
        except Exception as e:
            vend_logger.error(e)


class ImportProcessHandler(object):
    """
    Allowed only one import process per company, in same time!
    Explanation:
        csv validation passed successfully -> generate redis key for this company.
        cloud validation passed unsuccessfully -> update redis key/value with finished_process=True (next import for
        this company can start)
        cloud finish processing(cloud make final statistics response) -> update redis key/value with
        finished_process=True (next import for this company can start)
    """
    def __init__(self, company_id, elastic_hash, file_path, import_type):
        self.company_id = str(company_id)
        self.elastic_hash = elastic_hash
        self.file_path = file_path
        self.import_type = import_type
        self.redis_key = "{}_{}".format(self.company_id, 'import_fifo_procedure')
        self.redis_value = {
            'import_type': self.import_type,
            'file_path': self.file_path,
            'elastic_hash': self.elastic_hash,
            'company_id': self.company_id,
            'finished_process': False
        }

    @staticmethod
    def get_import_type_redis_key_duration(import_type_config_definition):
        """
        This method load redis key duration from envdir configuration (for specific import type) ...
        Default redis key duration is 5 hours ...
        :param import_type_config_definition: defined import_type in envdir config (str)
        :return: redis key duration in sec
        """
        redis_key_duration = 18000
        try:
            redis_key_duration = int(import_type_redis_key_duration[import_type_config_definition]) * 3600
        except Exception as e:
            logger_api.error(enum_msg.IMPORT_TYPE_REDIS_KEY_DURATION_ERROR.value['en'].format(e))
            logger_api.error(enum_msg.IMPORT_TYPE_REDIS_KEY_DURATION_DEFAULT.value['en'].format(redis_key_duration))

        return redis_key_duration

    def redis_set_running_import_process(self):

        master_data_import_types = [
            ImportType.PLANOGRAMS.name, ImportType.MACHINES.name, ImportType.LOCATIONS.name,
            ImportType.PRODUCTS.name, ImportType.USERS.name, ImportType.REGIONS.name, ImportType.CLIENTS.name,
            ImportType.MACHINE_TYPES.name,  ImportType.PACKINGS.name
        ]
        import_type_name = get_import_type(self.import_type)
        if import_type_name in master_data_import_types:
            redis_key_duration = self.get_import_type_redis_key_duration(self.import_type)
            RedisManagement.hmset_to_redis(key=self.redis_key, data=self.redis_value)
            RedisManagement.set_to_redis_expire(key=self.redis_key, time_in_seconds=redis_key_duration)

            logger_api.info(enum_msg.DEFINED_IMPORT_TYPE_REDIS_KEY_DURATION.value['en'].format(
                self.import_type, self.company_id, redis_key_duration,
            ))

    def check_run_next_import_process(self):
        """
        This method check if some import process is active and running for particular company.
        When importer start to import file and when csv validation passed successfully, that import
        process gets its redis key with specific values. So, every next import with the same company as the previously
        submitted file will not be allowed until the previous import process is finished.
        :return: run_next_import_process (boolean)
        """
        run_next_import_process = True
        elastic_hash = None
        redis_check_key_exists = RedisManagement.redis_key_exist_check(self.redis_key)

        if redis_check_key_exists:
            redis_data = RedisManagement.get_data_from_redis_hgetall(self.redis_key)
            finished_import_process = strtobool(redis_data['finished_process'])
            elastic_hash = redis_data['elastic_hash']
            if not finished_import_process:
                run_next_import_process = False

        return run_next_import_process, elastic_hash

    def finish_import_process_redis(self, finished_by, reason):
        exists_redis_key = RedisManagement.redis_key_exist_check(self.redis_key)
        if exists_redis_key:
            redis_value = {
                'import_type': self.import_type,
                'file_path': '',
                'elastic_hash': self.elastic_hash,
                'company_id': self.company_id,
                'finished_process': True
            }
            RedisManagement.hmset_to_redis(self.redis_key, redis_value)
            logger_api.info(
                "import type: {}, company_id: {}, elastic_hash: {}, finished_process: {}, filename: {},"
                " finished_by: {}, reason: {}".format(
                    self.import_type, self.company_id, self.elastic_hash, True,  self.file_path, finished_by, reason
                )
            )
