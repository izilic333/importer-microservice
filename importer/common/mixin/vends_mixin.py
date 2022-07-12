import codecs
import re
import os
import uuid
import shutil
import zipfile
import datetime
from common.logging.setup import vend_logger
from common.mixin.validator_import import VENDS_WORKING_DIR, create_if_doesnt_exist
from common.rabbit_mq.database_interaction_q.vend_db_publisher import vend_publish_to_database
from database.cloud_database.core.query import DeviceQueryOnCloud, CompanyQueryOnCloud
from common.mixin.enum_errors import enum_message_on_specific_language
from database.company_database.models.models import vend_device_history
from database.cloud_database.common.common import get_local_connection_safe
from database.company_database.core.query_history import CompanyFailHistory
from elasticsearch_component.core.query_vends import VendImportProcessLogger
from common.mixin.enum_errors import EnumValidationMessage as MainMessage, EnumErrorType

VENDS_INITIAL_INFO = {
    'vend_import_type': 'VENDS',
    'event_import_type': 'EVENTS',
    'cash_collection_import_type': 'CASH_COLLECTIONS',
    'import_type_dir': {'cpi': 'CPI', 'dex': 'DEX'},
    'cpi_import': {
        'file_regex': '(.*)_(\d{2}\d{2}\d{4}_\d{2}\d{2}\d{2}).zip$',
        'datetime_format_vend': '%m-%d-%Y %H:%M:%S.%f',
        'vends': '%m%d%Y_%H%M%S'
    },
    'dex_import': {
        'file_regex': '(.*)_(\d{2}\d{2}\d{4}_\d{2}\d{2}\d{2}).zip$',
        'datetime_format_vend': '%m-%d-%Y %H:%M:%S.%f',
        'vends': '%m%d%Y_%H%M%S'
    }


}


def new_device_pid_detect_operation(data, company_id, import_type):
    """
    This function basically parse make grouping eva fields, and prepare eva content for insert in local database
    history!

    :param data:
    :param company_id:
    :param import_type:
    :return: JSON with parsed eva content
    """
    working_directory = os.path.join(VENDS_WORKING_DIR, import_type, str(company_id))
    create_if_doesnt_exist(working_directory)
    eva = data['data']['import_filename']

    try:
        with codecs.open(eva, "r", encoding='utf-8', errors='ignore') as new_file:
            new_file_lines = new_file.readlines()
            la1_field = [field.rstrip("\n") for field in new_file_lines if field.startswith('LA1*')]
            pa_field = [field.rstrip("\n") for field in new_file_lines if field.startswith('PA')]
            ea_field = [field.rstrip("\n") for field in new_file_lines if field.startswith('EA')]
            ca_field = [field.rstrip("\n") for field in new_file_lines if field.startswith('CA')]
            da_field = [field.rstrip("\n") for field in new_file_lines if field.startswith('DA')]
            id_field = [field.rstrip("\n") for field in new_file_lines if field.startswith('ID')]
            va_field = [field.rstrip("\n") for field in new_file_lines if field.startswith('VA')]
            se_field = [field.rstrip("\n") for field in new_file_lines if field.startswith('SE')]

            eva_content = {
                "la_field": la1_field,
                "pa_field": pa_field,
                "ea_field": ea_field,
                "ca_field": ca_field,
                "da_field": da_field,
                "id_field": id_field,
                "va_field": va_field,
                "se_field": se_field
            }
            data["data"]["eva_content"] = eva_content
    except Exception as e:
        vend_logger.error(e)
        return {'status': False, 'data': eva}
    return {'status': True, 'data': data}


def handle_insert_device_pid_local_history(new_or_newest_pid_data):
    """
    This function insert data in local history
    :param new_or_newest_pid_data: data for insert
    :return: inserted data in local history
    """
    if new_or_newest_pid_data:
        try:
            # Make structure for insert
            with get_local_connection_safe() as conn_local:

                data = {
                    'company_id': new_or_newest_pid_data['company_id'],
                    'import_type':  new_or_newest_pid_data['import_type'],
                    'file_timestamp': new_or_newest_pid_data['data']['file_timestamp'],
                    'device_pid': new_or_newest_pid_data['data']['device_pid'],
                    'zip_filename': new_or_newest_pid_data['data']['zip_filename'],
                    'import_filename': new_or_newest_pid_data['data']['import_filename'],
                    'machine_id': new_or_newest_pid_data['data']['machine_id'],
                    'data':  new_or_newest_pid_data
                }
                conn_local.execute(vend_device_history.insert(), data)
        except Exception as e:
            vend_logger.error(e)


def get_vends_data_since_initialization(eva_string, eva_field_position):
    if eva_string:
        eva_fields = eva_string.split('*')
        if len(eva_fields) > eva_field_position:
            return eva_fields[eva_field_position]


def get_decimal_points(ird_fields):
    ird_id = 0
    for ird4 in ird_fields:
        match_ird, match_value = re.match('(^ID4.)([0-9])', ird4).groups()
        value_potency = 10 ** int(match_value)
        ird_id += value_potency

    return ird_id


def pairwise(item):
    item = iter(item)
    while True:
        yield next(item), next(item)


class MainVendProcessLogger(object):
    def __init__(self, company_id, import_type, process_request_type, token):
        self.company_id = company_id
        self.import_type = import_type
        self.process_request_type = process_request_type
        self.token = token

    @classmethod
    def update_importer_process_flow(cls, *args, status=None, language='en', message=None, key_enum=None,
                                     elastic_hash=None):
        if not message:
            message = enum_message_on_specific_language(key_enum, language, *args)

        VendImportProcessLogger.create_importer_validation_process_flow(
            status=status,
            message=message,
            process_hash=elastic_hash
        )

    def update_main_elastic_process(self, file_path, elastic_hash, data_hash, main_elastic_hash=None):
        VendImportProcessLogger.update_main_vend_process(
            status=EnumErrorType.ERROR.name,
            process_hash=elastic_hash
        )

        self.insert_fail_history(
            file_path=file_path,
            elastic_hash=elastic_hash,
            data_hash=data_hash,
            main_elastic_hash=main_elastic_hash
        )

    @classmethod
    def update_system_log_flow(cls, *args, key_enum=None, logs_level, language='en'):
        message = enum_message_on_specific_language(key_enum, language, *args)

        if logs_level.upper() == EnumErrorType.ERROR.name:
            vend_logger.error(message)

        if logs_level.upper() == EnumErrorType.FAIL.name:
            vend_logger.info(message)

        if logs_level.upper() == EnumErrorType.WARNING.name:
            vend_logger.debug(message)

        if logs_level.upper() == EnumErrorType.IN_PROGRESS.name:
            vend_logger.debug(message)

        return message

    def insert_fail_history(self, file_path, elastic_hash, data_hash, main_elastic_hash=None):
        CompanyFailHistory.insert_vend_fail_history(
            company_id=self.company_id,
            import_type=self.import_type,
            elastic_hash=elastic_hash,
            data_hash=data_hash,
            file_path=file_path,
            import_error_type=EnumErrorType.ERROR.value,
            token=self.token,
            main_elastic_hash=main_elastic_hash
        )

    @classmethod
    def update_general_process_flow(cls, *args, status=None, language=None, key_enum=None, elastic_hash):
        message = enum_message_on_specific_language(key_enum, language, *args)

        # Create elastic cloud process flow
        VendImportProcessLogger.create_cloud_validation_process_flow(
            status=status,
            message=message,
            process_hash=elastic_hash
        )

        # Create elastic importer process flow
        VendImportProcessLogger.create_importer_validation_process_flow(
            status=status,
            message=message,
            process_hash=elastic_hash
        )

        return message


def get_device_object(device_pid, company_id):

    try:
        device = DeviceQueryOnCloud.check_existing_device_on_cloud(
            device_pid=device_pid, company_id=company_id
        )

        if device:
            device_pid_check = [x['device_pid'] for x in device['device_result']]
            if device_pid_check:
                device_pid = ''.join(map(
                    str, list(map(lambda d: d['device_pid'], device['device_result'])))
                )
                device_alive = ''.join(map(
                    str, list(map(lambda d: d['device_alive'], device['device_result'])))
                )
                device_status = ''.join(map(
                    str, list(map(lambda d: d['device_status'], device['device_result'])))
                )
                device_type_id = ''.join(map(
                    str, list(map(lambda d: d['device_type_id'], device['device_result'])))
                )
                device_id = int(''.join(map(
                    str, list(map(lambda d: d['device_id'], device['device_result']))))
                )
                machine_external_id = device['machine_external_id']
                machine_id = device['machine_id']
                device_object = {
                    "device_pid": device_pid,
                    "device_alive": device_alive,
                    "device_status": device_status,
                    "device_type_id": device_type_id,
                    "device_id": device_id,
                    "machine_external_id": machine_external_id,
                    "machine_id": machine_id

                }
                if machine_id:
                    return device_object
                else:
                    return False
        else:
            return False

    except Exception as e:
        vend_logger.error(e)


def create_elastic_hash(company_id, import_type, import_request_type):
    """
    This function try to generate new elastic hash, and if can't it will update system log with message with error
    on creating elastic hash!
    :param company_id: company_id
    :param import_type:  import_type
    :param import_request_type: (CLOUD/FILE)
    :return: elastic_hash
    """
    generate_elastic_hash = VendImportProcessLogger.create_new_process(
        company_id=company_id,
        import_type=import_type,
        import_request_type=import_request_type
    )

    elastic_hash = generate_elastic_hash['id'] if generate_elastic_hash['process_created'] else None
    # If can't create elastic process hash, we must stop this import, because elastic hash must be included.

    if not elastic_hash:
        MainVendProcessLogger.update_system_log_flow(
            company_id, import_type,
            key_enum=MainMessage.VEND_ERROR_ON_CREATING_ELASTIC_HASH.value,
            logs_level=EnumErrorType.ERROR.name
        )

        return None
    return elastic_hash


class CleanLocalHistory(object):
    def __init__(self, process_logger):
        self.process_logger = process_logger

    def clean_directory(self, dir_path):
        try:
            for root, dirs, files in os.walk(dir_path):
                if files:
                    for file in files:
                        file_path = os.path.join(dir_path, file)
                        if os.path.isfile(file_path):
                            os.remove(file_path)
        except Exception as e:
            self.process_logger.update_system_log_flow(
                dir_path, e,
                key_enum=MainMessage.DELETE_LOCAL_DIR_ERROR_SYSTEM_LOG.value,
                logs_level=EnumErrorType.ERROR.name
            )

    def delete_local_working_file(self, file_path):
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
        except Exception as e:
            self.process_logger.update_system_log_flow(
                file_path, e,
                key_enum=MainMessage.DELETE_LOCAL_FILE_ERROR_SYSTEM_LOG.value,
                logs_level=EnumErrorType.ERROR.name
            )


def guid1():
    unique_id = uuid.uuid4()
    guid = str(unique_id)
    return guid


def generate_elastic_login_for_group_file(process_logger, file_paths,  company_id, import_type, fail_dir,
                                          elastic_hash, request_type):

    working_directory = os.path.join(VENDS_WORKING_DIR, import_type, str(company_id))
    create_if_doesnt_exist(working_directory)
    clean_local_archive = CleanLocalHistory(process_logger=process_logger)

    actual_date = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    actual_fail_filename = '{}${}.zip'.format(company_id, actual_date)
    cloud_fail_file_path = os.path.join(working_directory, actual_fail_filename)
    os.chdir(working_directory)
    eva_zip = zipfile.ZipFile(cloud_fail_file_path, 'w', zipfile.ZIP_DEFLATED)

    sub_elastic_hash = create_elastic_hash(company_id, import_type, request_type)
    for file in file_paths:
        process_logger.update_importer_process_flow(
            os.path.basename(file), sub_elastic_hash,
            status=EnumErrorType.WARNING.name,
            language='en',
            key_enum=MainMessage.ERROR_OCCUR_ON_OPEN_EVA_FILE_SUB_MESSAGE.value,
            elastic_hash=elastic_hash
        )
        process_logger.update_general_process_flow(
            os.path.basename(file), status=EnumErrorType.ERROR.name,
            language='en',
            key_enum=MainMessage.ERROR_OCCUR_ON_OPEN_EVA_FILE.value,
            elastic_hash=sub_elastic_hash
        )
        eva_zip.write(file)
        fail_pat_eva_file = os.path.join(working_directory, file)
        clean_local_archive.delete_local_working_file(fail_pat_eva_file)
    eva_zip.close()

    fail_file_path = os.path.join(fail_dir, actual_fail_filename)

    process_logger.update_main_elastic_process(
        file_path=fail_file_path,
        elastic_hash=sub_elastic_hash,
        data_hash='',
        main_elastic_hash=elastic_hash
    )
    shutil.copy2(cloud_fail_file_path, fail_dir)
    clean_local_archive.delete_local_working_file(cloud_fail_file_path)


class EvaHandler(object):
    """
    This class is created for EVA PA1 and EVA PA2 vend calculating.
    """
    def __init__(self, company_id, import_type, request_type, main_elastic_hash, general_process_logger, zip_filename,
                 cpi_payment_type):
        self.company_id = company_id
        self.import_type = import_type
        self.request_type = request_type
        self.main_elastic_hash = main_elastic_hash
        self.general_process_logger = general_process_logger
        self.zip_filename = zip_filename
        self.cpi_payment_type = cpi_payment_type

    @classmethod
    def eva_pa_fields_handler(cls, eva_type, pa_fields):
        """
        This method make basic sorting on PA1 and PA2 EVA fields and generate new initial working EVA line, because in
        original EVA file PA1 and PA2 fields is not in same rows, because of that this method sort and put this fields
        into one rows!
        :return: lists of initial sorted EVA PA fields
        """
        eva_position = [{eva_type: []}]
        for a, b in pairwise(pa_fields):
            if a.startswith('PA1'):
                pa101 = get_vends_data_since_initialization(a, 1)
            if b.startswith('PA1'):
                pa101 = get_vends_data_since_initialization(b, 1)
            if a.startswith('PA2'):
                pa201 = get_vends_data_since_initialization(a, 1)
                pa202 = get_vends_data_since_initialization(a, 2)
            if b.startswith('PA2'):
                pa201 = get_vends_data_since_initialization(b, 1)
                pa202 = get_vends_data_since_initialization(b, 2)

            for archive_dict in eva_position:
                archive_dict[eva_type].append({'pa101': pa101, 'pa201': pa201, 'pa202': pa202})
        return eva_position

    @classmethod
    def group_eva_pa_fields(cls, eva_position):
        """
        This method make basic grouping and generate new initial working EVA rows, this is useful when we calculate EVA
        vends, price and product column for this vends!
        :return: grouped lists with new and old eva with new generated eva string
        """
        final_new_pa_fields_list = []
        final_old_pa_fields_list = []

        for eva_list in eva_position:
            for key, value in eva_list.items():
                if key == 'old_eva_pa':
                    for old_eva_filed in value:
                        get_pa101_position_old = old_eva_filed['pa101']
                        get_pa201_position_old = old_eva_filed['pa201']
                        get_pa202_position_old = old_eva_filed['pa202']

                        final_old_pa_fields = 'PA' + str('*') + get_pa101_position_old + str('*') + str(
                            get_pa201_position_old) + str('*') + str(get_pa202_position_old)
                        final_old_pa_fields_list.append(final_old_pa_fields)
                if key == 'new_eva_pa':
                    for new_eva_field in value:
                        get_pa101_position_new = new_eva_field['pa101']
                        get_pa201_position_new = new_eva_field['pa201']
                        get_pa202_position_new = new_eva_field['pa202']
                        final_new_pa_fields = 'PA' + str('*') + get_pa101_position_new + str('*') + str(
                            get_pa201_position_new) + str('*') + str(get_pa202_position_new)
                        final_new_pa_fields_list.append(final_new_pa_fields)

        return final_new_pa_fields_list, final_old_pa_fields_list

    def vends_eva_exception_logger(self, device_pid_info, fail_path, json_hash, error):
        """
        This method make basic elastic and system logging.
        :param device_pid_info: device pid
        :param fail_path: file path of file which is NOK parsed
        :param json_hash: json hash of file which is NOK parsed
        :param error: error message
        :return: make basic logging
        """
        sub_elastic_hash = create_elastic_hash(self.company_id, self.import_type, self.request_type)

        self.general_process_logger.update_importer_process_flow(
            device_pid_info, self.zip_filename, sub_elastic_hash,
            status=EnumErrorType.WARNING.name,
            key_enum=MainMessage.VEND_IMPORTER_ERROR_OCCUR_SUB_MESSAGE.value,
            elastic_hash=sub_elastic_hash
        )
        self.general_process_logger.update_system_log_flow(
            device_pid_info, self.zip_filename,
            key_enum=MainMessage.VEND_IMPORTER_ERROR_OCCUR.value,
            logs_level=EnumErrorType.ERROR.name
        )
        self.general_process_logger.update_system_log_flow(
            self.company_id, self.import_type, device_pid_info, error,
            key_enum=MainMessage.VEND_IMPORTER_ERROR_OCCUR_DETAIL.value,
            logs_level=EnumErrorType.ERROR.name
        )
        self.general_process_logger.update_general_process_flow(
            device_pid_info, self.zip_filename,
            status=EnumErrorType.ERROR.name,
            key_enum=MainMessage.VEND_IMPORTER_ERROR_OCCUR.value,
            elastic_hash=sub_elastic_hash
        )
        self.general_process_logger.update_main_elastic_process(
            file_path=fail_path, elastic_hash=sub_elastic_hash,
            data_hash=json_hash,
            main_elastic_hash=self.main_elastic_hash
        )

    def eva_pa7_fields_handler(self, old_eva_pa_fields, new_eva_pa_fields, eva_file, machine_id, machine_external_id,
                               new_eva_decimal_points, file_timestamp):
        pa7_new = []
        pa7_old = []
        wrong_counter_file = []
        fail_processing_eva_file = []
        file_with_wrong_counter_details = []
        payment_type_cpi_list = ['CA', 'DB', 'DC', 'DD']
        total_vends = 0
        vends_array = []
        for old_eva_pa in old_eva_pa_fields:
            if old_eva_pa.startswith('PA7'):
                pa7_old.append(old_eva_pa)

        for new_eva_pa in new_eva_pa_fields:
            if new_eva_pa.startswith('PA7'):
                pa7_new.append(new_eva_pa)

        for price_list in payment_type_cpi_list:
            old_eva_fields = [x for x in pa7_old if str(
                get_vends_data_since_initialization(x, 2)) == price_list]
            new_eva_fields = [x for x in pa7_new if str(
                get_vends_data_since_initialization(x, 2)) == price_list]
            for new_eva_pa in new_eva_fields:
                processing_new_eva_price = '*'.join(new_eva_pa.split('*')[:4])
                final_processing = list(
                    filter(lambda y: processing_new_eva_price in y, old_eva_fields))
                difference = 0
                if final_processing:
                    difference = int(
                        get_vends_data_since_initialization(new_eva_pa, 5)) - int(
                        get_vends_data_since_initialization(final_processing[0], 5))

                    if difference < 0:
                        if self.zip_filename not in wrong_counter_file:
                            wrong_counter_file.append(self.zip_filename)
                        if eva_file not in fail_processing_eva_file:
                            fail_processing_eva_file.append(eva_file)

                        file_with_wrong_counter_details.append({
                            'file': self.zip_filename,
                            'device_pid': machine_id,
                            'machine_external_id': machine_external_id,
                            'difference': difference,
                            'final_processing': final_processing,
                            'eva_filename': eva_file,
                            'new_eva_field': new_eva_pa
                        })
                        continue

                    if difference >= 0:
                        total_vends += difference
                counter = 0
                for product in range(difference):
                    counter += 1
                    product_number = get_vends_data_since_initialization(new_eva_pa, 1)
                    transaction_id = u'{}-{}-{}'.format(price_list, product_number, counter)

                    if product_number.isdigit():
                        try:
                            total_price = int(
                                get_vends_data_since_initialization(new_eva_pa, 6)) - int(
                                get_vends_data_since_initialization(final_processing[0], 6)
                            )
                            calculated_price = total_price / difference
                            average_price = calculated_price / float(
                                get_decimal_points(new_eva_decimal_points))
                            vends_array.append({
                                'operator_identifier': machine_external_id,
                                'seTime': file_timestamp,
                                'product_code_in_map': int(product_number),
                                'seValue': average_price,
                                'payment_method_id': self.cpi_payment_type.get(price_list, 0),
                                'transaction_id': transaction_id,
                                'device_pid': machine_id,
                                'zip_filename': self.zip_filename})
                        except Exception as e:
                            fail_processing_eva_file.append(eva_file)
                            self.general_process_logger.update_system_log_flow(
                                self.company_id, self.import_type, machine_id, e,
                                key_enum=MainMessage.VEND_IMPORTER_ERROR_OCCUR_DETAIL.value,
                                logs_level=EnumErrorType.ERROR.name
                            )
                            # logger.error("Error description", exc_info=True)
                            continue
        response = {
            'vends_array': vends_array,
            'total_vends': total_vends,
            'wrong_counter_file': wrong_counter_file,
            'fail_processing_eva_file': fail_processing_eva_file,
            'file_with_wrong_counter_details': file_with_wrong_counter_details
        }
        return response

    def eva_pa_vend_calculation(self, new_pa_fields, old_pa_fields, eva_file, new_eva_decimal_points, file_timestamp,
                                machine_id, machine_external_id):
        wrong_counter_file = []
        fail_processing_eva_file = []
        file_with_wrong_counter_details = []
        total_vends = 0
        vends_array = []

        for new_eva_pa in new_pa_fields:
            processing_new_eva = '*'.join(new_eva_pa.split('*')[:2])
            final_processing = list(filter(lambda x: processing_new_eva in x, old_pa_fields))
            difference = 0
            if final_processing:
                new_eva_vend_since_init = get_vends_data_since_initialization(new_eva_pa, 2)
                old_eva_vend_since_init = get_vends_data_since_initialization(
                    final_processing[0], 2)
                difference = int(new_eva_vend_since_init) - int(old_eva_vend_since_init)
                if difference < 0:
                    if self.zip_filename not in wrong_counter_file:
                        wrong_counter_file.append(self.zip_filename)
                    if eva_file not in fail_processing_eva_file:
                        fail_processing_eva_file.append(eva_file)

                    file_with_wrong_counter_details.append({
                        'file': self.zip_filename,
                        'device_pid': machine_id,
                        'machine_external_id': machine_external_id,
                        'difference': difference,
                        'final_processing': final_processing,
                        'eva_filename': eva_file,
                        'new_eva_field': new_eva_pa
                    })
                    continue

                if difference >= 0:
                    total_vends += difference
            counter = 0
            if difference >= 0:
                for product in range(difference):
                    counter += 1
                    product_number = get_vends_data_since_initialization(new_eva_pa, 1)

                    define_transaction_id = u'{}-{}-{}'.format(0, product_number, counter)
                    if product_number.isdigit():
                        try:
                            total_price_new = get_vends_data_since_initialization(
                                new_eva_pa, 3)
                            total_price_old = get_vends_data_since_initialization(
                                final_processing[0], 3)
                            final_total_price = int(total_price_new) - int(total_price_old)
                            calculated_price = final_total_price / difference
                            average_price = calculated_price / float(
                                get_decimal_points(new_eva_decimal_points))

                            vends_array.append({
                                'operator_identifier': machine_external_id,
                                'seTime': file_timestamp,
                                'product_code_in_map': int(product_number),
                                'seValue': average_price,
                                'payment_method_id': self.cpi_payment_type['CA'],
                                'transaction_id': define_transaction_id,
                                'device_pid': machine_id,
                                'zip_filename': self.zip_filename,
                            })

                        except Exception as e:
                            vend_logger.error("Error description: ", e)
                            fail_processing_eva_file.append(eva_file)
                            # logger.error("Error description", exc_info=True)
                            continue

        response = {
            'vends_array': vends_array,
            'total_vends': total_vends,
            'wrong_counter_file': wrong_counter_file,
            'fail_processing_eva_file': fail_processing_eva_file,
            'file_with_wrong_counter_details': file_with_wrong_counter_details
        }
        return response


class EvaAfterCloudValidationTasks(object):
    def __init__(self, company_id, general_process_logger, import_type, request_type, working_directory, elastic_hash,
                 email, token):
        """
        This is main class for tasks after finish vend cloud validation, this is for CPI and DEX vends!

        :param company_id: company_id (int)
        :param general_process_logger: initialized process logger for elastic
        :param import_type: vend import type
        :param request_type: FTP/API
        :param working_directory: vend working dir
        :param elastic_hash: main elastic hash
        :param email: email
        :param token: token
        """
        self.company_id = company_id
        self.import_type = import_type
        self.request_type = request_type
        self.elastic_hash = elastic_hash
        self.general_process_logger = general_process_logger
        self.working_directory = working_directory
        self.email = email
        self.token = token

    def eva_final_validation(self, file_with_wrong_counter_details, total_vends, fail_dir, success_dir):
        """
        This method generate one elastic message per device PID, with details about wrong counter ,total vends per
        device pid, and general total vends, handle elastic message, clean working dir, etc ...

        :param file_with_wrong_counter_details:
        :param total_vends: total_vends (int)
        :param fail_dir: fail_dir path
        :param success_dir: success_dir path
        :return: elastic message and local working dir handler
        """

        clean_local_archive = CleanLocalHistory(process_logger=self.general_process_logger)

        if len(file_with_wrong_counter_details):
            sub_elastic_hash = create_elastic_hash(self.company_id, self.import_type, self.request_type)
            generate_message_per_pid = {}
            for element in file_with_wrong_counter_details:
                wrong_counter_machine_external_id = element['machine_external_id']
                wrong_counter_fields = element['final_processing']
                wrong_counter_eva_filename = element['eva_filename']
                wrong_counter_device_pid = element['device_pid']
                wrong_counter_new_eva_field = element['new_eva_field']
                wrong_counter_difference = element['difference']
                wrong_counter_file = element['file']

                generate_message_per_pid.setdefault(wrong_counter_device_pid, [])

                generate_message_per_pid[wrong_counter_device_pid].append({
                    'machine_external_id': wrong_counter_machine_external_id,
                    'eva_filename': wrong_counter_eva_filename,
                    'new_eva_field': wrong_counter_new_eva_field,
                    'difference': wrong_counter_difference,
                    'eva_fields': wrong_counter_fields,
                    'zip_filename': wrong_counter_file,
                })

            fail_processing_eva_file = []
            for key, value in generate_message_per_pid.items():
                self.general_process_logger.update_importer_process_flow(
                    key, sub_elastic_hash, status=EnumErrorType.WARNING.name, elastic_hash=self.elastic_hash,
                    key_enum=MainMessage.ERROR_OCCUR_ON_VEND_CALCULATE_SUB_MESSAGE.value,
                )
                for x in value:
                    self.general_process_logger.update_general_process_flow(
                        key, x['machine_external_id'], x['difference'], ''.join(x['eva_fields']), x['new_eva_field'],
                        x['zip_filename'], status=EnumErrorType.FAIL.name, elastic_hash=sub_elastic_hash,
                        key_enum=MainMessage.VEND_IMPORTER_ERROR_OCCUR_ON_VEND_CALCULATE.value,
                    )

                    if x['eva_filename'] not in fail_processing_eva_file:
                        fail_processing_eva_file.append(x['eva_filename'])

            # Make zip archive of fail file and update main elastic process
            filename_date = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            actual_fail_filename = '{}${}.zip'.format(self.company_id, filename_date)
            cloud_fail_zip_file_path = os.path.join(self.working_directory, actual_fail_filename)
            os.chdir(self.working_directory)
            eva_zip = zipfile.ZipFile(cloud_fail_zip_file_path, 'w', zipfile.ZIP_DEFLATED)

            for file in fail_processing_eva_file:
                eva_zip.write(file)

            eva_zip.close()
            shutil.copy2(cloud_fail_zip_file_path, fail_dir)
            clean_local_archive.delete_local_working_file(cloud_fail_zip_file_path)
            fail_file_path = os.path.join(fail_dir, actual_fail_filename)
            self.general_process_logger.update_main_elastic_process(
                file_path=fail_file_path, elastic_hash=sub_elastic_hash,
                data_hash='', main_elastic_hash=self.elastic_hash
            )

        # Define elastic message, vends per specific device!
        if total_vends:
            total = sum(total_vends.get('vends', 0))
            machine_device = total_vends.get('machine_device')
            machine_id = machine_device.get('machine_id')
            device_id = machine_device.get('device_pid')
            if self.import_type == 'CPI_VENDS':
                self.general_process_logger.update_general_process_flow(
                    device_id, machine_id, total, status=EnumErrorType.IN_PROGRESS.name,
                    key_enum=MainMessage.VEND_IMPORTER_NEW_VEND_FOUND_PER_DEVICE.value,
                    elastic_hash=self.elastic_hash)
            elif self.import_type == 'DEX_VENDS':
                self.general_process_logger.update_general_process_flow(
                    machine_id, total, status=EnumErrorType.IN_PROGRESS.name,
                    key_enum=MainMessage.DEX_VEND_IMPORTER_NEW_VEND_FOUND_PER_DEVICE.value,
                    elastic_hash=self.elastic_hash
                )

    def publish_eva_data_to_cloud(self, success_processing_eva_file, success_dir, reimport_list, reimport_filename,
                                  vends_array, machine_id, all_processing_eva_file, fail_processing_eva_file,
                                  datetime_format, filename_regex):
        """
        This method publish vends data to cloud, and make final task after cloud validation...

        :param success_processing_eva_file: success processed eva file (list)
        :param success_dir: success dir path (str)
        :param reimport_list: reimport info (list)
        :param reimport_filename: detected reimport eva (str)
        :param vends_array: generated final vends (list)
        :param machine_id: machine_id (str)
        :param all_processing_eva_file: total processed eva (list)
        :param fail_processing_eva_file: NOK processed eva (list)
        :param datetime_format: datetime format of eva file timestamp
        :param filename_regex: eva filename regex
        :return:
        """
        try:
            # Generate success filename, success file path, make success zip archive, clean working dir...
            # handle reimport, publish vends to CLOUD ...
            clean_local_archive = CleanLocalHistory(process_logger=self.general_process_logger)
            actual_date = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            actual_success_filename = '{}${}.zip'.format(self.company_id, actual_date)
            cloud_success_zip_file_path = os.path.join(self.working_directory, actual_success_filename)
            os.chdir(self.working_directory)
            eva_zip = zipfile.ZipFile(cloud_success_zip_file_path, 'w', zipfile.ZIP_DEFLATED)

            for file in success_processing_eva_file:
                if file not in fail_processing_eva_file:
                    eva_zip.write(file)

            eva_zip.close()
            shutil.copy2(cloud_success_zip_file_path, success_dir)
            success_file_path = os.path.join(success_dir, actual_success_filename)
            finally_reimport_list = []
            reimport_files = []
            if reimport_list:
                for init_key, init_value in reimport_list.items():
                    total_reimport_vends = 0
                    for value in init_value:
                        for k, v in value.items():
                            reimport_files.append(k)
                            total_reimport_vends += v
                    finally_reimport_list.append({init_key: total_reimport_vends})

                self.general_process_logger.update_general_process_flow(
                    ', '.join(reimport_filename), machine_id, status=EnumErrorType.IN_PROGRESS.name,
                    key_enum=MainMessage.VEND_REIMPORT_INFO.value, elastic_hash=self.elastic_hash)

                for element in finally_reimport_list:
                    for reimport_filename, reimport_total_vends in element.items():
                        self.general_process_logger.update_general_process_flow(
                            reimport_filename, reimport_total_vends, machine_id,
                            status=EnumErrorType.IN_PROGRESS.name, key_enum=MainMessage.VEND_REIMPORT_FILE.value,
                            elastic_hash=self.elastic_hash)

            company_query = CompanyQueryOnCloud.get_company_by_id(self.company_id)
            company_result = company_query.get('results')
            company_timezone = ''.join([x['company_timezone'] for x in company_result])

            publish_vend_message = {
                'vend_data': vends_array,
                'email': self.email,
                'token': self.token,
                'reimport': finally_reimport_list,
                'reimport_info': reimport_files,
                'company': self.company_id,
                'import_type': self.import_type,
                'vend_timezone': company_timezone,
                'elastic_hash': self.elastic_hash,
                'type_of_process': self.request_type,
                'success_file_path': success_file_path,
                'datetime_format': datetime_format,
                'filename_regex': filename_regex
            }
            self.general_process_logger.update_system_log_flow(
                self.company_id, self.import_type, vends_array,
                key_enum=MainMessage.VEND_EVA_DATA_PUBLISHED_TO_CLOUD_SYSTEM_LOG.value,
                logs_level=EnumErrorType.IN_PROGRESS.name)

            vend_publish_to_database(publish_vend_message)
            clean_local_archive.delete_local_working_file(cloud_success_zip_file_path)

            # Clean working directory after finish processing
            for file_path in all_processing_eva_file:
                clean_local_archive.delete_local_working_file(file_path)

        except Exception as e:
            self.general_process_logger.update_system_log_flow(
                e, key_enum=MainMessage.ERROR_OCCUR_ON_CREATING_ZIP_ARCHIVE.value,
                logs_level=EnumErrorType.ERROR.name)

