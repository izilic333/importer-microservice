from collections import Counter
import copy
import json
import os

from common.importers.cloud_db.machine_type import MachineTypeImporter
from common.logging.setup import logger
from common.mixin.elastic_login import ElasticCloudLoginFunctions
from common.mixin.enum_errors import EnumErrorType, PlanogramEnum
from common.mixin.enum_errors import EnumValidationMessage as Const
from common.mixin.enum_errors import enum_message_on_specific_language
from common.mixin.handle_file import ImportProcessHandler
from common.mixin.mixin import generate_hash_for_json
from common.mixin.validation_const import (
    ImportType, ImportAction, get_import_type_by_name, return_import_type_id_custom_validation)
from common.importers.cloud_db.common import InvalidImportData
from common.synchronize.request import ParseRabbitRequests
from common.validators.cloud_db.common_validators import MachineTypeValidator
from common.rabbit_mq.database_interaction_q.db_publisher import PublishJsonFileToDatabaseQ
from common.importers.cloud_db.planogram import PlanogramValidator
from common.validators.cloud_db.user_validator import UserValidator, DuplicateRowsError, DbQueryError
from database.cloud_database.core.query import (
    ClientQueryOnCloud, ClientTypeQueryOnCloud,
    MachineQueryOnCloud, LocationQueryOnCloud, MachineTypeQueryOnCloud,
    RegionQueryOnCloud, ProductQueryOnCloud, PackingsQueryOnCloud, CompanyQueryOnCloud,
    MachineCategoryQueryOnCloud, WarehouseQueryOnCloud, ProductRotationGroupQueryOnCloud)
from database.company_database.core.query_export import ExportHistory
from database.company_database.core.query_history import CompanyFailHistory, CompanyHistory
from elasticsearch_component.core.logger import CompanyProcessLogger
import psycopg2

logger_api = logger
empty_header_value = PlanogramEnum.EMPTY_HEADER_VALUE.value


def get_values_from_dict_arr(in_dict, key):
    return [m[key] for m in in_dict]


def remove_duplicated(work_on, from_data, check_names=True):
    # remove duplicated external id-s
    prefix = work_on.lower() + '_'
    control_array = []
    for m in from_data:
        if m[prefix + 'id'] not in control_array:
            control_array.append(m[prefix + 'id'])
            m['to_delete'] = False
        else:
            m['to_delete'] = True

    removed_ids = [m for m in from_data if m.get('to_delete', False) is True]
    out_data = [m for m in from_data if m.get('to_delete', False) is False]

    if check_names:
        # remove duplicated names
        control_array = []
        for m in out_data:
            if m[prefix + 'name'] not in control_array:
                control_array.append(m[prefix + 'name'])
                m['to_delete'] = False
            else:
                m['to_delete'] = True

        removed_names = [m for m in out_data if m.get('to_delete', False) is True]
        out_data = [m for m in out_data if m.get('to_delete', False) is False]
    else:
        removed_names = []

    return out_data, removed_ids, removed_names


def get_alive_and_dead(external_ids, all_entities):
    entities_with_ids = [e for e in all_entities if e['ext_id'] in external_ids]
    entities_alive = [e for e in entities_with_ids if e['alive'] is True]
    entities_dead = [e for e in entities_with_ids if e['alive'] is False]
    alive_ids = get_values_from_dict_arr(entities_alive, 'ext_id')
    dead_ids = get_values_from_dict_arr(entities_dead, 'ext_id')
    dead_ids = [id for id in dead_ids if id not in alive_ids]

    return alive_ids, dead_ids


def tax_rate_doesnt_exist(company_id, tax_rates_from_file):
    """
    Checks if any of the tax_rates given from file can't be found in db.
    """
    tax_rates_from_db = []
    tax_rates_query = ProductQueryOnCloud.get_tax_rates_for_company(company_id)
    for tax_rate_dict in tax_rates_query:
        floated_tax_rate = float(tax_rate_dict['value'])
        tax_rates_from_db.append(floated_tax_rate)

    tax_rates_not_found = []
    for tax_rate in tax_rates_from_file:
        # if given actual value for tax_rate
        if tax_rate and not tax_rate == '<null>':
            if float(tax_rate) not in tax_rates_from_db \
                    and tax_rate not in tax_rates_not_found:
                tax_rates_not_found.append(tax_rate)

    return tax_rates_not_found


def tax_rate_duplicates(company_id):
    """Creates a list of duplicate tax rates for a company."""
    duplicates_found = []
    tax_rates = ProductQueryOnCloud.get_tax_rates_for_company(company_id)
    tax_rates_list = [tr['value'] for tr in tax_rates]
    occurrences = Counter(tax_rates_list)

    for i in occurrences:
        if occurrences[i] > 1:
            duplicates_found.append(i)

    return duplicates_found


def check_field_uniqueness(data_from_file, data_from_db):
    """
    Check field values for uniqueness
    1) if creating, must be unique
    2) if updating must be different than others
    3) check against data from file, too

    Keyword arguments:
    data_from_file -- list of lists [[product_id, field_to_check, product_action], ...]
    data_from_db -- list of lists [[product_id, field_to_check], ...]
    """
    data_from_db_list = [b[1] for b in data_from_db]
    data_from_file_list = [b[1] for b in data_from_file]

    # check uniqueness with data from file
    not_unique_in_file = []
    occurrences_in_file = Counter(data_from_file_list)
    for i in occurrences_in_file:
        if i not in (None, '', '<null>') and occurrences_in_file[i] > 1:
            not_unique_in_file.append(i)

    # check uniqueness with data from db
    not_unique_with_db = []
    for j in data_from_file:
        product_id = j[0]
        value = j[1]
        action = int(j[2])

        # create product action
        if action == 0:
            if value not in (None, '', '<null>') and value in data_from_db_list:
                if value not in not_unique_with_db: # don't add if already added
                    not_unique_with_db.append(value)

        elif action == 1: # update action
            # remove from data_from_db to not check against it
            update_combination = [product_id, value]
            data_from_db_copy = data_from_db[:]
            if update_combination in data_from_db:
                del data_from_db_copy[data_from_db_copy.index(update_combination)]
                data_from_db_list = [b[1] for b in data_from_db_copy]

            if value not in (None, '', '<null>') and value in data_from_db_list:
                if value not in not_unique_with_db: # don't add if already added
                    not_unique_with_db.append(value)

    return not_unique_in_file, not_unique_with_db


class FileOnCloudValidator(object):
    """
    Class implements functions for validations against data on televend cloud.

    It is using sql alchemy core functionality to obtain information from cloud and makes basic logical evaluation
    of requested import like:
    - if delete or update operation is requested, checks if entity for deletion is existing in cloud
    - if insert operation is requested, checks if external_id already exists in cloud
    """
    def __init__(self, body):
        """
        Data that is used for validation is stored into instance for reuse during validation process
        :param body: body of the message coming from rabbitmq, it contains actual data to be validated
        """
        self.body = body
        self.elastic_hash = body['elastic_hash']
        self.company_id = body['company_id']
        self.data = body['data']
        self.json_hash = generate_hash_for_json(self.data)
        self.import_type = get_import_type_by_name(body['type'])
        self.input_file = body['input_file']
        self.token = body['token']
        self.errors = []
        self.warning = []
        self.email = body['email'] if body['email'] else ''
        self.language = body['language']

    def save_error_and_finish_main_process(self, message):
        """
        IF critical logical or code error is found during cloud validation, process logging needs to be updated and
        main elastic logging process needs to be closed:
         . local rotated file log is appended with error
         - elastic process messages for cloud is inserted
         - elastic internal importer process message is inserted
         - main elastic process is closed by error
         - local database is updated with error
        :param message: message which is inserted in all logs
        :return:
        """
        logger_api.error(message)
        ElasticCloudLoginFunctions.create_process_flow(self.elastic_hash, EnumErrorType.ERROR.name, message)
        ElasticCloudLoginFunctions.create_cloud_process_flow(
            self.elastic_hash, EnumErrorType.ERROR.name, message)

        ElasticCloudLoginFunctions.update_main_process(self.elastic_hash,
                                                       EnumErrorType.ERROR.name)
        ElasticCloudLoginFunctions.update_local_database_error(
            self.company_id, self.import_type.value['id'], self.elastic_hash, self.json_hash,
            self.body['input_file'], EnumErrorType.ERROR.value, self.token)
        # cloud validator update redis key, process for this company finished,

        redis_import_process_handler = ImportProcessHandler(
            company_id=self.company_id,
            elastic_hash=self.elastic_hash,
            file_path=self.input_file,
            import_type=self.import_type,
        )
        redis_import_process_handler.finish_import_process_redis(
            finished_by='cloud validator', reason='save_error_and_finish_main_process'
        )

    def emosl(self, const, *args):
        return enum_message_on_specific_language(const.value, self.language, *args)

    def validate_file_on_cloud(self):
        """
        Main function which is called from rabbitmq queue "validator_message" using kombu library
        Import type is checked and if recognised, appropriate validator (instance method) is called.
        :return:
        """
        hist, _ = CompanyHistory.exists_in_history(self.company_id, self.json_hash)

        if hist:
            self.save_error_and_finish_main_process(
                self.emosl(Const.DATABASE_VALIDATION_ERROR, self.emosl(Const.DATABASE_REPEAT_IMPORT_ERROR))
            )
            return False

        try:
            name_import = self.emosl(Const.DATABASE_VALIDATION_STARTED, '{}'.format(self.import_type.name))
            name_const = '{}'.format(EnumErrorType.IN_PROGRESS.name)

            ElasticCloudLoginFunctions.create_process_flow(
                self.elastic_hash, name_const, name_import
            )
        except Exception as e:
            logger_api.error("Elastic search error: {}".format(e))

        # check that all actions are either 50 or without 50
        actions = []

        for entry in self.data:
            for k, v in entry.items():
                if k.upper().endswith('_ACTION') and int(v) == ImportAction.UNKNOWN.value:
                    actions.append(int(v))
        if actions and len(actions) != len(self.data):
            self.save_error_and_finish_main_process(self.emosl(Const.DATABASE_UNKNOWN_TYPE_ERROR))
            return False

        if self.import_type is ImportType.MACHINES:
            return self.validate_machines()
        elif self.import_type is ImportType.LOCATIONS:
            return self.validate_locations()
        elif self.import_type is ImportType.MACHINE_TYPES:
            return self.validate_machine_types()
        elif self.import_type is ImportType.REGIONS:
            return self.validate_region()
        elif self.import_type is ImportType.PRODUCTS:
            return self.validate_product()
        elif self.import_type is ImportType.CLIENTS:
            return self.validate_client()
        elif self.import_type is ImportType.PLANOGRAMS:
            return self.validate_planogram()
        elif self.import_type is ImportType.USERS:
            return self.validate_user()
        elif self.import_type is ImportType.PACKINGS:
            return self.validate_packing()

        self.save_error_and_finish_main_process(self.emosl(Const.DATABASE_UNKNOWN_IMPORT_ERROR, self.import_type.name))

        return False

    def write_cloud_and_process_flow_validation_errror(self, message):
        """
        Calls error logging for importer process (detailed messages) and cloud process (messages visible to cloud) into
        local Elastic using direct loggers.
        :param message: Message to be logged
        :return:
        """
        CompanyProcessLogger.create_process_flow(
            self.elastic_hash, message, EnumErrorType.ERROR.name)
        CompanyProcessLogger.create_cloud_process_flow(
            self.elastic_hash, EnumErrorType.ERROR.name, message)

    def write_cloud_and_process_flow_validation_warining(self, message):
        """
        Calls error logging for importer process (detailed messages) and cloud process (messages visible to cloud) into
        local Elastic using direct loggers.
        :param message: Message to be logged
        :return:
        """
        CompanyProcessLogger.create_process_flow(
            self.elastic_hash, message, EnumErrorType.WARNING.name)
        CompanyProcessLogger.create_cloud_process_flow(
            self.elastic_hash, EnumErrorType.WARNING.name, message)

    def write_warnings(self, warning_type=None):
        self.write_cloud_and_process_flow_validation_warining(self.emosl(Const.DATABASE_VALIDATION_WARNING, ''))
        self.write_cloud_and_process_flow_validation_warining(json.dumps(self.warning))

        logger_api.info(self.emosl(Const.DATABASE_VALIDATION_ERROR, json.dumps(self.warning)))

    def write_errors(self, error_type=None):
        """
        Logs logical errors in validation process. Those errors are resulted from logical data checking against cloud
        database using sqlalchemy requests against cloud database.
        Errors are written into elastic, main process logger and local postgres database company fail history.
        Notification about errors in import is sent to the end user via email
        :param errors: errors to be written in loggers
        :param error_type: error type to be written into local database for company
        :return:
        """
        self.write_cloud_and_process_flow_validation_errror(self.emosl(Const.DATABASE_VALIDATION_ERROR, ''))
        self.write_cloud_and_process_flow_validation_errror(json.dumps(self.errors))
        CompanyProcessLogger.update_main_process(self.elastic_hash, EnumErrorType.ERROR.name)

        CompanyFailHistory.insert_fail_history(
            company_id=self.company_id,
            import_type=self.import_type.name,
            elastic_hash=self.elastic_hash,
            data_hash=self.json_hash,
            file_path=self.input_file,
            import_error_type=error_type,
            token=self.token
        )

        if self.import_type not in [ImportType.PLANOGRAMS, ImportType.MACHINE_TYPES]:

            # cloud validator update redis key, process for this company finished,

            redis_import_process_handler = ImportProcessHandler(
                company_id=self.company_id,
                elastic_hash=self.elastic_hash,
                file_path=self.input_file,
                import_type=self.import_type)
            redis_import_process_handler.finish_import_process_redis(finished_by='cloud validator', reason='write_errors')

        logger_api.error(self.emosl(Const.DATABASE_VALIDATION_ERROR, json.dumps(self.errors)))

        # Call function and send EMAIL
        if self.email:
            import threading
            prepared_data = {
                'company_id': self.company_id,
                'hash': self.elastic_hash,
                'email': self.email,
                'import_type': self.import_type
            }
            threading.Thread(
                target=ExportHistory.export_specific_hash, args=(prepared_data,), daemon=True
            ).start()

        return

    def write_history(self):
        CompanyProcessLogger.create_process_flow(
            self.elastic_hash,
            self.emosl(Const.DATABASE_VALIDATION_SUCCESS, os.path.split(self.input_file)[-1]),
            EnumErrorType.IN_PROGRESS.name
        )

        cloud_inserted = False
        if self.import_type is ImportType.PLANOGRAMS or self.import_type is ImportType.MACHINE_TYPES:
            cloud_inserted = True

        CompanyHistory.insert_history(
            company_id=self.company_id,
            import_json=self.data,
            import_type=return_import_type_id_custom_validation(self.import_type.name),
            elastic_hash=self.elastic_hash,
            data_hash=self.json_hash,
            file_path=self.input_file,
            token=self.token,
            cloud_inserted=cloud_inserted
        )

        logger_api.info(self.emosl(Const.DATABASE_VALIDATION_SUCCESS, self.input_file))
        return

    def write_import_history(self, import_stats):
        def insert_into_elastic(data, hash, progress):
            ElasticCloudLoginFunctions.create_cloud_process_flow(
                hash, progress, json.dumps(data))

            ElasticCloudLoginFunctions.create_process_flow(
                hash, progress, json.dumps(data))
            return

        error_name = EnumErrorType.SUCCESS.name
        message = 'Data imported into cloud database {}'.format(self.import_type.name)
        insert_msg = enum_message_on_specific_language(
            Const.DATABASE_IMPORT_STATS.value, 'en', import_stats['inserted'],
            import_stats['updated'], import_stats['deleted'], import_stats['errors']
        )
        insert_into_elastic(insert_msg, self.elastic_hash, error_name)

        # Start elastic process
        ParseRabbitRequests.save_logging_message({'elastic_hash': self.elastic_hash},
                                                 error_name, message)

        update_method = (
            CompanyHistory.update_local_finish_process(
                self.elastic_hash, import_stats, import_stats, False)
        )

        if not update_method:
            logger.info(
                'Error update local process: {} {}'.format(self.import_type.name, import_stats))
        logger.info('Updated local process: {} {}'.format(self.import_type.name, import_stats))

    def write_history_and_publish(self):
        """
        This function is called, if validation process is successful. Cloud validation process stage completion is
        logged into elastic and into company history. Local logger is logging successful stage competion
        and new message is published into cloud importer rabbitmq queue named 'database_message'.
        :return:
        """
        CompanyProcessLogger.create_process_flow(
            self.elastic_hash,
            self.emosl(Const.DATABASE_VALIDATION_SUCCESS, os.path.split(self.input_file)[-1]),
            EnumErrorType.IN_PROGRESS.name
        )

        CompanyHistory.insert_history(
            self.company_id, self.data,
            return_import_type_id_custom_validation(self.import_type.name),
            self.elastic_hash, self.json_hash, self.input_file, self.token
        )

        logger_api.info(self.emosl(Const.DATABASE_VALIDATION_SUCCESS, self.input_file))

        PublishJsonFileToDatabaseQ.publish_new_data_to_database(
            self.company_id, self.elastic_hash, self.data, self.import_type.name, self.token,
            self.email
        )

        return

    def append_error(self, record_part, message_part):
        """
        Appending an error dict to the internal array which has record identification part and message part
        :param record_part: record identification and details part
        :param message_part: error message part
        :return:
        """
        self.errors.append({
            'record': record_part,
            'message': message_part
        })

    def append_warning(self, record_part, message_part):
        """
        Appending an warning dict to the internal array which has record identification part and message part
        :param record_part: record identification and details part
        :param message_part: error message part
        :return:
        """
        self.warning.append({
            'record': record_part,
            'message': message_part
        })

    def write_duplicated_errors(self, work_on, removed_ids, removed_names):
        prefix = work_on.lower() + '_'
        id_fld_name = prefix + 'id'
        name_fld = prefix + 'name'
        for id in removed_ids:
            self.append_error(
                id[id_fld_name],
                self.emosl(Const.DATABASE_VALIDATION_ERROR, "Repeated {}".format(id_fld_name))
            )
        for id in removed_names:
            self.append_error(
                id[id_fld_name],
                self.emosl(Const.DATABASE_VALIDATION_ERROR, 'Repeated {} {}'.format(name_fld, id[name_fld]))
            )
        self.write_errors(error_type=EnumErrorType.ERROR.value)

    @staticmethod
    def check_message_already_exist(list_of_dict, message, key):

        check_message_status = next(filter(lambda obj: obj.get(key) == message, list_of_dict), None)

        return check_message_status

    def handle_field_uniqueness(
        self,
        field,
        fields_from_file,
        all_alive_cloud_products,
        product_ext_id=None,
        barcode_validation=None
    ):
        """
        Check field uniqueness and report errors, if found:

        Keyword arguments:
        field -- name of field, i.e. 'barcode'
        fields_from_file -- field values collected from file data
        all_alive_cloud_products -- Product query result
        """
        fields_from_db = [[p['ext_id'], p[field]] for p in all_alive_cloud_products]
        fields_not_unique_in_file, fields_not_unique_with_db = check_field_uniqueness(
            fields_from_file,
            fields_from_db
        )

        not_unique_field_message = self.emosl(
            Const.FIELD_UNIQUENESS_IN_FILE, field,
            ', '.join(fields_not_unique_in_file)
        )

        check_message_status = self.check_message_already_exist(
            list_of_dict=self.errors,
            message=not_unique_field_message,
            key='message'
        )
        # Check field uniqueness, product import file
        if fields_not_unique_in_file and not check_message_status:
            self.append_error(
                ', '.join(fields_not_unique_in_file),
                not_unique_field_message
            )
        # Check field uniqueness, product company database level
        if fields_not_unique_with_db:
            field_not_unique = ', '.join(fields_not_unique_with_db)

            try:
                db_product_ext_id = list(
                    filter(lambda x: x[1] == field_not_unique, fields_from_db)
                )

                if db_product_ext_id:
                    db_product_ext_id = db_product_ext_id[0][0]

            except Exception as e:
                logger_api.error("Error occurred on product import, barcode unique validation, error: {}".format(e))
                db_product_ext_id = ""

            if barcode_validation and db_product_ext_id == product_ext_id:
                # barcode validation, update barcode with same values is allowed!

                logger_api.info("Update barcode on product: {}, fields_from_file: {}, field: {}".format(
                    product_ext_id, fields_from_file, field)
                )
                return

            if db_product_ext_id:

                # Field is expected to be unique for each product, message with product ext_id
                db_not_unique_field_message_1 = self.emosl(
                    Const.FIELD_NOT_UNIQUE_PRODUCT_EXTERNAL_ID,
                    field, field_not_unique, db_product_ext_id
                )

                check_message_status = self.check_message_already_exist(
                    list_of_dict=self.errors,
                    message=db_not_unique_field_message_1,
                    key='message'
                )

                if not check_message_status:
                    self.append_error(
                        ', '.join(fields_not_unique_with_db), db_not_unique_field_message_1
                    )

            else:
                # Field is expected to be unique for each product, message without product ext_id
                db_not_unique_field_message_2 = (
                    self.emosl(
                        Const.FIELD_NOT_UNIQUE,
                        field,
                        field_not_unique,
                    )
                )
                check_message_status = self.check_message_already_exist(
                    list_of_dict=self.errors,
                    message=db_not_unique_field_message_2,
                    key='message'
                )

                if not check_message_status:
                    self.append_error(
                        ', '.join(fields_not_unique_with_db), db_not_unique_field_message_2
                    )

    @staticmethod
    def get_upsert_if_needed(action, entity_id, alive_ids):
        """

        :param action: ImportAction.value
        :param entity_data: item
        :param entity_id: item_id
        :param alive_ids: all alive ids
        :return: ImportAction.CREATE if
        """

        entity_exists = entity_id in alive_ids

        # UPDATE and NOT EXISTS
        if action == ImportAction.UPDATE and not entity_exists:
            return ImportAction.CREATE, ImportAction.CREATE.value

        if action == ImportAction.CREATE and entity_exists:
            return ImportAction.UPDATE, ImportAction.UPDATE.value

        return action, action.value

    def validate_meter_readings(self, machine_data, meter_type_external_ids,
                                company_meter_readings):
        machine_id = machine_data.get('machine_id', None)
        if machine_data.get('meter_reading_tracking', 0) in ('<null>', None, ''):
            return True
        else:
            meter_reading_tracking = bool(int(machine_data.get('meter_reading_tracking', 0)))
        meter_readings = machine_data.get('meter_readings_list', None)

        # CHECK IF NO METER READINGS
        if not meter_reading_tracking:
            return True

        # CHECKOUT IF COMPANY HAS METER READINGS
        if not company_meter_readings:
            message = self.emosl(Const.METER_READINGS_LIST_COMPANY_HAS_NO_SETTING, machine_id)
            self.append_error('meter_readings_list', message)
            return False

        machine_readings_list = meter_readings.split('#')
        if meter_readings in ('<null>', None, ''):
            machine_readings_list = []

        # CHECKING EXISTENCE OF METER TYPE
        reading_key_list = []
        for reading in machine_readings_list:
            key_value = reading.split(':')
            key = key_value[0]

            if key in reading_key_list:
                self.append_error(key, self.emosl(Const.METER_READINGS_LIST_DUPLICATE, key,
                                    machine_id))
            if key not in meter_type_external_ids:
                self.append_error(key, self.emosl(Const.METER_READINGS_LIST_DOES_NOT_EXISTS, key))
            reading_key_list.append(key)
        return True

    def validate_machines(self):
        """
        Makes machines import request validation against cloud database. Data needed to be validated are inside
        class instance. Basic validation process is described in class description. Specific additiona validation
        made is check if requested location and machine type exist if update or insert is requested-
        :return: True - no errors found, False if any error
        """
        work_on = ImportType.MACHINES.value['capitalised_name']
        id_fld = work_on.lower() + '_id'
        name_fld = work_on.lower() + '_name'
        action_fld = work_on.lower() + '_action'

        working_data = copy.deepcopy(self.data)

        working_data, removed_ids, removed_names = remove_duplicated(work_on, working_data)

        def check_exists_machine_cluster_id(mc_id):
            if mc_id not in [None, '']:
                return LocationQueryOnCloud.machine_cluster_id(mc_id, self.company_id)
            else:
                return False
        # Do not accept files with repeated external ids and names
        if removed_ids or removed_names:
            self.write_duplicated_errors(work_on, removed_ids, removed_names)
            return False

        cloud_machines_all = MachineQueryOnCloud.get_machines_for_company(self.company_id)
        if not cloud_machines_all['status']:
            self.append_error(work_on, self.emosl(Const.DATABASE_QUERY_ERROR, work_on))
            self.write_errors(error_type=EnumErrorType.ERROR.value)
            return False
        else:
            cloud_machines_all = cloud_machines_all['results']

        all_alive_cloud_machines = [m for m in cloud_machines_all if m['alive'] is True]
        all_alive_cloud_machines_ids = get_values_from_dict_arr(all_alive_cloud_machines, 'ext_id')

        external_ids = set([m[id_fld] for m in working_data])
        alive_ids, dead_ids = get_alive_and_dead(external_ids, cloud_machines_all)
        all_work_ids = alive_ids + dead_ids

        try:
            action_50 = int(self.data[0][action_fld]) == 50
        except ValueError:
            action_50 = False

        if action_50:
            for m in [m for m in working_data if m[id_fld] in all_work_ids]:
                m[action_fld] = ImportAction.UPDATE.value
                m['resurrect_entity'] = True if m[id_fld] in dead_ids else False
            for m in [m for m in working_data if m[id_fld] not in all_work_ids]:
                m[action_fld] = ImportAction.CREATE.value

            # all machine IDs which are not to be updated or inserted are to be deleted and we need to add 'em
            for m in [m for m in all_alive_cloud_machines if m['ext_id'] not in external_ids]:
                working_data = [{
                    id_fld: m['ext_id'],
                    name_fld: m['name'],
                    action_fld: ImportAction.DELETE.value,
                    'action50_delete': True,
                    'cloud_id': m['id'],
                    'cluster_id':m['cluster_id'],
                    'machine_location_id': m['location_id'],
                    'machine_type_id': m['type_id']
                }] + working_data
            # refresh ID's
            external_ids = set([m[id_fld] for m in working_data])
            alive_ids, dead_ids = get_alive_and_dead(external_ids, cloud_machines_all)

        # meke sure that this is not performed on inserted entities i.e. must have m.get('machine_location_id')
        locs_types = [(m['machine_location_id'], m['machine_type_id']) for m in working_data
                      if m[action_fld] != ImportAction.DELETE.value and m.get('machine_location_id')]
        location_ids, types_ids = zip(*locs_types)
        locations = LocationQueryOnCloud.get_locations_by_external_ids(self.company_id, set(location_ids))
        if not locations['status']:
            self.append_error('', self.emosl(Const.DATABASE_CONNECTION, 'machine_location_id'))
            self.write_errors(error_type=EnumErrorType.ERROR.value)
            return
        locations = locations['results']
        location_ids = set(get_values_from_dict_arr(locations, 'ext_id'))
        types = MachineTypeQueryOnCloud.get_machine_type_by_external_ids(self.company_id, set(types_ids))
        if not types['status']:
            self.append_error('', self.emosl(Const.DATABASE_CONNECTION, 'machine_type_id'))
            self.write_errors(error_type=EnumErrorType.ERROR.value)
            return
        types = types['results']
        types_ids = set(get_values_from_dict_arr(types, 'ext_id'))

        machine_categories_cloud = MachineCategoryQueryOnCloud.get_categories_for_company(self.company_id)
        if not machine_categories_cloud['status']:
            self.append_error(work_on, self.emosl(Const.DATABASE_QUERY_ERROR, work_on))
            self.write_errors(error_type=EnumErrorType.ERROR.value)
            return False
        else:
            machine_categories_cloud = machine_categories_cloud['results']
        machine_categories_ids = get_values_from_dict_arr(machine_categories_cloud, 'ext_id')

        warehouses_cloud = WarehouseQueryOnCloud.get_warehouses_for_company(self.company_id)
        if not warehouses_cloud['status']:
            self.append_error(work_on, self.emosl(Const.DATABASE_QUERY_ERROR, work_on))
            self.write_errors(error_type=EnumErrorType.ERROR.value)
            return False
        else:
            warehouses_cloud = warehouses_cloud['results']
        warehouses_ids = get_values_from_dict_arr(warehouses_cloud, 'ext_id')

        meter_type_external_ids = MachineQueryOnCloud.get_meter_type_keys(self.company_id)

        company_meter_readings = CompanyQueryOnCloud.get_company_meter_readings_by_id(self.company_id)['results']
        for w_data in working_data:
            entity_id = w_data[id_fld]
            entity_name = w_data[name_fld]
            cloud_clusters = w_data.get('cluster_id')
            split_clusters = cloud_clusters.split('#') if cloud_clusters else None
            machine_cluster_id = split_clusters[0] if split_clusters else None

            machine_category_id = w_data.get('machine_category_id', None)
            if machine_category_id and machine_category_id in ['', '<null>']:
                machine_category_id = None

            warehouse_id = w_data.get('location_warehouse_id', None)
            if warehouse_id and warehouse_id in ['', '<null>']:
                warehouse_id = None

            if machine_cluster_id not in [None, '', '<null>']:
                mc_response_from_db = check_exists_machine_cluster_id(machine_cluster_id)
                if not mc_response_from_db:
                    self.append_warning(entity_id, self.emosl(Const.MACHINE_CLUSTER_ID, machine_cluster_id))

            try:
                action = ImportAction(int(w_data[action_fld]))
                if not w_data.get('resurrect_entity'):
                    action, w_data[action_fld] = self.get_upsert_if_needed(action, entity_id, alive_ids)

            except ValueError:
                self.append_error(entity_id, self.emosl(Const.DATABASE_WRONG_ACTION, work_on, entity_name))
                continue

            if machine_category_id:
                if machine_category_id not in machine_categories_ids:
                    self.append_error(
                        entity_id,
                        self.emosl(
                            Const.DATABASE_NO_MACHINE_CATEGORY,
                            machine_category_id
                        )
                    )

            if warehouse_id:
                if warehouse_id not in warehouses_ids:
                    self.append_error(
                        entity_id,
                        self.emosl(
                            Const.DATABASE_NO_WAREHOUSE,
                            warehouse_id
                        )
                    )

            if action == ImportAction.UPDATE:
                # to be updated, machine must exist in cloud database
                if w_data.get('resurrect_entity'):
                    if entity_id not in dead_ids:
                        self.append_error(entity_id,
                                          self.emosl(Const.DATABASE_NOT_FOUND, work_on, entity_name, action.name))
                else:
                    if entity_id not in alive_ids:
                        self.append_error(entity_id, self.emosl(Const.DATABASE_NOT_FOUND, work_on, entity_name, action.name))

            elif action == ImportAction.DELETE:
                # to be deleted, machine must exist in cloud database
                if entity_id not in all_alive_cloud_machines_ids:
                    self.append_error(entity_id,
                                      self.emosl(Const.DATABASE_NOT_FOUND, work_on, entity_name, action.name))
                    continue
            elif action == ImportAction.CREATE:
                # new machine is to be inserted into database
                if entity_id in all_alive_cloud_machines_ids:
                    self.append_error(entity_id, self.emosl(Const.DATABASE_FOUND, work_on, entity_name, action.name))
                    continue
            if action != ImportAction.DELETE:
                # check if requested location exists
                if w_data.get('machine_location_id') and w_data.get('machine_location_id') not in location_ids:
                    self.append_error(entity_id,
                                      self.emosl(Const.DATABASE_NO_LOCATION, w_data['machine_location_id']))
                if w_data.get('machine_type_id') not in empty_header_value and w_data.get('machine_type_id') not in types_ids:
                    self.append_error(entity_id,
                                      self.emosl(Const.DATABASE_NO_MACHINE_TYPE, w_data['machine_type_id']))

            if action in (ImportAction.CREATE, ImportAction.UPDATE, ImportAction.UNKNOWN):
                self.validate_meter_readings(w_data, meter_type_external_ids,
                    company_meter_readings)

        if self.errors:
            self.write_errors(error_type=EnumErrorType.ERROR.value)
            return False

        if self.warning:
            self.write_warnings(warning_type=EnumErrorType.WARNING.value)

        self.data = working_data
        self.write_history_and_publish()
        return True

    def validate_locations(self):
        """
        Makes location import request validation against cloud database. Data needed to be validated are inside
        class instance. Basic validation process is described in class description.
        :return: True - no errors found, False if any error
        """
        # validate import of locations against database
        work_on = ImportType.LOCATIONS.value['capitalised_name']
        id_fld = work_on.lower() + '_id'
        name_fld = work_on.lower() + '_name'
        action_fld = work_on.lower() + '_action'

        working_data = copy.deepcopy(self.data)
        working_data, removed_ids, removed_names = remove_duplicated(work_on, working_data)

        # Do not accept files with repeated external ids and names
        if removed_ids or removed_names:
            self.write_duplicated_errors(work_on, removed_ids, removed_names)
            return False

        cloud_locations_all = LocationQueryOnCloud.get_locations_for_company(self.company_id)
        if not cloud_locations_all['status']:
            self.append_error(work_on, self.emosl(Const.DATABASE_QUERY_ERROR, work_on))
            self.write_errors(error_type=EnumErrorType.ERROR.value)
            return False
        else:
            cloud_locations_all = cloud_locations_all['results']

        all_alive_cloud_locations = [m for m in cloud_locations_all if m['alive'] is True]
        all_alive_cloud_locations_ids = get_values_from_dict_arr(all_alive_cloud_locations, 'ext_id')

        external_ids = set([m[id_fld] for m in working_data])
        alive_ids, dead_ids = get_alive_and_dead(external_ids, cloud_locations_all)
        all_work_ids = alive_ids + dead_ids

        try:
            action_50 = int(self.data[0][action_fld]) == 50
        except ValueError:
            logger_api.exception("Error getting action")
            action_50 = False

        if action_50:
            for m in [m for m in working_data if m[id_fld] in all_work_ids]:
                m[action_fld] = ImportAction.UPDATE.value
                m['resurrect_entity'] = True if m[id_fld] in dead_ids else False
            for m in [m for m in working_data if m[id_fld] not in all_work_ids]:
                m[action_fld] = ImportAction.CREATE.value

            # all machine IDs which are not to be updated or inserted are to be deleted and we need to add 'em
            for m in [m for m in all_alive_cloud_locations if m['ext_id'] not in external_ids]:
                working_data = [{
                    id_fld: m['ext_id'],
                    name_fld: m['name'],
                    action_fld: ImportAction.DELETE.value,
                    'action50_delete': True,
                    'cloud_id': m['id']
                }] + working_data
            # refresh ID's
            external_ids = set([m[id_fld] for m in working_data])
            alive_ids, dead_ids = get_alive_and_dead(external_ids, cloud_locations_all)

        regions_cloud = RegionQueryOnCloud.get_regions_for_company(self.company_id)
        if not regions_cloud['status']:
            self.append_error(work_on, self.emosl(Const.DATABASE_QUERY_ERROR, work_on))
            self.write_errors(error_type=EnumErrorType.ERROR.value)
            return False
        else:
            regions_cloud = regions_cloud['results']
            region_ids = get_values_from_dict_arr(regions_cloud, 'ext_id')

        for w_item in working_data:
            entity_id = w_item[id_fld]
            entity_name = w_item[name_fld]
            region_id = w_item.get('region_id', None)
            if region_id and region_id in ['', '<null>']:
                region_id = None

            try:
                action = ImportAction(int(w_item[action_fld]))
                if not w_item.get('resurrect_entity'):
                    action, w_item[action_fld] = self.get_upsert_if_needed(action, entity_id, alive_ids)
            except ValueError:
                self.append_error(entity_id, self.emosl(Const.DATABASE_WRONG_ACTION, work_on, w_item[action_fld]))
                continue

            if region_id:
                if region_id not in region_ids:
                    self.append_error(
                        entity_id,
                        self.emosl(
                            Const.DATABASE_NO_REGION,
                            region_id
                        )
                    )

            if action == ImportAction.UPDATE:
                if w_item.get('resurrect_entity'):
                    if entity_id not in dead_ids:
                        self.append_error(entity_id,
                                          self.emosl(Const.DATABASE_NOT_FOUND, work_on, entity_name, action.name))
                else:
                    if entity_id not in alive_ids:
                        self.append_error(entity_id,
                                          self.emosl(Const.DATABASE_NOT_FOUND, work_on, entity_name, action.name))
            if action == ImportAction.DELETE:
                if entity_id not in all_alive_cloud_locations_ids:
                    self.append_error(entity_id,
                                      self.emosl(Const.DATABASE_NOT_FOUND, work_on, entity_name, action.name))
            elif action is ImportAction.CREATE:
                if entity_id in all_alive_cloud_locations_ids:
                    self.append_error(entity_id,
                                      self.emosl(Const.DATABASE_FOUND, work_on, entity_name, action.name))

        if self.errors:
            self.write_errors(error_type=EnumErrorType.ERROR.value)

            return False

        self.data = working_data
        self.write_history_and_publish()
        return True

    def generate_error_statistic_message(self, database_validation_error=None):
        validation_errors = len(self.errors) if not database_validation_error else database_validation_error
        statistics = {
            "inserted": 0,
            "updated": 0,
            "deleted": 0,
            "errors": validation_errors,
        }
        insert_msg = enum_message_on_specific_language(
            Const.DATABASE_IMPORT_STATS.value, 'en', statistics['inserted'],
            statistics['updated'], statistics['deleted'], statistics['errors']
        )

        ElasticCloudLoginFunctions.create_cloud_process_flow(
            self.elastic_hash, EnumErrorType.ERROR.name, json.dumps(insert_msg))

        ElasticCloudLoginFunctions.create_process_flow(
            self.elastic_hash, EnumErrorType.ERROR.name, json.dumps(insert_msg))

    def validate_machine_types(self):
        validator = MachineTypeValidator(self.company_id, self.data)
        warnings = []
        rows = []
        used_machine_type = {}

        try:
            rows, warnings, used_machine_type, validation_statistic_error = validator.validate()
        except DuplicateRowsError as e:
            for field, duplicated in e.duplicates.items():
                for id in duplicated:
                    self.append_error(id, self.emosl(Const.DATABASE_VALIDATION_ERROR,
                                                     "Repeated {}".format(field)))
        for w in warnings:
            self.append_warning('machine_type', self.emosl(*w))

        if self.warning:
            self.write_warnings(warning_type=EnumErrorType.WARNING.value)

        if not rows:
            self.append_error('machine_type', self.emosl(Const.NO_ROWS_TO_IMPORT))

        if self.errors:
            self.write_errors(error_type=EnumErrorType.ERROR.value)
            # business logic requires statistics in this case ...
            self.generate_error_statistic_message()
            return False

        importer = MachineTypeImporter(self.company_id)
        try:
            importer.populate(rows, used_machine_type)
            import_stats = importer.save()
            validation_database_errors = importer.validation_database_errors_count
            validation_database_errors_messages = importer.validation_database_errors_message
        except DbQueryError as e:
            self.append_error(e.import_name, self.emosl(e.msg_const, e.import_name))
        except InvalidImportData as e:
            for err in e.errors:
                self.append_error(err[0], self.emosl(*err[1:]))
        except psycopg2.DatabaseError:
            self.append_error('machine_type', self.emosl(Const.IMPORT_FAIL,
                                                         'machine_type'))
        if self.errors:
            self.write_errors(error_type=EnumErrorType.ERROR.value)
            return False

        # business logic requires validation database error statistics in this case ...
        if validation_database_errors_messages:
            for validation_message in validation_database_errors_messages:
                self.append_warning('machine_type', self.emosl(*validation_message))
            self.write_warnings(warning_type=EnumErrorType.WARNING.value)

        if import_stats or validation_statistic_error or validation_database_errors:
            total_error = import_stats["errors"] + validation_statistic_error + validation_database_errors
            import_stats["errors"] = total_error

        self.write_history()
        self.write_import_history(import_stats)

    def validate_region(self):
        """
        Makes region import request validation against cloud database. Data needed to be validated are inside
        class instance. Basic validation process is described in class description. Additional check is done
        for parent region existance if parent region is specified. Parent region is not mandatory field and
        in case of not finding parent region, only warning is written into Elastic process.
        :return: True - no errors found, False if any error
        """
        work_on = ImportType.REGIONS.value['capitalised_name']
        id_fld = work_on.lower() + '_id'
        name_fld = work_on.lower() + '_name'
        action_fld = work_on.lower() + '_action'

        working_data = copy.deepcopy(self.data)
        working_data, removed_ids, removed_names = remove_duplicated(work_on, working_data)

        # Do not accept files with repeated external ids and names
        if removed_ids or removed_names:
            self.write_duplicated_errors(work_on, removed_ids, removed_names)
            return False

        cloud_regions_all = RegionQueryOnCloud.get_regions_for_company(self.company_id)
        cloud_locations_all = LocationQueryOnCloud.get_region_id_and_location(self.company_id)
        all_alive_cloud_region_id_per_location = get_values_from_dict_arr(cloud_locations_all, 'region_id')

        if not cloud_regions_all['status']:
            self.append_error(work_on, self.emosl(Const.DATABASE_QUERY_ERROR, work_on))
            self.write_errors(error_type=EnumErrorType.ERROR.value)
            return False
        else:
            cloud_regions_all = cloud_regions_all['results']

        all_alive_cloud_regions = [m for m in cloud_regions_all if m['alive'] is True]
        all_alive_cloud_regions_ids = get_values_from_dict_arr(all_alive_cloud_regions, 'ext_id')

        location_and_region_id = [{'name': x["location_name"], 'region_id': x["region_id"]}
                                  for x in cloud_locations_all]

        external_ids = set([m[id_fld] for m in working_data])
        alive_ids, _ = get_alive_and_dead(external_ids, cloud_regions_all)

        try:
            action_50 = int(self.data[0][action_fld]) == 50
        except ValueError:
            action_50 = False

        not_delete_region_action50 = []

        if action_50:
            for item in all_alive_cloud_regions:
                if item['ext_id'] not in ["", None]:
                    filter_region = list(filter(lambda k: k['region_id'] == item['ext_id'], location_and_region_id))
                    r = filter_region[0] if filter_region else None

                    if item['ext_id'] in set(all_alive_cloud_region_id_per_location):
                        not_delete_region_action50.append(item)
                        self.append_warning(item['ext_id'], self.emosl(Const.ACTIVE_REGION_ON_LOCATION, work_on, r['name']))

            for m in [m for m in working_data if m[id_fld] in alive_ids]:
                m[action_fld] = ImportAction.UPDATE.value
            for m in [m for m in working_data if m[id_fld] not in alive_ids]:
                m[action_fld] = ImportAction.CREATE.value

            if not_delete_region_action50:
                for items in not_delete_region_action50:
                    all_alive_cloud_regions.remove(items)

            for m in [m for m in all_alive_cloud_regions if m['ext_id'] not in external_ids]:
                working_data = [{
                    id_fld: m['ext_id'],
                    name_fld: m['name'],
                    action_fld: ImportAction.DELETE.value,
                    'action50_delete': True,
                    'cloud_id': m['id'],
                    'parent_region_id': '',
                }] + working_data
            # refresh ID's
            external_ids = set([m[id_fld] for m in working_data])
            alive_ids, _ = get_alive_and_dead(external_ids, cloud_regions_all)

        not_deleted_region = []

        for w_item in working_data:
            entity_id = w_item[id_fld]
            entity_name = w_item[name_fld]
            parent_item_id =(
                w_item.get('parent_region_id')
                if w_item.get('parent_region_id') and w_item.get('parent_region_id') != '<null>'
                else None
            )
            try:
                action = ImportAction(int(w_item[action_fld]))
                action, w_item[action_fld] = self.get_upsert_if_needed(action, entity_id, alive_ids)
            except ValueError:
                self.append_error(w_item[id_fld], self.emosl(Const.DATABASE_WRONG_ACTION, work_on, w_item[action_fld]))
                continue

            if action == ImportAction.UPDATE:
                if entity_id not in alive_ids:
                    self.append_error(entity_id,
                                      self.emosl(Const.DATABASE_NOT_FOUND, work_on, entity_name, action.name))
            if action == ImportAction.DELETE:
                if entity_id not in all_alive_cloud_regions_ids:
                    self.append_error(entity_id,
                                      self.emosl(Const.DATABASE_NOT_FOUND, work_on, entity_name, action.name))
                elif entity_id in all_alive_cloud_region_id_per_location:
                    self.append_warning(entity_id,
                                      self.emosl(Const.ASSIGNED_REGION, work_on, entity_name))
                    not_deleted_region.append(w_item)
            elif action is ImportAction.CREATE:
                if entity_id in all_alive_cloud_regions_ids:
                    self.append_error(entity_id, self.emosl(Const.DATABASE_FOUND, work_on, entity_name, action.name))

            if action in [ImportAction.CREATE, ImportAction.UPDATE] and parent_item_id:
                if parent_item_id and parent_item_id not in all_alive_cloud_regions_ids:
                    msg = self.emosl(Const.DATABASE_NO_PARENT_REGION, parent_item_id)
                    CompanyProcessLogger.create_process_flow(
                        self.elastic_hash, json.dumps([{
                            'record': entity_id,
                            'message': msg
                        }]), EnumErrorType.WARNING.name)
                    logger_api.warning(msg)

        if not_deleted_region:
            for item in not_deleted_region:
                working_data.remove(item)
        if self.warning:
            self.write_warnings()

        if len(working_data) == 0:
            self.append_error(work_on, self.emosl(Const.ALL_ENTITIES_REMOVED))

        if self.errors:
            self.write_errors(error_type=EnumErrorType.ERROR.value)
            return False

        self.data = working_data
        self.write_history_and_publish()
        return True

    def validate_client(self):
        """
        Makes client import request validation against cloud database. Data needed to be validated are inside
        class instance. Basic validation process is described in class description. Additional check is done
        for parent client existence if parent client is specified. Parent client is not mandatory field and
        in case of not finding parent client, only warning is written into Elastic process.
        :return: True - no errors found, False if any error
        """
        work_on = ImportType.CLIENTS.value['capitalised_name']
        id_fld = work_on.lower() + '_id'
        name_fld = work_on.lower() + '_name'
        action_fld = work_on.lower() + '_action'

        working_data = copy.deepcopy(self.data)
        working_data, removed_ids, removed_names = remove_duplicated(work_on, working_data)

        # Do not accept files with repeated external ids and names
        if removed_ids or removed_names:
            self.write_duplicated_errors(work_on, removed_ids, removed_names)
            return False

        cloud_clients_all = ClientQueryOnCloud.get_clients_for_company(self.company_id)
        cloud_clients_types_alive_all = ClientTypeQueryOnCloud.get_alive_clients_types_for_company(self.company_id)

        client_with_active_machines = ClientQueryOnCloud.get_clients_with_active_machines_for_company(self.company_id)
        client_with_active_machines_ids = get_values_from_dict_arr(client_with_active_machines['results'], 'ext_id')

        if not cloud_clients_all['status']:
            self.append_error(work_on, self.emosl(Const.DATABASE_QUERY_ERROR, work_on))
            self.write_errors(error_type=EnumErrorType.ERROR.value)
            return False
        else:
            cloud_clients_all = cloud_clients_all['results']

        all_alive_cloud_clients = [m for m in cloud_clients_all if m['alive'] is True]
        all_alive_cloud_clients_ids = get_values_from_dict_arr(all_alive_cloud_clients, 'ext_id')
        cloud_clients_types_alive_all_ids = get_values_from_dict_arr(cloud_clients_types_alive_all['results'], 'ext_id')

        external_ids = set([m[id_fld] for m in working_data])
        alive_ids, dead_ids = get_alive_and_dead(external_ids, cloud_clients_all)

        try:
            action_50 = int(self.data[0][action_fld]) == 50
        except ValueError:
            action_50 = False

        not_delete_client_action50 = []

        if action_50:
            for item in all_alive_cloud_clients:
                if item['ext_id'] in set(client_with_active_machines_ids):
                    not_delete_client_action50.append(item)
                    self.append_warning(item['ext_id'], self.emosl(Const.ASSIGNED_MACHINE_CLIENT, item['ext_id']))

            for m in [m for m in working_data if m[id_fld] in alive_ids]:
                m[action_fld] = ImportAction.UPDATE.value
            for m in [m for m in working_data if m[id_fld] not in alive_ids]:
                m[action_fld] = ImportAction.CREATE.value

            if not_delete_client_action50:
                for items in not_delete_client_action50:
                    all_alive_cloud_clients.remove(items)

            for m in [m for m in all_alive_cloud_clients if m['ext_id'] not in external_ids]:
                working_data = [{
                    id_fld: m['ext_id'],
                    name_fld: m['name'],
                    action_fld: ImportAction.DELETE.value,
                    'action50_delete': True,
                    'cloud_id': m['id'],
                    'parent_region_id': '',
                }] + working_data
            # refresh ID's
            external_ids = set([m[id_fld] for m in working_data])
            alive_ids, dead_ids = get_alive_and_dead(external_ids, cloud_clients_all)


        not_deleted_client = []

        for w_item in working_data:
            entity_id = w_item[id_fld]
            entity_name = w_item[name_fld]
            parent_item_id = (
                w_item.get('parent_client_id')
                if w_item.get('parent_client_id') and w_item.get('parent_client_id') != '<null>'
                else None
            )
            client_type_id = (
                w_item.get('client_type_id')
                if w_item.get('client_type_id') and w_item.get('client_type_id') != '<null>'
                else None
            )
            try:
                action = ImportAction(int(w_item[action_fld]))
                action, w_item[action_fld] = self.get_upsert_if_needed(action, entity_id, alive_ids)
            except ValueError:
                self.append_error(w_item[id_fld], self.emosl(Const.DATABASE_WRONG_ACTION, work_on, w_item[action_fld]))
                continue

            if action == ImportAction.UPDATE:
                if entity_id not in alive_ids:
                    self.append_error(entity_id, self.emosl(Const.DATABASE_NOT_FOUND, work_on, entity_name, action.name))
            elif action == ImportAction.DELETE:
                if entity_id not in all_alive_cloud_clients_ids:
                    self.append_error(entity_id, self.emosl(Const.DATABASE_NOT_FOUND, work_on, entity_name, action.name))
                elif entity_id in client_with_active_machines_ids:
                    self.append_warning(entity_id,
                                        self.emosl(Const.ASSIGNED_MACHINE_CLIENT, entity_name))
                    not_deleted_client.append(w_item)
            elif action == ImportAction.CREATE:
                if entity_id in all_alive_cloud_clients_ids:
                    self.append_error(entity_id, self.emosl(Const.DATABASE_FOUND, work_on, entity_name, action.name))

            if action in [ImportAction.CREATE, ImportAction.UPDATE]:
                if parent_item_id and parent_item_id not in all_alive_cloud_clients_ids:
                    self.append_warning(entity_id, self.emosl(Const.DATABASE_NO_PARENT_CLIENT, parent_item_id))
                if client_type_id and client_type_id not in cloud_clients_types_alive_all_ids:
                    self.append_error(entity_id, self.emosl(Const.DATABASE_NO_CLIENT_TYPE, client_type_id))

        for item in not_deleted_client:
            working_data.remove(item)

        if self.warning:
            self.write_warnings()

        if len(working_data) == 0:
            self.append_error(work_on, self.emosl(Const.ALL_ENTITIES_REMOVED))

        if self.errors:
            self.write_errors(error_type=EnumErrorType.ERROR.value)
            return False
        self.data = working_data
        self.write_history_and_publish()
        return True

    def _group_by_product_id(self, packings_list, singlepacks_all, working_data):
        grouped_packings = {}
        used_product_ids = {}
        for item in working_data:
            used_product_ids[item['product_id']] = True

        for packing in packings_list:
            if packing['product_id'] not in used_product_ids:
                continue
            product_id = packing['product_id']
            if product_id not in grouped_packings.keys():
                grouped_packings[product_id] = PackingsForProduct(product_id, singlepacks_all[product_id])

            grouped_packings[product_id].add(packing)

        return grouped_packings

    def _prepare_state_after_import(self, grouped_packing_list, import_data, singlepacks_all):

        for import_item in import_data:
            action = ImportAction(int(import_item['packing_action']))
            product_id = import_item['product_id']
            if product_id not in grouped_packing_list:
                grouped_packing_list[product_id] = PackingsForProduct(product_id, singlepacks_all[product_id])

            if action in [ImportAction.CREATE, ImportAction.UPDATE]:
                grouped_packing_list[product_id].add(import_item)
            if action == ImportAction.DELETE:
                grouped_packing_list[product_id].delete(import_item['packing_id'])

        return grouped_packing_list

    def _check_packingsizes_constraints(self, grouped_packing_list):

        messages = []

        for packing_for_product in grouped_packing_list.values():
            name_messages = packing_for_product.names_correct(self.language)

            if not packing_for_product.default_correct():
                messages.append(self.emosl(Const.PACKING_INVALID_DEFAULTS, packing_for_product.product_id))
            if not packing_for_product.quantity_correct():
                messages.append(self.emosl(Const.PACKING_INVALID_QUANTITIES, packing_for_product.product_id))

            messages.extend(name_messages)

        return messages

    def validate_packing(self):
        work_on = ImportType.PACKINGS.value['capitalised_name']
        id_fld = work_on.lower() + '_id'
        name_fld = work_on.lower() + '_name_id'
        action_fld = work_on.lower() + '_action'

        working_data = copy.deepcopy(self.data)
        working_data, removed_ids, removed_names = remove_duplicated(work_on, working_data, False)

        try:
            action_50 = int(self.data[0][action_fld]) == 50
        except ValueError:
            action_50 = False

        # Do not accept files with repeated external ids and names
        if removed_ids or removed_names:
            self.write_duplicated_errors(work_on, removed_ids, removed_names)
            return False

        packing_sizes_query = PackingsQueryOnCloud.get_product_packings_for_company(self.company_id)
        products_all = ProductQueryOnCloud.get_products_for_company(self.company_id)
        products_all_alive = [m for m in products_all['results'] if m['alive'] is True]
        product_all_alive_ids = get_values_from_dict_arr(products_all_alive, 'ext_id')
        packing_names_all = PackingsQueryOnCloud.get_packing_names_for_company(self.company_id)

        existing_name_ids = get_values_from_dict_arr(packing_names_all['results'], 'ext_id')

        product_dict = {}

        for product in products_all_alive:
            p = {}

            ext_id = product['ext_id']
            if ext_id is None or ext_id == '':
                continue
            p['use_packing'] = product['use_packing']

            product_dict[ext_id] = p

        singlepack_id = None
        if not packing_sizes_query['status']:
            self.append_error(work_on, self.emosl(Const.DATABASE_QUERY_ERROR, work_on))
            self.write_errors(error_type=EnumErrorType.ERROR.value)
            return False
        else:
            singlepacks_all = {}
            singlepack_barcodes = {}
            packing_sizes_all = []
            packing_sizes_results = packing_sizes_query['results']
            for p in packing_sizes_results:
                if p['quantity'] > 1 and p['alive']:
                    packing_sizes_all.append(p)
                elif p['system_default'] and p['alive']: # we don't care about deleted singlepacks, no ressurection
                    singlepacks_all[p['ext_id']] = p
                    singlepack_barcodes[p['barcode']] = True
                    if singlepack_id is None:
                        singlepack_id = [m for m in packing_names_all['results'] if m['packing_name'] == p['packing_name']][0]['ext_id']

        all_alive_cloud_packing_sizes = [m for m in packing_sizes_all if m['alive'] is True and m['product_id'] != '']
        all_alive_cloud_packing_sizes_ids = get_values_from_dict_arr(all_alive_cloud_packing_sizes, 'ext_id')

        external_ids = set([m[id_fld] for m in working_data])
        alive_ids, dead_ids = get_alive_and_dead(external_ids, packing_sizes_all)

        external_id_products = {}
        for packing in packing_sizes_all:
            external_id_products[packing['ext_id']] = packing['product_id']


        if action_50:

            for m in [m for m in working_data if m[id_fld] in alive_ids]:
                m[action_fld] = ImportAction.UPDATE.value
            for m in [m for m in working_data if m[id_fld] not in alive_ids]:
                m[action_fld] = ImportAction.CREATE.value

            for m in [m for m in all_alive_cloud_packing_sizes if m['ext_id'] not in external_ids]:
                working_data = [{
                    id_fld: m['ext_id'],
                    name_fld: m['packing_name_id'],
                    action_fld: ImportAction.DELETE.value,
                    'action50_delete': True,
                    'cloud_id': m['id'],
                    'product_id': m['product_id']
                }] + working_data
            # refresh ID's
            external_ids = set([m[id_fld] for m in working_data])
            alive_ids, dead_ids = get_alive_and_dead(external_ids, packing_sizes_all)

        barcodes_from_file = []
        removed_packings_row = []

        for w_item in working_data:
            entity_id = w_item[id_fld]
            entity_name = w_item[name_fld]

            if w_item['packing_name_id'] == singlepack_id:
                removed_packings_row.append(w_item)
                self.append_warning(entity_id,
                                    self.emosl(Const.PACKING_SINGLEPACK_SKIPPED, w_item['product_id']))
                continue

            if w_item['packing_id'] in singlepacks_all:
                self.append_error(entity_id, self.emosl(Const.PACKING_REPEATED_EXTERNAL_ID, w_item['packing_id']))
                self.write_errors(error_type=EnumErrorType.ERROR.value)
                return False

            if w_item['packing_name_id'] not in existing_name_ids:
                removed_packings_row.append(w_item)
                self.append_warning(entity_id,
                                    self.emosl(Const.PACKING_INVALID_NAME,  w_item['product_id'], w_item['packing_name_id']))
                continue

            barcode = w_item['barcode'] if 'barcode' in w_item else None

            if barcode != '':
                if barcode in singlepack_barcodes:
                    self.append_error(entity_id, self.emosl(Const.FIELD_NOT_UNIQUE, 'barcode', barcode))
                    self.write_errors(error_type=EnumErrorType.ERROR.value)
                    return False

                barcodes_from_file.append([
                    w_item['packing_id'], barcode, w_item['packing_action']
                ])

            w_item['ext_id'] = w_item['packing_id']
            try:
                action = ImportAction(int(w_item[action_fld]))
                if action == ImportAction.CREATE:

                    if w_item['ext_id'] in external_id_products and w_item['product_id'] != external_id_products[w_item['ext_id']]:
                        self.append_error(entity_id,
                                            self.emosl(Const.PACKING_ANOTHER_PRODUCT, entity_id, external_id_products[w_item['ext_id']]))
                        self.write_errors(error_type=EnumErrorType.ERROR.value)
                        return False

                action, w_item[action_fld] = self.get_upsert_if_needed(action, entity_id, alive_ids)
            except ValueError:
                self.append_error(w_item[id_fld], self.emosl(Const.DATABASE_WRONG_ACTION, work_on, w_item[action_fld]))
                continue

            if action in [ImportAction.UPDATE, ImportAction.CREATE]:
                if w_item['product_id'] not in product_all_alive_ids:
                    self.append_warning(entity_id,
                                      self.emosl(Const.PACKING_INVALID_PRODUCT, entity_id, w_item['product_id']))
                    removed_packings_row.append(w_item)
                    continue
                if not product_dict[w_item['product_id']]['use_packing']:
                    self.append_warning(entity_id,
                                        self.emosl(Const.PACKING_PRODUCT_NO_USE_PACKING, entity_id, w_item['product_id']))
                    removed_packings_row.append(w_item)
                    continue

            if action == ImportAction.UPDATE:
                if entity_id not in alive_ids:
                    self.append_error(entity_id,
                                      self.emosl(Const.DATABASE_NOT_FOUND, work_on, entity_name, action.name))
            elif action == ImportAction.DELETE:
                if entity_id not in all_alive_cloud_packing_sizes_ids:
                    self.append_error(entity_id,
                                      self.emosl(Const.DATABASE_NOT_FOUND, work_on, entity_name, action.name))
            elif action == ImportAction.CREATE:
                if entity_id in all_alive_cloud_packing_sizes_ids:
                    self.append_error(entity_id, self.emosl(Const.DATABASE_FOUND, work_on, entity_name, action.name))

        self.handle_field_uniqueness(
            'barcode',
            barcodes_from_file,
            all_alive_cloud_packing_sizes,
            barcode_validation=True
        )

        for item in removed_packings_row:
            if item in working_data:
                working_data.remove(item)

        grouped_packings = {}

        if not action_50:
            grouped_packings = self._group_by_product_id(all_alive_cloud_packing_sizes, singlepacks_all, working_data)

        grouped_packings = self._prepare_state_after_import(grouped_packings, working_data, singlepacks_all)
        validation_messages = self._check_packingsizes_constraints(grouped_packings)

        for packing in grouped_packings.values():
            if packing.singlepack_overriden_value is not None:
                working_data = [{
                    id_fld:  packing.singlepack['ext_id'],
                    name_fld: singlepack_id,
                    action_fld: ImportAction.UPDATE.value,
                    'singlepack_override': True,
                    'cloud_id': packing.singlepack['id'],
                    'product_id': packing.singlepack['product_id'],
                    'quantity': 1,
                    'default': packing.singlepack_overriden_value,
                    'barcode': packing.singlepack['barcode'],

                }] + working_data

        if len(validation_messages) > 0:
            self.append_error(work_on, self.emosl(Const.PACKING_INVALID_DATA, validation_messages))

        if self.warning:
            self.write_warnings()

        if len(working_data) == 0:
            self.append_error(work_on, self.emosl(Const.ALL_ENTITIES_REMOVED))

        if self.errors:
            self.write_errors(error_type=EnumErrorType.ERROR.value)
            return False

        self.data = working_data
        self.write_history_and_publish()

    def validate_product(self):
        work_on = ImportType.PRODUCTS.value['capitalised_name']
        id_fld = work_on.lower() + '_id'
        name_fld = work_on.lower() + '_name'
        action_fld = work_on.lower() + '_action'

        working_data = copy.deepcopy(self.data)
        working_data, removed_ids, removed_names = remove_duplicated(
            work_on, working_data
        )
        all_company_product_rotation_groups = ProductRotationGroupQueryOnCloud.get_product_rotation_groups_for_company(
            self.company_id
        )
        all_alive_company_product_rotation_groups = all_company_product_rotation_groups.get('results')

        rotation_group_assigned_products = []

        # get products for specific product rotation group
        for prg in all_alive_company_product_rotation_groups:
            prg_products = ProductRotationGroupQueryOnCloud.get_products_for_specific_product_rotation_group(
                prg_id=prg['id']
            )
            prg_products = prg_products.get('results')
            if prg_products:
                rotation_group_assigned_products.append(prg_products)

        # don't accept files with repeated external ids and repeated names
        if removed_ids or removed_names:
            self.write_duplicated_errors(work_on, removed_ids, removed_names)
            return False

        # if found non unique tax rates for company, stop process
        duplicates_found = tax_rate_duplicates(self.company_id)
        if duplicates_found:
            calculate_message = [str(x) for x in duplicates_found]
            self.append_error(
                ', '.join(calculate_message),
                self.emosl(
                    Const.TAX_RATE_DUPLICATE_FOUND,
                    ', '.join(calculate_message)
                )
            )
            self.write_errors(error_type=EnumErrorType.ERROR.value)
            return False

        # query db for products
        cloud_products_all = ProductQueryOnCloud.get_products_for_company(self.company_id)
        if not cloud_products_all['status']:
            self.append_error(
                work_on, self.emosl(Const.DATABASES_QUERY_ERROR, work_on)
            )
            self.write_errors(error_type=EnumErrorType.ERROR.value)
            return False
        else:
            cloud_products_all = cloud_products_all['results']

        # alive products
        all_alive_cloud_products = [
            p for p in cloud_products_all if p['alive'] is True
        ]
        all_alive_cloud_products_ids = get_values_from_dict_arr(
            all_alive_cloud_products, 'ext_id'
        )

        external_ids = set([p[id_fld] for p in working_data])
        alive_ids, _ = get_alive_and_dead(external_ids, cloud_products_all)

        try:
            action_50 = int(self.data[0][action_fld]) == 50
        except ValueError:
            action_50 = False

        if action_50:
            # if product exists and is alive
            for p in [p for p in working_data if p[id_fld] in alive_ids]:
                p[action_fld] = ImportAction.UPDATE.value
            # if product exists, but is not alive
            for p in [p for p in working_data if p[id_fld] not in alive_ids]:
                p[action_fld] = ImportAction.CREATE.value

            # all alive products whose external id is not in csv add DELETE
            # action in a new row in data. add them another field 'action50_delete'
            # and 'cloud_id'
            for p in [p for p in all_alive_cloud_products if p['ext_id'] not in external_ids]:
                working_data = [{
                    id_fld: p['ext_id'],
                    name_fld: p['name'],
                    action_fld: ImportAction.DELETE.value,
                    'action50_delete': True,
                    'cloud_id': p['id']
                }] + working_data
            # refresh ids
            external_ids = set([p[id_fld] for p in working_data])
            alive_ids, _ = get_alive_and_dead(external_ids, cloud_products_all)

        # validate specific non-required fields
        tax_rates_from_file = []
        barcodes_from_file = []
        product_names_from_file = []
        products_for_deletion = []

        non_singlepacks = PackingsQueryOnCloud.get_used_non_singlepack_packings(self.company_id)

        if non_singlepacks['status']:
            packing_barcodes = [x['barcode'] for x in non_singlepacks['results']]
            packing_ext_id = [x['ext_id'] for x in non_singlepacks['results']]
        else:
            packing_barcodes = []
            packing_ext_id = []

        discard_import_rows = []
        for row in working_data:
            if int(row['product_action']) in [ImportAction.UPDATE.value, ImportAction.CREATE.value]:
                if row['default_barcode'] not in ['', '<null>'] and row['default_barcode'] in packing_barcodes:
                    self.append_error(
                        row['product_id'],
                        self.emosl(
                            Const.PACKING_REPEATED_BARCODE_PRODUCT,
                            row['default_barcode'])
                        )

                if row['product_id'] in packing_ext_id:
                    self.append_error(
                        row['product_id'],
                        self.emosl(
                            Const.PACKING_REPEATED_EXTERNAL_ID_PRODUCT,
                            row['product_id'])
                        )

            # SET RIGHT ACTION
            action = ImportAction(int(row[action_fld]))
            action, row['product_action'] = self.get_upsert_if_needed(action, row['product_id'],
                                                                      all_alive_cloud_products_ids)

            # product assigned on product rotation cannot be deleted
            if action in [ImportAction.DELETE, ImportAction.UNKNOWN]:
                for prg_products in rotation_group_assigned_products:
                    prg_product = next(filter(lambda obj: obj.get('product_ext_id') == row['product_id'] or obj.get('product_code') == row['product_id'], prg_products), None)
                    if prg_product:
                        prg_product_ext_id = prg_product['product_ext_id']
                        prg_product_name = prg_product['product_name']
                        prg_name = prg_product['prg_name']
                        prg_ext_id = prg_product['prg_ext_id']

                        self.append_warning(
                            row['product_id'],
                            self.emosl(
                                Const.PRODUCT_ROTATION_GROUP_STOP_DELETE,
                                prg_product_name, prg_product_ext_id, prg_name, prg_ext_id, action.value)
                        )
                        discard_import_rows.append(row)

            tax_rate = row['tax_rate'] if 'tax_rate' in row else None
            barcode = row['default_barcode'] if 'default_barcode' in row else None
            barcode1 = row['barcode1'] if 'barcode1' in row else None
            barcode2 = row['barcode2'] if 'barcode2' in row else None
            barcode3 = row['barcode3'] if 'barcode3' in row else None
            barcode4 = row['barcode4'] if 'barcode4' in row else None
            product_name = row['product_name']

            tax_rates_from_file.append(tax_rate)

            validation_barcodes = [barcode, barcode1, barcode2, barcode3, barcode4]

            for validation_barcode in validation_barcodes:

                barcodes_from_file.append([
                    row['product_id'], validation_barcode, row['product_action']
                ])

            product_names_from_file.append([
                row['product_id'], product_name, row['product_action']
            ])

            if row['product_action'] == '2':  # delete action
                products_for_deletion.append(row['product_id'])  # external ids

            # validate barcodes
            db_barcode_for_validate = ['barcode', 'barcode1', 'barcode2', 'barcode3', 'barcode4']

            for barcode_name in db_barcode_for_validate:
                self.handle_field_uniqueness(
                    barcode_name,
                    barcodes_from_file,
                    all_alive_cloud_products,
                    product_ext_id=row['product_id'],
                    barcode_validation=True
                )
            db_current_product = list(filter(lambda d: d['ext_id'] == row['product_id'], all_alive_cloud_products))
            current_db_default_barcode = None
            current_db_barcode1 = None
            current_db_barcode2 = None
            current_db_barcode3 = None
            current_db_barcode4 = None

            if db_current_product:
                db_current_product = db_current_product[0]
                current_db_default_barcode = db_current_product['barcode']
                current_db_barcode1 = db_current_product['barcode1']
                current_db_barcode2 = db_current_product['barcode2']
                current_db_barcode3 = db_current_product['barcode3']
                current_db_barcode4 = db_current_product['barcode4']

            if current_db_default_barcode and not barcode:
                if barcode1 or barcode2 or barcode3 or barcode4:
                    self.append_error(
                        row['product_id'],
                        self.emosl(
                            Const.DELETE_DEFAULT_BARCODE_WHEN_ADDITIONAL_BARCODE_EXIST_ERROR,
                            row['product_id'])
                    )

            elif not current_db_default_barcode and not barcode:
                if barcode1 or barcode2 or barcode3 or barcode4:
                    self.append_error(
                        row['product_id'],
                        self.emosl(
                            Const.STOP_CREATE_ADDITIONAL_BARCODE,
                            row['product_id'])
                    )
                if current_db_barcode1 or current_db_barcode2 or current_db_barcode3 or current_db_barcode4:
                    self.append_error(
                        row['product_id'],
                        self.emosl(
                            Const.STOP_CREATE_ADDITIONAL_BARCODE,
                            row['product_id'])
                    )

        # get machines and planograms if products connected to them
        products_machine_status = ProductQueryOnCloud.products_machine_status(
            self.company_id,
            products_for_deletion
        )
        products_planogram_status = ProductQueryOnCloud.products_planogram_status(
            self.company_id,
            products_for_deletion
        )

        if not products_machine_status['allowed']:
            results = products_machine_status['results']

            for key, value in results.items():
                p_ext_id = key

                self.append_error(
                    p_ext_id,
                    self.emosl(
                    Const.PRODUCT_DELETE_ERROR_MACHINES,
                    p_ext_id,
                    len(value),
                    ', '.join(value)
                    )
                )

        if not products_planogram_status['allowed']:
            results = products_planogram_status['results']

            for key, value in results.items():
                p_ext_id = key

                self.append_error(
                    p_ext_id,
                    self.emosl(
                    Const.PRODUCT_DELETE_ERROR_PLANOGRAMS,
                    p_ext_id,
                    ', '.join(value)
                    )
                )

        # if tax_rate provided, but doesn't exists for this company,
        # terminate process
        tax_rates_not_in_db = tax_rate_doesnt_exist(
            self.company_id, tax_rates_from_file
        )
        if tax_rates_not_in_db:
            self.append_error(
                ', '.join(tax_rates_not_in_db),
                self.emosl(
                    Const.TAX_RATE_NOT_EXISTS,
                    ', '.join(tax_rates_not_in_db)
                )
            )
            self.write_errors(error_type=EnumErrorType.ERROR.value)
            return False

        # validate product_name
        self.handle_field_uniqueness(
            'name',
            product_names_from_file,
            all_alive_cloud_products
        )
        if self.warning:
            self.write_warnings(warning_type=EnumErrorType.WARNING.value)

        if self.errors:
            self.write_errors(error_type=EnumErrorType.ERROR.value)
            return False

        for w_item in working_data:
            entity_id = w_item[id_fld]
            entity_name = w_item[name_fld]
            try:
                action = ImportAction(int(w_item[action_fld]))
            except ValueError:
                self.append_error(
                    w_item[id_fld],
                    self.emosl(
                        Const.DATABASE_WRONG_ACTION,
                        work_on,
                        w_item[action_fld]
                    )
                )
                continue

            if action == ImportAction.UPDATE:
                if entity_id not in alive_ids:
                    self.append_error(
                        entity_id,
                        self.emosl(
                            Const.DATABASE_NOT_FOUND,
                            work_on,
                            entity_name,action.name
                        )
                    )

            elif action == ImportAction.DELETE:
                if entity_id not in all_alive_cloud_products_ids:
                    self.append_error(
                        entity_id,
                        self.emosl(
                            Const.DATABASE_NOT_FOUND,
                            work_on,
                            entity_name,
                            action.name
                        )
                    )

            elif action == ImportAction.CREATE:
                if entity_id in all_alive_cloud_products_ids:
                    self.append_error(
                        entity_id,
                        self.emosl(
                            Const.DATABASE_FOUND,
                            work_on,
                            entity_name,
                            action.name
                        )
                    )

        if self.errors:
            self.write_errors(error_type=EnumErrorType.ERROR.value)
            return False

        for item in discard_import_rows:
            if item in working_data:
                working_data.remove(item)
        self.data = working_data
        self.write_history_and_publish()
        return True

    def validate_planogram(self):
        working_data = copy.deepcopy(self.data)

        planogram_validator = PlanogramValidator(
            company_id=self.company_id,
            working_data=working_data,
            language=self.language)
        self.errors, self.warning, import_stats, database_validation_errors_count = planogram_validator.validate()

        # business logic requires validation database error statistics in this case ...
        if import_stats["errors"] or database_validation_errors_count:
            total_error = import_stats["errors"] + database_validation_errors_count
            import_stats["errors"] = total_error

        if self.warning:
            self.write_warnings(warning_type=EnumErrorType.WARNING.value)
        if self.errors:
            self.write_errors(error_type=EnumErrorType.ERROR.value)
            if import_stats:
                self.generate_error_statistic_message(import_stats["errors"])
            return False

        self.write_history()

        self.write_import_history(import_stats)
        return True

    def validate_user(self):
        validator = UserValidator(self.company_id, self.data)
        try:
            data, errors = validator.validate()
            for e in errors:
                self.append_error(e[0], self.emosl(*e[1:]))
        except DuplicateRowsError as e:
            for field, duplicated in e.duplicates.items():
                for id in duplicated:
                    self.append_error(id, self.emosl(Const.DATABASE_VALIDATION_ERROR,
                                                     "Repeated {}".format(field)))
        except DbQueryError as e:
            self.append_error(e.import_name, self.emosl(e.msg_const, e.import_name))

        if self.errors:
            self.write_errors(error_type=EnumErrorType.ERROR.value)
            return False

        self.data = data
        self.write_history_and_publish()


class PackingsForProduct(object):

    product_id = None
    singlepack_overriden_value = None

    def __init__(self, product_id, singlepack):
        self.product_id = str(product_id)
        self.singlepack = singlepack
        self.packings = {}

    def __repr__(self):

        repr = "Product id:" + str(self.product_id) + "\n"
        for packing in self.packings.values():
            repr += packing['ext_id']+"|"+packing['packing_name_id']+"|"+str(packing['quantity'])+"|"+str(packing['default']) + "\n"
        return  repr

    def add(self, packing):
        product_id = str(packing['product_id'])
        if self.product_id != product_id:
            raise Exception("Wrong product_id => {}".format(product_id))

        key = packing['ext_id'] if packing['ext_id']!='' else packing['id']

        self.packings[key] = packing

    def delete(self, packing_id):

        self.packings.pop(packing_id, None)

    def default_correct(self):
        if len(self.packings) == 0:
            if not self.singlepack['default']:
                self.singlepack_overriden_value = True
            return True

        default_count = 0
        for packing in self.packings.values():
            if int(packing['default']) == 1:
                default_count += 1

        if self.singlepack['default']:
            if default_count == 0: #singlepack stays default
                return True
            if default_count == 1:  # singlepack needs to be made non default
                self.singlepack_overriden_value = False
                return True
        else:
            if default_count == 0:
                self.singlepack_overriden_value = True
                return True
            if default_count == 1:
                return True

        return False

    def quantity_correct(self):
        if len(self.packings) == 0:
            return True

        used_quantites = {}
        used_quantites['1'] = True #do not allow to override singlepack

        for packing in self.packings.values():
            if str(packing['quantity']) in used_quantites or int(packing['quantity']) < 1:
                return False
            used_quantites[str(packing['quantity'])] = True

        return True

    def names_correct(self, language):
        messages = []
        if len(self.packings) == 0:
            return messages

        used_names = {}

        for packing in self.packings.values():
            if packing['packing_name_id'] == '':
                continue #ignore cloud products without external id

            if packing['packing_name_id'] in used_names:
                messages.append(enum_message_on_specific_language(Const.PACKING_REPEATED_NAME.value, language, packing['product_id'], packing['packing_name_id']))
            else:
                used_names[packing['packing_name_id']] = True

        return messages
