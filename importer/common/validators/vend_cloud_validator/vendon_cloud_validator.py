from common.rabbit_mq.database_interaction_q.vend_db_publisher import vend_publish_to_database
from common.mixin.enum_errors import EnumErrorType, EnumValidationMessage
from database.cloud_database.core.query import MachineQueryOnCloud, VendQueryOnCloud
from common.mixin.enum_errors import enum_message_on_specific_language as emosl
from common.mixin.vends_mixin import (MainVendProcessLogger)
from database.company_database.core.query_history import CompanyFailHistory
from common.email.send_email import send_email_on_import_error
from common.logging.setup import vend_logger


class VendonCloudValidator(object):
    """
    This is main class for cloud validation, in this part we make some basic validation on cloud:
        - check if machine with given external id exists in cloud
        - check if transaction is unique
    All vends that pass checks are passed on for cloud insertion

    """

    def __init__(self, data):
        self.data = data
        self.main_content = self.data['data']
        self.elastic_hash = self.data.get('elastic_hash')
        self.email = self.data.get('email')
        self.request_type = self.data.get('type')
        self.token = self.data.get('token')
        self.company_id = self.data.get('company_id')
        self.import_type = self.data.get('import_type')
        self.language = self.data.get('language')
        self.skipped_vends = self.data.get('skipped_vends')
        self.import_file_path = ""
        self.general_process_logger = MainVendProcessLogger(
            company_id=self.company_id, import_type=self.import_type, process_request_type=self.request_type,
            token=self.token
        )

    def validate(self):
        machine_external_ids = list(set([x['machine_ext_id'] for x in self.main_content]))
        vend_logger.info("Validating machines for {}".format(self.elastic_hash))

        machines = MachineQueryOnCloud.get_machines_by_external_ids(self.company_id, machine_external_ids, with_devices=True)

        if machines['status']:
            machine_ids_in_cloud_vendon = list(set([x['ext_id'] for x in machines['results'] if x['device_type'] == 'VO' and x['device_active'] is True]))
            machine_ids_in_cloud_nonvendon = list(set([x['ext_id'] for x in machines['results'] if x['device_type'] is None or (x['device_type'] != 'VO' and x['device_active'] is True)]))

        else:
            self.general_process_logger.update_general_process_flow(
                machine_external_ids,
                status=EnumErrorType.WARNING.name,
                key_enum=EnumValidationMessage.VENDON_CLOUD_SKIPPED.value,
                elastic_hash=self.elastic_hash
            )
            self._fail_job(EnumValidationMessage.VENDON_CLOUD_ALL_SKIPPED, "")
            return

        transformation_errors_skippable = []
        valid_vends = []

        vend_logger.info("Processing cloud results for {}".format(self.elastic_hash))

        for vend in self.main_content:
            if vend['machine_ext_id'] not in machine_ids_in_cloud_vendon:
                ext_id = vend['machine_ext_id'] if vend['machine_ext_id'] is not None else ''
                vendon_id = vend['machine_vendon_id'] if 'machine_vendon_id' in vend and vend['machine_ext_id'] is not None else 'None'
                if vend['machine_ext_id'] in machine_ids_in_cloud_nonvendon:
                    transformation_errors_skippable.append(emosl(EnumValidationMessage.VENDON_MACHINE_NOT_VENDON_DEVICE.value, self.language, vendon_id, ext_id))
                    continue
                transformation_errors_skippable.append(emosl(EnumValidationMessage.VENDON_MACHINE_NOT_FOUND_CLOUD.value, self.language, vend.get('machine_vendon_id', ''), ext_id))
                continue

            valid_vends.append(vend)

        self.skipped_vends += len(transformation_errors_skippable)
        transformation_errors_skippable = list(set(transformation_errors_skippable))

        if len(transformation_errors_skippable):
            self.general_process_logger.update_general_process_flow(
                transformation_errors_skippable,
                status=EnumErrorType.WARNING.name,
                key_enum=EnumValidationMessage.VENDON_CLOUD_SKIPPED.value,
                elastic_hash=self.elastic_hash
            )

        if len(valid_vends) == 0:
            self._fail_job(EnumValidationMessage.VENDON_CLOUD_ALL_SKIPPED, "")
            return

        is_partial = len(self.main_content) > len(valid_vends)

        self._publish_to_cloud(valid_vends, is_partial)
        vend_logger.info("Published to cloud queue {}".format(self.elastic_hash))
        return

    def _publish_to_cloud(self, vends, is_partial):

        publish_vend_message = {
            'vend_data': vends,
            'email': self.email,
            'token': self.token,
            'reimport': False,
            'company': self.company_id,
            'import_type': self.import_type,
            'elastic_hash': self.elastic_hash,
            'type_of_process': self.request_type,
            'skipped_vends': self.skipped_vends
        }

        self.general_process_logger.update_general_process_flow(
            status=EnumErrorType.IN_PROGRESS.name,
            key_enum=EnumValidationMessage.VENDON_DATA_PUBLISHED_TO_CLOUD.value,
            elastic_hash=self.elastic_hash
        )
        
        self.general_process_logger.update_system_log_flow(
            self.company_id, self.import_type,
            key_enum=EnumValidationMessage.VENDON_DATA_PUBLISHED_TO_CLOUD.value,
            logs_level=EnumErrorType.IN_PROGRESS.name
        )

        vend_publish_to_database(publish_vend_message)

        return

    def _fail_job(self, key, e):
        self.general_process_logger.update_general_process_flow(
            e,
            status=EnumErrorType.ERROR.name,
            key_enum=key.value,
            elastic_hash=self.elastic_hash
        )

        CompanyFailHistory.insert_vend_fail_history(
            company_id=self.company_id,
            import_type=self.import_type,
            elastic_hash=self.elastic_hash,
            data_hash=None,
            file_path='',
            import_error_type=EnumErrorType.ERROR.value,
            token=self.token,
            main_elastic_hash=None
        )

        if self.email:
                import threading

                threading.Thread(
                    target=send_email_on_import_error, args=(self.email, self.import_type, key.value['en'].format(e), self.company_id), daemon=True
                ).start()

        return