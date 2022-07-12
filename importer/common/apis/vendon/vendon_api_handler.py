import sys

import requests
import time
import copy
from datetime import datetime

from common.logging.setup import logger

from common.mixin.vends_mixin import (MainVendProcessLogger, create_elastic_hash)
from database.cloud_database.core.query import CustomUserQueryOnCloud
from database.company_database.core.company_parameters import CompanyParameters
from common.email.send_email import send_email_on_import_error

from common.mixin.enum_errors import EnumErrorType, EnumValidationMessage

from kombu import Exchange, Producer, Queue
from common.rabbit_mq.common.const import ValidationEnum
from common.rabbit_mq.connection.connection import conn
from database.company_database.core.query_history import CompanyFailHistory


vend_validator_exchange = Exchange("{}".format(ValidationEnum.VEND_VALIDATION_API_Q.value), type="direct")
vend_producer = Producer(
    exchange=vend_validator_exchange,
    channel=conn,
    routing_key="{}".format(ValidationEnum.VEND_VALIDATION_API_Q_KEY.value)
)

vend_queue = Queue(
    name="{}".format(ValidationEnum.VEND_VALIDATION_API_Q.value),
    exchange=vend_validator_exchange,
    routing_key="{}".format(ValidationEnum.VEND_VALIDATION_API_Q_KEY.value)
)

vend_queue.maybe_bind(conn)
vend_queue.declare()


class VendonApiException(Exception):
    pass


class UnathorizedUserException(Exception):
    pass


class VendonApiHandler(object):

    def __init__(self, company_id, endpoint):
        self.company_id = company_id

        self.url = CompanyParameters.get_parameter(self.company_id, 'vendon_base_url') + "/" + str.lstrip(endpoint)
        self.api_key = CompanyParameters.get_parameter(self.company_id,'vendon_api_key')

    def fetch_data(self, query_params=None):

        result_set = []
        fetch_complete = False
        records_fetched = 0
        offset = 0
        headers = {'Authorization': 'Token ' + self.api_key}

        while fetch_complete is False:

            try:
                response = requests.get(self.url, params=query_params, headers=headers, timeout=60)
            except Exception as e:
                raise VendonApiException("Network error:" + str(e))

            if response.status_code != 200:
                raise VendonApiException("Vendon API communication error:" + response.text)

            results = response.json()

            result_set.extend(results["result"])

            paging_info = results['paging']
            total_records = paging_info['total']
            limit = paging_info['limit']

            if total_records - offset > limit:
                offset += limit
                query_params["offset"] = offset
                records_fetched += offset
            else:
                fetch_complete = True
                records_fetched += total_records - offset

        return result_set


class VendonVendsFetcher(VendonApiHandler):

    def __init__(self, company_id):
        super().__init__(company_id=company_id, endpoint='stats/vends')
        self.interval_seconds = int(CompanyParameters.get_parameter(self.company_id, 'vendon_interval_minutes_vends'))*60
        self.delay = int(CompanyParameters.get_parameter(self.company_id, 'vendon_delay_minutes_vends', 15))*60


class VendonMachinesFetcher(VendonApiHandler):

    def __init__(self, company_id):
        super().__init__(company_id=company_id, endpoint='machine')


class VendonApiJob:
    def __init__(self, company_id, user_id):
        self.company_id = company_id
        self.elastic_hash = None
        self.import_type = None
        self.request_type = None
        self.general_process_logger = None
        self.user_id = user_id
        user_response = CustomUserQueryOnCloud.get_auth_token_from_user_id(user_id)
        if user_response['status'] == False:
            raise UnathorizedUserException("Unauthorized user id:"+str(user_id))

        self.token = user_response['token']
        self.language = 'en'
        self.email = CompanyParameters.get_parameter(self.company_id, 'vendon_vends_notification_mails')
        self.error_message = ""


    def generate_elastic_process(self):
        """
        :return: created main elastic process
        """
        self.elastic_hash = create_elastic_hash(company_id=self.company_id, import_type=self.import_type,
                                                import_request_type=self.request_type)
        if not self.elastic_hash:
            return False

        self.general_process_logger = MainVendProcessLogger(
            company_id=self.company_id, import_type=self.import_type, process_request_type=self.request_type,
            token=self.token
        )

        return True

    def _fail_job(self, key, e):
        self.error_message = self.general_process_logger.update_general_process_flow(
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


class VendonApiTransformException(Exception):
    pass


class VendonApiFatalTransformException(Exception):
    pass


class VendonVendsApiJob(VendonApiJob):

    def __init__(self, company_id, user_id):
        super().__init__(company_id, user_id)
        self.import_type = 'VENDON_VENDS'
        self.request_type = 'VENDON_API'
        self.vend_field_mapping = {
            "transaction_id": [{"name": "transaction_id"}],
            "price": [{"name": "value"}],
            "quantity": [{"name": "quantity"}],
            "selection": [{"name": "column_index"}],
            "datetime": [{"name": "timestamp", "func": "_to_timestamp"}],
            "machine_id": [{"name": "machine_ext_id", "func": "_get_machine_ext_id"}, {"name": "machine_vendon_id"}],
            "payment_method": [{"name": "payment_type", "func": "_get_payment_type"}]
        }
        self.machines = {}
        self.skipped_vends = 0
        self.vends_fetcher = VendonVendsFetcher(self.company_id)

    def get_scheduled_timestamps(self):
        end_time = int(time.time()) - self.vends_fetcher.delay
        last_timestamp = CompanyParameters.get_parameter(self.company_id, "vendon_last_fetched_timestamp_vends")

        if last_timestamp is None:
            start_time = end_time - self.vends_fetcher.interval_seconds
        else:
            start_time = int(last_timestamp) + 1
            max_interval = min(86400, 10 * self.vends_fetcher.interval_seconds)
            if (end_time - start_time) > max_interval:
                # API rate control
                end_time = start_time + max_interval
        return start_time, end_time

    def fetch_vends(self, start_time, end_time, machine_id=None, cli_output=False):
        hash_success = self.generate_elastic_process()

        if not hash_success:
            return False

        try:
            self.transformation_errors.clear()

            # Get fresh machines set first
            self._fetch_machines()

            query_params = {"from_timestamp": start_time, "to_timestamp": end_time, "search_time": "registered"}

            if machine_id is not None:

                query_params['machine_id'] = self._get_machine_vendon_id(machine_id)

            self.general_process_logger.update_general_process_flow(
                self.vends_fetcher.url, query_params,
                status=EnumErrorType.IN_PROGRESS.name,
                key_enum=EnumValidationMessage.VENDON_VENDS_START_REQUEST.value,
                elastic_hash=self.elastic_hash
            )

            vends = self.vends_fetcher.fetch_data(query_params)

            self.general_process_logger.update_general_process_flow(
                len(vends),
                status=EnumErrorType.IN_PROGRESS.name,
                key_enum=EnumValidationMessage.VENDON_VENDS_END_REQUEST.value,
                elastic_hash=self.elastic_hash
            )

            try:
                transformed_vends = self._transform_vends(vends)
            except VendonApiFatalTransformException as e:
                self._fail_job(EnumValidationMessage.VENDON_TRANSFORM_ERROR_FATAL, e)
                return False

            self._send_data_for_queue(transformed_vends)

            if cli_output:
                print("Vends fetched -> {}".format(len(transformed_vends)))

        except VendonApiException as e:
            self._fail_job(EnumValidationMessage.VENDON_API_ERROR, e)
            return False
        except Exception as e:

            self.general_process_logger.update_system_log_flow(e,
                                                               key_enum=EnumValidationMessage.VENDON_UNKNOWN_ERROR_DETAILED.value,
                                                               logs_level=EnumErrorType.ERROR.name)
            self._fail_job(EnumValidationMessage.VENDON_UNKNOWN_ERROR, "")
            return False

        return True

    transformation_errors = {}

    def _update_dict_count(self, key):
        if key not in self.transformation_errors:
            self.transformation_errors[key] = 1
        self.transformation_errors[key] += 1

    def _send_data_for_queue(self, transformed_vends):
        vend_api_message = {"company_id": self.company_id,
                            "language": self.language,
                            "token": self.token,
                            "elastic_hash": self.elastic_hash,
                            "import_type": self.import_type,
                            'data': transformed_vends,
                            'skipped_vends': self.skipped_vends,
                            'email': self.email
                            }
        try:
            vend_producer.publish(vend_api_message, retry=True)
        except Exception as e:
            logger.exception(e)
            sys.exit(1)

    def _to_timestamp(self, value):
        return datetime.utcfromtimestamp(value).strftime('%Y-%m-%dT%H:%M:%SZ')

    def _get_machine_vendon_id(self, machine_external_id):
        for vendon_id, machine_id in self.machines.items():
            if str(machine_external_id) == str(machine_id):
                return str(vendon_id)

        raise VendonApiTransformException("Machine with external id {} not found ".format(machine_external_id))

    def _get_machine_ext_id(self, value):
        if value not in self.machines:
            raise VendonApiTransformException("Machine with id {} not found ".format(value))

        machine_id = self.machines[value]

        if len(str(machine_id).strip()) == 0:
            raise VendonApiTransformException("Machine with id {} has empty machine_id value ".format(value))

        return machine_id

    def _get_payment_type(self, value):
        payment_mapping = {
            "CASH": "CASH",
            "CASHLESS": "CASHLESS_1",
            "CASHLESS2": "CASHLESS_2",
            "TOKEN": "TOKEN",
            "FREEVEND": "FREE",
            "TEST": "TEST"
        }
        if value in payment_mapping:
            return payment_mapping[value]
        else:
            raise VendonApiTransformException("Unsupported payment method: {} ".format(value))

    def _transform_vends(self, vends):

        transformed_list = []

        for vend in vends:
            try:
                transformed_vend = {}
                for field_name, field_params in self.vend_field_mapping.items():
                    for field_param in field_params:
                        if field_param.get("func"):
                            transformed_vend[field_param["name"]] = getattr(self, field_param.get("func"))(vend[field_name])
                        else:
                            transformed_vend[field_param["name"]] = vend[field_name]

            except VendonApiTransformException as e:
                self.skipped_vends += 1
                self._update_dict_count(str(e))
                continue

            transformed_list.append(transformed_vend)

        if len(self.transformation_errors):
            self.general_process_logger.update_general_process_flow(
                self.transformation_errors,
                status=EnumErrorType.WARNING.name,
                key_enum=EnumValidationMessage.VENDON_TRANSFORM_ERROR.value,
                elastic_hash=self.elastic_hash
            )

        return transformed_list

    def _fetch_machines(self):
        self.machines = {}
        machines_fetcher = VendonMachinesFetcher(self.company_id)
        machines = machines_fetcher.fetch_data()

        for machine in machines:
            self.machines[machine['id']] = str(machine['machine_id'])

        return


