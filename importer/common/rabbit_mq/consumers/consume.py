from kombu import Exchange, Queue
from kombu.mixins import ConsumerMixin

from common.logging.setup import logger
from common.mixin.enum_errors import EnumErrorType
from common.rabbit_mq.common.const import ValidationEnum
from common.rabbit_mq.connection.connection import conn
from common.validators.cloud_db.cloud_validator import FileOnCloudValidator
from common.validators.csv.csv_validator import CsvFileValidatorLocal, ProcessLogger
from database.company_database.core.query_export import ExportHistory
from common.mixin.enum_errors import EnumValidationMessage as enum_msg


exchange = Exchange("{}".format(ValidationEnum.VALIDATION_Q.value), type="direct")
queue = Queue(
    name="{}".format(ValidationEnum.VALIDATION_Q.value),
    exchange=exchange,
    routing_key="{}".format(ValidationEnum.VALIDATION_Q_KEY.value)
)

exchange4 = Exchange("{}".format(ValidationEnum.EXPORT_PUBLISHER.value), type="direct")
queue4 = Queue(
    name="{}".format(ValidationEnum.EXPORT_PUBLISHER.value),
    exchange=exchange4,
    routing_key="{}".format(ValidationEnum.EXPORT_Q_PUBLISHER.value)
)

logger_api = logger


class ConsumeQ(ConsumerMixin):
    def __init__(self, connection):
        self.connection = connection

    def get_consumers(self, Consumer, channel):
        return [
            Consumer(queue, callbacks=[self.on_message_file], accept=["json"]),
            Consumer(queue4, callbacks=[self.on_message_export], accept=["json"])
        ]

    def on_connection_error(self, exc, interval):
        logger_api.error("Consumer connection fail. {}".format(self.channel_errors))

    def connection_errors(self):
        logger_api.error("Consumer connection error. {}".format(self.channel_errors))

    def on_message_file(self, body, message):
        """

        :param body: RabbitMQ body
        :param message: JSON message from Q
        :return: it will call function for next process
        """
        try:
            fcv = FileOnCloudValidator(body)
            try:
                fcv.validate_file_on_cloud()
            except Exception as e:
                elastic_hash = body.get('elastic_hash')
                company_id = body.get('company_id')
                language = body.get('language')
                user_token = body.get('token')
                import_type = body.get('type')
                email = body.get('email')
                file_path = body.get('input_file')

                # General logger initialization!
                cloud_logger = ProcessLogger(
                    company_id=company_id, import_type=import_type, elastic_hash=elastic_hash, token=user_token
                )
                # Message for importer/super user and system log message, with exception details!
                cloud_logger.update_process_flow(
                    e, error=EnumErrorType.ERROR.name,
                    language=language,
                    key_enum=enum_msg.CLOUD_VALIDATION_EXCEPTION_ERROR_DETAIL_MESSAGE.value,
                )
                # Cloud main message, and update main elastic process!
                cloud_logger.create_process_and_cloud_flow_and_main(
                    error=EnumErrorType.ERROR,
                    file_path=file_path,
                    email=email,
                    language=language,
                    key_enum=enum_msg.CLOUD_VALIDATION_EXCEPTION_ERROR.value
                )
                logger_api.exception('Error in cloud validator: {}'.format(e))
        except Exception as e:
            logger_api.exception('Error in cloud validator: {}'.format(e))

        message.ack()

    def on_message_validate_file(self, body, message):
        """

            :param body: RabbitMQ body
            :param message: JSON message from Q
            :return: it will call function for next process
        """
        try:
            cfv = CsvFileValidatorLocal(body['data'], body['filename'], body['elastic_hash'],
                                        body['token'])
            try:
                cfv.validation_for_specific_file()
            except Exception as e:
                elastic_hash = body.get('elastic_hash')
                company_id = body.get('company_id')
                language = body.get('language')
                user_token = body.get('token')
                import_type = body.get('type')
                email = body.get('email')
                file_path = body.get('input_file')

                # General logger initialization!
                cloud_logger = ProcessLogger(
                    company_id=company_id, import_type=import_type, elastic_hash=elastic_hash, token=user_token
                )
                # Message for importer/super user and system log message, with exception details!
                cloud_logger.update_process_flow(
                    e, error=EnumErrorType.ERROR.name,
                    language=language,
                    key_enum=enum_msg.CONSUMER_VALIDATION_ERROR_DETAILS.value,
                )
                # Cloud main message, and update main elastic process!
                cloud_logger.create_process_and_cloud_flow_and_main(
                    error=EnumErrorType.ERROR,
                    file_path=file_path,
                    email=email,
                    language=language,
                    key_enum=enum_msg.CLOUD_VALIDATION_EXCEPTION_ERROR.value
                )

        except Exception as e:
            logger_api.exception('CSV validator error: {}'.format(e))

        message.ack()

    def on_message_export(self, body, message):
        """

            :param body: RabbitMQ body
            :param message: JSON message from Q
            :return: it will call function for next process
        """
        try:
            ExportHistory.call_method_based_on_type(body)
        except Exception as e:
            logger_api.exception('Export Q error: {}'.format(e))
        message.ack()

ConsumeQ(conn).run()
