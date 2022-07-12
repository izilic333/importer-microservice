from kombu import Exchange, Queue
from kombu.mixins import ConsumerMixin

from common.logging.setup import logger
from common.mixin.enum_errors import EnumErrorType
from common.rabbit_mq.common.const import ValidationEnum
from common.rabbit_mq.connection.connection import conn
from common.validators.csv.csv_validator import CsvFileValidatorLocal, ProcessLogger
from common.mixin.enum_errors import EnumValidationMessage as enum_msg


exchange3 = Exchange("{}".format(ValidationEnum.VALIDATION_FILE_API_Q.value), type="direct")
queue3 = Queue(
    name="{}".format(ValidationEnum.VALIDATION_FILE_API_Q.value),
    exchange=exchange3,
    routing_key="{}".format(ValidationEnum.VALIDATION_Q_FILE_KEY.value)
)

logger_api = logger


class ConsumeQ(ConsumerMixin):
    def __init__(self, connection):
        self.connection = connection

    def get_consumers(self, Consumer, channel):
        return [
            Consumer(queue3, callbacks=[self.on_message_validate_file], accept=["json"]),
        ]

    def on_connection_error(self, exc, interval):
        logger_api.error("Consumer connection fail. {}".format(self.channel_errors))

    def connection_errors(self):
        logger_api.error("Consumer connection error. {}".format(self.channel_errors))

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


ConsumeQ(conn).run()