from common.mixin.validation_const import return_import_type_id_custom_validation
from database.company_database.core.query_history import CompanyHistory
from elasticsearch_component.core.logger import CompanyProcessLogger
from kombu import Exchange, Producer, Queue

from common.rabbit_mq.common.const import ValidationEnum
from common.rabbit_mq.connection.connection import conn
from common.mixin.enum_errors import EnumErrorType as status

from common.logging.setup import logger

exchange = Exchange("{}".format(ValidationEnum.VALIDATION_FILE_API_Q.value), type="direct")

producer = Producer(
    exchange=exchange,
    channel=conn,
    routing_key="{}".format(ValidationEnum.VALIDATION_Q_FILE_KEY.value)
)
queue = Queue(
    name="{}".format(ValidationEnum.VALIDATION_FILE_API_Q.value),
    exchange=exchange,
    routing_key="{}".format(ValidationEnum.VALIDATION_Q_FILE_KEY.value)
)

queue.maybe_bind(conn)
queue.declare()


class PublishFileToValidationMessageQ(object):
    @classmethod
    def publish_new_data_to_validation(cls, data, filename, elastic_hash, token):
        """

        :param data: JSON data
        :param filename: filename path
        :param elastic_hash: elastic hash uuid
        :param token: JWT token
        :return: it will return results of success or fail process inserting in Rabbit MQ
        """

        message = {
            'data': data,
            'elastic_hash': elastic_hash,
            'filename': filename,
            'token': token
        }

        try:
            CompanyProcessLogger.create_process_flow(
                elastic_hash, 'Processes in Q for file validation.', status.IN_PROGRESS.name
            )
        except Exception as e:
            logger.error("Problems with calling elastic {}".format(e))

        try:
            producer.publish(message, retry=True)
            CompanyHistory.insert_history(
                data['company'], 'csv_validator',
                return_import_type_id_custom_validation(data['import_type']),
                elastic_hash, '', filename, token
            )

            logger.info("Process publish to Q: {}".format(filename))
            return {'success': True, 'message': "Published to Q. Your data: {}".format(message)}
        except ValueError as e:
            logger.error("Cant publish to validation Q for file validation. Error: %s" % e)
            return {
                'success': True, 'message': "RabbitMQ fail, setting data to redis. Error: %s" % e
            }
