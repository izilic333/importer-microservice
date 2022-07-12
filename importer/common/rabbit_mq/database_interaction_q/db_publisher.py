from elasticsearch_component.core.logger import CompanyProcessLogger
from kombu import Exchange, Producer, Queue

from common.rabbit_mq.common.const import ValidationEnum
from common.rabbit_mq.connection.connection import conn
from common.mixin.enum_errors import EnumErrorType as status
from common.logging.setup import logger

exchange = Exchange("{}".format(ValidationEnum.DATABASE_Q.value), type="direct")

producer = Producer(
    exchange=exchange,
    channel=conn,
    routing_key="{}".format(ValidationEnum.DATABASE_Q_KEY.value)
)

queue = Queue(
    name="{}".format(ValidationEnum.DATABASE_Q.value),
    exchange=exchange,
    routing_key="{}".format(ValidationEnum.DATABASE_Q_KEY.value)
)

queue.maybe_bind(conn)
queue.declare()


class PublishJsonFileToDatabaseQ(object):

    @classmethod
    def publish_new_data_to_database(cls, company_id, elastic_hash, data, type_of_process, token,
                                     email):
        """

        :param company_id: cloud company_id
        :param elastic_hash: uuid of elastic hash
        :param data: JSON data for specific request
        :param type_of_process: MACHINES, REGIONS,
        :param token: JWT token for CLOUD
        :param email: email if exists for email alert
        :return: it will return results of success or fail process inserting in Rabbit MQ
        """
        CompanyProcessLogger.create_process_flow(
            elastic_hash, 'Processes in Q for database insert.', status.IN_PROGRESS.name
        )

        message = {
            'company_id': company_id,
            'elastic_hash': elastic_hash,
            'data': data,
            'type': type_of_process,
            'token': token,
            'email': email
        }

        try:
            producer.publish(message, retry=True)
            logger.info("Process publish to Q, type_of_process: {}, company_id: {}".format(type_of_process, company_id))
            return {'success': True, 'message': "Published to Q. Your data: {}".format(message)}
        except ValueError as e:
            logger.error("Cant publish to validation Q for file validation. Error: %s" % e)
            return {
                'success': False, 'message': "RabbitMQ fail, setting data to redis. Error: %s" % e
            }

    @classmethod
    def republish_to_database(cls, data):
        """

        :param data: JSON data
        :return: it will return results of success or fail process inserting in Rabbit MQ
        """
        CompanyProcessLogger.create_process_flow(
            data['elastic_hash'], 'Processes in Q for database insert.', status.IN_PROGRESS.name
        )
        try:
            producer.publish(data, retry=True)
            logger.info("Process publish to Q")
            return {'success': True, 'message': "Published to Q. Your data: {}".format(data)}
        except ValueError as e:
            logger.error("Cant publish to validation Q for file validation. Error: %s" % e)
            return {
                'success': False, 'message': "RabbitMQ fail, setting data to redis. Error: %s" % e
            }


