from kombu import Exchange, Producer, Queue
from common.rabbit_mq.common.const import ValidationEnum
from common.rabbit_mq.connection.connection import conn

from common.logging.setup import logger

exchange = Exchange("{}".format(ValidationEnum.EXPORT_PUBLISHER.value), type="direct")

producer = Producer(
    exchange=exchange,
    channel=conn,
    routing_key="{}".format(ValidationEnum.EXPORT_Q_PUBLISHER.value)
)
queue = Queue(
    name="{}".format(ValidationEnum.EXPORT_PUBLISHER.value),
    exchange=exchange,
    routing_key="{}".format(ValidationEnum.EXPORT_Q_PUBLISHER.value)
)

queue.maybe_bind(conn)
queue.declare()


class PublishExportQ(object):
    @classmethod
    def publish_new_export(cls, data):
        """

        :param data: JSON data to export request
        :return: it will return results of success or fail process inserting in Rabbit MQ
        """
        try:
            producer.publish(data, retry=True)
            logger.info("New publish for export: {}".format(data))
            return {'success': True, 'message': 'File pushed to Q.'}
        except ValueError as e:
            logger.error("Cant publish new export request. Error: %s" % e)
            return {
                'success': False, 'message': "RabbitMQ fail. Error: %s" % e
            }
