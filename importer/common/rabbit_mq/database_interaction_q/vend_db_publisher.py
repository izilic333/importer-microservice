from kombu import Exchange, Producer, Queue
from common.rabbit_mq.common.const import ValidationEnum
from common.rabbit_mq.connection.connection import conn

vend_database_exchange = Exchange("{}".format(ValidationEnum.VEND_DATABASE_Q.value), type="direct")

producer = Producer(
    exchange=vend_database_exchange,
    channel=conn,
    routing_key="{}".format(ValidationEnum.VEND_DATABASE_Q_KEY.value)
)

vend_database_queue = Queue(
    name="{}".format(ValidationEnum.VEND_DATABASE_Q.value),
    exchange=vend_database_exchange,
    routing_key="{}".format(ValidationEnum.VEND_DATABASE_Q_KEY.value)
)

vend_database_queue.maybe_bind(conn)
vend_database_queue.declare()


def vend_publish_to_database(vend_data):

    try:
        producer.publish(vend_data, retry=True)
        return {'success': True, 'message': 'Publish vends data to database Q'.format(vend_data)}

    except ValueError as e:
        return{
            'success': False, 'message': "RabbitMQ fail, setting data to redis. Error: %s " % e
        }
