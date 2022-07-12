import json
import pika

from base64 import b64encode

from common.rabbit_mq.connection.connection import conn
from common.logging.setup import logger
from common.mixin.mixin import generate_hash_for_json
from common.urls.urls import pika_host, pika_port, pika_username, pika_password, rabbit_mq_url

from kombu import Exchange, Producer
from urllib.request import urlopen, Request


class FallBackRabbitMq(object):

    @staticmethod
    def generate_hash_for_json_object(json_object, data):
        hash_input = generate_hash_for_json(data)
        for x in json_object:
            make_hash_loop = generate_hash_for_json(json.loads(x['payload']))
            if make_hash_loop == hash_input:
                return True
            else:
                return False

    @classmethod
    def re_insert_to_q(cls, custom_publisher, custom_q_publisher, data):
        exchange = Exchange("{}".format(custom_publisher), type="direct")

        producer = Producer(
            exchange=exchange,
            channel=conn,
            routing_key="{}".format(custom_q_publisher)
        )

        pika_connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='%s' % pika_host,
            port=pika_port,
            credentials=pika.credentials.PlainCredentials('%s' % pika_username, '%s' % pika_password),
        ))

        channel_conn = pika_connection.channel()

        def return_rabbit_mq_json():
            total_msg = (
                channel_conn
                .queue_declare(queue="{}".format(custom_publisher), durable=True, exclusive=False,
                               auto_delete=False).method.message_count
            )

            params = json.dumps(
                {
                    "vhost": "/",
                    "name": "{}".format(custom_publisher),
                    "truncate": "50000",
                    "requeue": "true",
                    "encoding": "auto",
                    "count": total_msg
                }
            ).encode('utf8')

            headers = {'authorization':
                           b'Basic ' + b64encode(
                               ('%s' % pika_username + ':' + '%s' % pika_password).encode('utf-8')
                           ),
                       'Content-Type': 'application/json'
                       }

            rabbit_mq_host = '%s' % rabbit_mq_url

            url = rabbit_mq_host.format(custom_publisher)

            request_to_rabbit = urlopen(Request('{}'.format(url), params, headers))
            try_request = request_to_rabbit.read()

            return json.loads(try_request.decode())

        # Response from Q
        while not len(return_rabbit_mq_json()):
            output_data = return_rabbit_mq_json()
            if len(output_data):
                q_check = cls.generate_hash_for_json_object(output_data, data)
                if not q_check:
                    logger.info('Data is not in cloud. {}'.format(data))
                    producer.publish(data, retry=True)
                    return True
                logger.info('Data is already in cloud. {}'.format(data))
                return True

        return

