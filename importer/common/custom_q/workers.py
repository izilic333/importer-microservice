import json
from base64 import b64encode
from threading import Thread
from urllib.request import urlopen, Request
import pika

from common.mixin.mixin import generate_hash_for_json
from common.rabbit_mq.common.const import ValidationEnum
from common.rabbit_mq.connection.connection import conn
from database.cloud_database.core.company_query import CloudLocalDatabaseSync
from common.logging.setup import logger

import time

from kombu import Exchange, Producer, Queue

exchange = Exchange("{}".format(ValidationEnum.CUSTOM_PUBLISHER.value), type="direct")

producer = Producer(
    exchange=exchange,
    channel=conn,
    routing_key="{}".format(ValidationEnum.CUSTOM_Q_PUBLISHER.value)
)
queue = Queue(
    name="{}".format(ValidationEnum.CUSTOM_PUBLISHER.value),
    exchange=exchange,
    routing_key="{}".format(ValidationEnum.CUSTOM_Q_PUBLISHER.value)
)

queue.maybe_bind(conn)
queue.declare()

pika_connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost',
            port=5672,
            credentials=pika.credentials.PlainCredentials('guest', 'guest'),
        ))

channel_conn = pika_connection.channel()
channel_conn.basic_qos(prefetch_size=0, prefetch_count=1, global_qos=False, callback=None)


class FillRabbitMQWithCloudProcess(object):
    def __init__(self, sleep_time):
        self.time_sleep = sleep_time

    def sync_importer_with_cloud(self):
        CloudLocalDatabaseSync.update_main_importer_status_on_cloud()

    def run_sync_with_cloud(self):
        CloudLocalDatabaseSync.query_cloud_company_initial_insert()


    def run_message(self):

        total_msg = channel_conn\
            .queue_declare(queue="{}".format(ValidationEnum.CUSTOM_PUBLISHER.value),
            durable=True, exclusive=False,
            auto_delete=False).method.message_count

        params = json.dumps(
            {
                "vhost": "/",
                "name": "{}".format(ValidationEnum.CUSTOM_PUBLISHER.value),
                "truncate": "50000",
                "requeue": "true",
                "encoding": "auto",
                "count": total_msg
            }
        ).encode('utf8')

        headers = {'authorization':
                    b'Basic ' + b64encode(('guest' + ':' + 'guest').encode('utf-8')),
                   'Content-Type':'application/json'
                   }

        url = (
                'http://127.0.0.1:15672/api/queues/%2F/{}/get'
                .format(ValidationEnum.CUSTOM_PUBLISHER.value)
        )
        r = urlopen(Request(url, params, headers))
        tr = r.read()
        output_data = json.loads(tr.decode())

        def check_json(input_base):
            for x in output_data:
                make_hash_loop = generate_hash_for_json(json.loads(x['payload']))
                if make_hash_loop == input_base:
                    return True
                else:
                    return False

        query_data = CloudLocalDatabaseSync.setup_cron_job()
        if query_data:
            for cron_job in query_data:
                what_to_do = check_json(generate_hash_for_json(cron_job))
                if not what_to_do:
                    producer.publish(
                        cron_job, retry=True, headers={
                            "x-message-ttl": int(cron_job['cron_time'])*10
                        },
                    )

                continue

    def run(self):
        while True:
            self.run_sync_with_cloud()
            self.sync_importer_with_cloud()
            self.run_message()
            time.sleep(self.time_sleep)

"""
if __name__=='__main__':
    third = FillRabbitMQWithCloudProcess(10)
    third_thread = Thread(third.run())
    third_thread.start()
"""



