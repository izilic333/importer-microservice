from kombu import Exchange, Queue
from kombu.mixins import ConsumerMixin
from common.logging.setup import vend_logger
from common.rabbit_mq.connection.connection import conn
from common.rabbit_mq.common.const import ValidationEnum
from common.validators.vend_cloud_validator.vendon_cloud_validator import VendonCloudValidator


vend_cloud_validator_exchange_api = Exchange("{}".format(ValidationEnum.VEND_VALIDATION_API_Q.value), type='direct')
vend_cloud_validator_queue_api = Queue(
    name="{}".format(ValidationEnum.VEND_VALIDATION_API_Q.value),
    exchange=vend_cloud_validator_exchange_api,
    routing_key="{}".format(ValidationEnum.VEND_VALIDATION_API_Q_KEY))


class VendConsumeQ(ConsumerMixin):
    def __init__(self, connection):
        self.connection = connection

    def get_consumers(self, consumer, channel):
        return [
            consumer(vend_cloud_validator_queue_api, callbacks=[self.api_cloud_processing], accept=["json"])
        ]

    @staticmethod
    def api_cloud_processing(data, message):

        try:
            validator = VendonCloudValidator(data)
            process_hash = data.get('elastic_hash')
            try:
                vend_logger.info("Starting Vendon API cloud validation for hash {}".format(process_hash))
                validator.validate()
                vend_logger.info("Finished Vendon API cloud validation for hash {}".format(process_hash))
            except Exception as e:
                vend_logger.error(e)
        except Exception as e:
            vend_logger.error(e)
        message.ack()


VendConsumeQ(conn).run()


