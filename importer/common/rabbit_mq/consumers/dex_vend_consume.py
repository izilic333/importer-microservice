from kombu import Exchange, Queue
from kombu.mixins import ConsumerMixin
from common.logging.setup import vend_logger
from common.rabbit_mq.connection.connection import conn
from common.rabbit_mq.common.const import ValidationEnum
from common.validators.vend_cloud_validator.dex_cloud_validator import DEXCloudValidator


dex_vend_cloud_validator_exchange = Exchange("{}".format(ValidationEnum.DEX_VALIDATION_FILE_Q.value), type='direct')
dex_vend_cloud_validator_queue = Queue(
    name="{}".format(ValidationEnum.DEX_VALIDATION_FILE_Q.value),
    exchange=dex_vend_cloud_validator_exchange,
    routing_key="{}".format(ValidationEnum.DEX_VALIDATION_FILE_Q_KEY))


class VendConsumeQ(ConsumerMixin):
    def __init__(self, connection):
        self.connection = connection

    def get_consumers(self, consumer, channel):
        return [
            consumer(dex_vend_cloud_validator_queue, callbacks=[self.dex_vend_processing], accept=["json"]),
        ]

    @staticmethod
    def dex_vend_processing(data, message):
        try:
            processing = DEXCloudValidator(data)
            try:
                processing.main_basic_cloud_dex_validations()
            except Exception as e:
                vend_logger.error(e)
        except Exception as e:
            vend_logger.error(e)
        message.ack()


VendConsumeQ(conn).run()


