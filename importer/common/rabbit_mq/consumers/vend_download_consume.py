from kombu import Exchange, Queue
from kombu.mixins import ConsumerMixin
from common.logging.setup import vend_logger
from common.rabbit_mq.connection.connection import conn
from common.rabbit_mq.common.const import ValidationEnum
from common.validators.vend_importer_validator.cpi_vend_processing import ProcessingVendFile


vend_file_downloader_exchange2 = Exchange("{}".format(ValidationEnum.VEND_DOWNLOADER_FILE_Q.value), type="direct")
vend_file_downloader_queue2 = Queue(
    name="{}".format(ValidationEnum.VEND_DOWNLOADER_FILE_Q.value),
    exchange=vend_file_downloader_exchange2,
    routing_key="{}".format(ValidationEnum.VEND_DOWNLOADER_FILE_Q_KEY))


class VendConsumeQ(ConsumerMixin):
    def __init__(self, connection):
        self.connection = connection

    def get_consumers(self, consumer, channel):
        return [
            consumer(vend_file_downloader_queue2, callbacks=[self.file_downloader], accept=["json"]),
        ]

    @staticmethod
    def file_downloader(data, message):
        try:
            processing = ProcessingVendFile(data)
            try:
                processing.processing_vends_ftp_file()
            except Exception as e:
                vend_logger.error(e)
        except Exception as e:
            vend_logger.error(e)
        message.ack()


VendConsumeQ(conn).run()


