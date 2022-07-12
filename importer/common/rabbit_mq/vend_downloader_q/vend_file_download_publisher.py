from common.logging.setup import vend_logger
from kombu import Exchange, Producer, Queue
from common.rabbit_mq.common.const import ValidationEnum
from common.rabbit_mq.connection.connection import conn
from common.redis_setup.core.handle_fail_of_rabbitmq import RedisHandler


# Define exchange for vend file download publisher
exchange = Exchange("{}".format(ValidationEnum.VEND_DOWNLOADER_FILE_Q.value), type="direct")

# Define vend producer for file download publisher
download_producer = Producer(
    exchange=exchange,
    channel=conn,
    routing_key="{}".format(ValidationEnum.VEND_DOWNLOADER_FILE_Q_KEY.value)
)

# Define queue for vend file download publisher
file_vend_download_queue = Queue(
    name="{}".format(ValidationEnum.VEND_DOWNLOADER_FILE_Q.value),
    exchange=exchange,
    routing_key="{}".format(ValidationEnum.VEND_DOWNLOADER_FILE_Q_KEY.value)
)
# Bind instance to channel if not already bound, a binding is a relationship between an exchange and a queue
file_vend_download_queue.maybe_bind(conn)

# Declare queue and exchange then binds queue to exchange, this step is necessary as publishing
# to a non-existing exchange is forbidden.
file_vend_download_queue.declare()


def vend_file_download_publisher(company_id, process_request_type, elastic_hash, email, vend_type, token, language,
                                 file_path):
    """
    This is download file publisher, simply all download file put in this Queue, in this step we assuming that file were
    previously grouped by device PID and then sorted by file timestamp! The order of file processing is very important
    part.
    :param company_id: company_id
    :param process_request_type: file
    :param elastic_hash: process hash
    :param email: email
    :param vend_type: CPI, SIP, etc ...
    :param token: JWT token
    :param language: for now is setting on english
    :param file_path: processing file path
    :return: It will return file for first step in processing vends!
    """

    # Second, check if there some vend keys in redis that wasn't published, republish this message if exists.
    # Because if publisher for some reason couldn't publish message in Queue, it will push them in redis.

    redis_all_vends_key = RedisHandler.return_all_data_from_redis_by_key(vend_type)
    if len(redis_all_vends_key) > 0:
        for vends_key in redis_all_vends_key:
            # Make elastic logging
            vends_redis_key = vends_key['type'] + '_' + vends_key['elastic_hash']
            RedisHandler.delete_redis_key_from_storage(vends_redis_key)
            download_producer.publish(vends_key, retry=True)

    # Third, create vends file processing message for publishing in vend file queue, and publish this message.
    # Try publish message to Queue if can't, set message to redis. In this way we increase the level of security that
    # the message will be processed, because this publisher on every time when is called, make checking if there is
    # some vend type keys in redis for republish!

    vend_processing_message = {
        'company_id': company_id,
        'elastic_hash': elastic_hash,
        'type': vend_type,
        'process_request_type': process_request_type,
        'email': email,
        'token': token,
        'language': language,
        'file_path': file_path
    }

    # Make elastic logging
    try:
        download_producer.publish(vend_processing_message, retry=True)
        return {'success': True, 'message': 'File download publisher. Your data: {}'.format(
            vend_processing_message)}
    except Exception as e:
        vend_logger(e)
        try:
            RedisHandler.set_redis_validation_fail_process(vend_processing_message, vend_type)
            return {'success': True, 'message': "RabbitMQ fail, setting data to redis. Error: {}".format(e)}
        except Exception as e:
            vend_logger(e)
            return {'success': False, 'message': "RabbitMQ fail, setting data to redis. Error: {}".format(e)}

