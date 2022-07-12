from common.rabbit_mq.validator_file_q.validator_publisher import validate_publish_message
from kombu import Exchange, Producer, Queue
from common.rabbit_mq.common.const import ValidationEnum
from common.rabbit_mq.connection.connection import conn
from common.redis_setup.core.handle_fail_of_rabbitmq import RedisHandler


# This is producer and queue definition for cpi vend importer
cpi_vend_validator_exchange = Exchange("{}".format(ValidationEnum.VEND_VALIDATION_FILE_Q.value), type="direct")
cpi_vend_producer = Producer(
    exchange=cpi_vend_validator_exchange,
    channel=conn,
    routing_key="{}".format(ValidationEnum.VEND_VALIDATION_FILE_Q_KEY.value))

cpi_vend_queue = Queue(
    name="{}".format(ValidationEnum.VEND_VALIDATION_FILE_Q.value),
    exchange=cpi_vend_validator_exchange,
    routing_key="{}".format(ValidationEnum.VEND_VALIDATION_FILE_Q_KEY.value))

cpi_vend_queue.maybe_bind(conn)
cpi_vend_queue.declare()

# This is producer and queue definition for dex vend importer
dex_vend_validator_exchange = Exchange("{}".format(ValidationEnum.DEX_VALIDATION_FILE_Q.value), type="direct")
dex_vend_producer = Producer(
    exchange=dex_vend_validator_exchange,
    channel=conn,
    routing_key="{}".format(ValidationEnum.DEX_VALIDATION_FILE_Q_KEY.value))

dex_vend_queue = Queue(
    name="{}".format(ValidationEnum.DEX_VALIDATION_FILE_Q.value),
    exchange=dex_vend_validator_exchange,
    routing_key="{}".format(ValidationEnum.DEX_VALIDATION_FILE_Q_KEY.value))

dex_vend_queue.maybe_bind(conn)
dex_vend_queue.declare()


def publish_vend_file_processing(company_id, import_type, elastic_hash, data, vend_type, emails, token, cpi_processor,
                                 dex_processor, language='en'):
    """
    This is main function for publish vend in Q, based on file processing. Function make basic validation on
    his parameters as first step, and then check if there is something in redis from previous processing that
    could't be published in Queue, and if there is something make republish, and as main thing make publish
    in vend Queue for file processing.

    :param company_id: cloud company_id
    :param cpi_processor: CPI_VENDS
    :param dex_processor: DEX_VENDS
    :param import_type: import type
    :param elastic_hash: uuid of elastic search process
    :param data: JSON vend data for processing
    :param vend_type: CPI, NAYAX, Vcore, SIP, Unicum, etc ...
    :param emails: email if exists
    :param token: JWT token
    :param language: for now is setting on english
    :return: it will return results of success or fail process inserting in Rabbit MQ
    """
    # Make basic validation on main data that was sent
    basic_data_validate = validate_publish_message(company_id, elastic_hash, data, vend_type)
    if not basic_data_validate['success']:
        return basic_data_validate

    # Check if there some vend keys in redis that wasn't published, republish this message if exists.
    # Because if publisher for some reason couldn't publish message in Queue, it will push them in redis.

    redis_all_vends_key = RedisHandler.return_all_data_from_redis_by_key(vend_type)
    if len(redis_all_vends_key) > 0:
        for vend_keys in redis_all_vends_key:
            # Make elastic logging
            vend_redis_key = vend_keys['import_type'] + '_' + vend_keys['elastic_hash']
            if import_type == cpi_processor:
                RedisHandler.delete_redis_key_from_storage(vend_redis_key)
                cpi_vend_producer.publish(vend_keys, retry=True)
            elif import_type == dex_processor:
                RedisHandler.delete_redis_key_from_storage(vend_redis_key)
                dex_vend_producer.publish(vend_keys, retry=True)

    # Create vends file processing message for publishing in vend file queue, and publish this message.
    # Try publish message to Queue if can't, set message to redis. In this way we increase the level of security that
    # the message will be processed, because this publisher on every time when is called, make checking if there is
    # some vend type keys in redis for republish!
    vend_file_message = {
        'company_id': company_id,
        'elastic_hash': elastic_hash,
        'data': data,
        'type': vend_type,
        'email': emails,
        'token': token,
        'language': language,
        'import_type': import_type
    }

    # Make elastic/general logging for thi action

    try:
        if import_type == cpi_processor:
            cpi_vend_producer.publish(vend_file_message, retry=True)
            return {'success': True, 'message': 'Published to Q. Your data: {}'.format(vend_file_message)}
        elif import_type == dex_processor:
            dex_vend_producer.publish(vend_file_message, retry=True)
            return {'success': True, 'message': 'Published to Q. Your data: {}'.format(vend_file_message)}
    except Exception as e:
        # Make logging
        try:
            RedisHandler.set_redis_validation_fail_process(vend_file_message, import_type)
            return {'success': True, 'message': "RabbitMQ fail, setting data to redis. Error: {}".format(e)}
        except Exception as e:
            # Make logging
            return {'success': False, 'message': "RabbitMQ fail, setting data to redis. Error: {}".format(e)}








