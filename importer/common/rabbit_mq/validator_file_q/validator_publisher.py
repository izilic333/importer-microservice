from kombu import Exchange, Producer, Queue

from common.redis_setup.core.handle_fail_of_rabbitmq import RedisHandler
from elasticsearch_component.core.logger import CompanyProcessLogger

from common.rabbit_mq.common.const import ValidationEnum
from common.rabbit_mq.connection.connection import conn
from common.mixin.enum_errors import EnumErrorType as status

from common.logging.setup import logger

exchange = Exchange("{}".format(ValidationEnum.VALIDATION_Q.value), type="direct")

producer = Producer(
    exchange=exchange,
    channel=conn,
    routing_key="{}".format(ValidationEnum.VALIDATION_Q_KEY.value)
)
queue = Queue(
    name="{}".format(ValidationEnum.VALIDATION_Q.value),
    exchange=exchange,
    routing_key="{}".format(ValidationEnum.VALIDATION_Q_KEY.value)
)

queue.maybe_bind(conn)
queue.declare()

def validate_publish_message(company_id, elastic_hash, data, type_of_process):
    """

    :param company_id: cloud company_id
    :param elastic_hash: uuid of process elasticsearch
    :param data: JSON process
    :param type_of_process: MACHINES, LOCATIONS ...
    :return: JSON of success validation
    """
    if not company_id or company_id is None:
        return {'success': False, 'message': 'Please send company_id.'}
    elif data and len(data) == 0:
        return {'success': False, 'message': 'Data is empty for this request.'}
    elif len(elastic_hash) == 0:
        return {'success': False, 'message': 'Please send elastic_hash.'}
    elif type_of_process is None:
        return {'success': False, 'message': 'Please send type_of_process.'}
    else:
        return {'success': True, 'message': 'Process added to Q.'}


def publish_file_validation(company_id, elastic_hash, data, type_of_process, emails, token,
                            input_file, language='en'):
    """

    :param company_id:  cloud company_id
    :param elastic_hash: uuid of process elasticsearch
    :param data: JSON process
    :param type_of_process: MACHINES, LOCATIONS ...
    :param emails: email if exists
    :param token: JWT token
    :param input_file: file path
    :return: it will return results of success or fail process inserting in Rabbit MQ
    """
    check_data = validate_publish_message(company_id, elastic_hash, data, type_of_process)
    if not check_data['success']:
        return check_data


    # Check redis if keys exists

    redis_store = RedisHandler.return_all_data_from_redis_by_key(type_of_process)
    if len(redis_store) > 0:
        for x in redis_store:
            CompanyProcessLogger.create_process_flow(x['elastic_hash'], 'Processes in Q',
                                                     status.IN_PROGRESS.name)
            redis_key = x['type'] + '_' + x['elastic_hash']
            RedisHandler.delete_redis_key_from_storage(redis_key)
            producer.publish(x, retry=True)

    # Process current message

    message = {
        'company_id': company_id,
        'elastic_hash': elastic_hash,
        'data': data,
        'type': type_of_process,
        'email': emails,
        'token': token,
        'input_file': input_file,
        'language': language
    }

    status_update = CompanyProcessLogger.create_process_flow(
        elastic_hash,
        'Processes in Q for database validation.',
        status.IN_PROGRESS.name
    )

    if not status_update['process_updated']:
        return {'success': False, 'message': status_update['message']}

    try:
        producer.publish(message, retry=True)
        logger.info("Process published to Q")
        return {'success': True, 'message': "Published to Q. Your data: {}".format(message)}
    except Exception as e:
        logger.error("Cant publish to validation Q for file validation. Error: {}".format(e))
        logger.info("Calling redis for data store. Data: %s" % message)
        try:
            RedisHandler.set_redis_validation_fail_process(message, type_of_process)
            return {'success': True,
                    'message': "RabbitMQ fail, setting data to redis. Error: {}".format(e)}
        except Exception as e:
            logger.error("Redis error on inserting to storage. Error: {}".format(e))
            return {'success': False,
                    'message': "RabbitMQ fail, setting data to redis. Error: {}".format(e)
                    }
