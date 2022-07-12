import uuid

from common.mixin.enum_errors import EnumErrorType
from common.logging.setup import logger
from datetime import datetime

from database.cloud_database.core.company_query import CloudLocalDatabaseSync
from elasticsearch_component.models.models import CompanyProcess
from elasticsearch_dsl import ValidationException
from elasticsearch_component.core.mixin import convert_string_to_json

logger_api = logger


class CompanyProcessLogger(object):
    """

        This class represent multi class function.
        Functions for creating new process and update a process follow log.

    """
    @classmethod
    def create_new_process(cls, company_id, process_type, process_request_type):
        """

        :param company_id: int type
        :param process_type: string type (API, FILE, CLOUD)
        :param process_request_type: string type (FAIL, SUCCESS ...)
        :return: elastic hash ID (uuid)

        """

        process_id = uuid.uuid4().hex
        try:
            company_process = CompanyProcess(meta={'id': process_id})
            company_process.company_id = int(company_id)
            company_process.company_name = CloudLocalDatabaseSync.return_company_name(company_id)
            company_process.process_type = '%s' % process_type
            company_process.status = '%s' % str(EnumErrorType.STARTED.name)
            company_process.process_request_type = '%s' % process_request_type
            company_process.created_at = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            company_process.save()


            return {'process_created': True,
                    'message': 'Process created for company: %d' % int(company_id),
                    'id': process_id
                    }
        except ValidationException as e:
            return {'process_created': False, 'message': e}

    @classmethod
    def create_process_flow(cls, process_hash, message, status):
        """

        :param process_hash: uuid main process hash
        :param message: message generated from functions on each step process
        :param status: string type (FAIL, SUCCESS, IN_PROGRESS)
        :return: JSON with results True or False of create process flow

        """
        try:
            if CompanyProcess.get(id=process_hash, ignore=404):

                company_process_log = CompanyProcess.get(id=process_hash, ignore=404)
                company_process_log.add_process(message, status)
                company_process_log.save()

                return {'process_updated': True,
                        'message': 'Process updated with hash: %s' % process_hash
                        }
            else:
                return {
                    'process_updated': False,
                    'message': 'Process not exists for hash: %s' % process_hash
                }
        except Exception as e:
            logger_api.error("Elastic error update process: {}".format(e))
            return {'process_updated': False, 'message': e}

    @classmethod
    def create_cloud_process_flow(cls, process_hash, status, message):
        """

            :param process_hash: uuid main process hash
            :param message: message generated from functions on each step process
            :param status: string type (FAIL, SUCCESS, IN_PROGRESS)
            :return: JSON with results True or False of create process flow

        """
        if CompanyProcess.get(id=process_hash, ignore=404):
            try:
                company_process_log = CompanyProcess.get(id=process_hash, ignore=404)
                company_process_log.add_cloud_process(message, status)
                company_process_log.save()

                return {'process_updated': True,
                        'message': 'Process updated with hash: %s' % process_hash
                        }
            except ValidationException as e:
                return {'process_updated': False, 'message': e}
        else:
            return {
                'process_updated': False,
                'message': 'Process not exists for hash: %s' % process_hash
            }

    @classmethod
    def update_main_process(cls, process_hash, status):
        """

        :param process_hash:  uuid main process hash
        :param status: ERROR, SUCCESS and WARNING
        :return: JSON with results True or False of closing process

        """
        if CompanyProcess.get(id=process_hash, ignore=404):
            try:
                company_process_log = CompanyProcess.get(id=process_hash, ignore=404)
                company_process_log.status = str(status)
                company_process_log.save()

                return {'process_updated': True,
                        'message': 'Process status updated with: %s' % process_hash
                        }
            except ValidationException as e:
                return {'process_updated': False, 'message': e}
        else:
            return {
                'process_updated': False,
                'message': 'Process not exists for hash: %s' % process_hash
            }

    @classmethod
    def query_company_process(cls, process_hash):
        """
        :param process_hash: uuid of main process hash
        :return: JSON with results of query request
        """
        company_process = (CompanyProcess()
                           .search()
                           .query('match', _id=process_hash)
                           )
        response = company_process.execute()
        output_query = []

        for hit in response:
            output_query.append(
                {
                    'id': hit._id,
                    'company_id': hit.company_id,
                    'process_type': hit.process_type,
                    'process_request_type': hit.process_request_type,
                    'company_name': hit.company_name,
                    'status': hit.status,
                    'created_at': hit.created_at,
                    'process_history': [
                        {
                            'message': convert_string_to_json(x['message']),
                            'status': x['status'],
                            'date': x['process_created_at'].strftime("%Y-%m-%d %H:%M:%S")

                        } for x in hit.process
                    ],
                    'process_cloud': [
                        {
                            'message': convert_string_to_json(x['cloud_message']),
                            'status': x['cloud_status'],
                            'date': x['cloud_process_created_at'].strftime("%Y-%m-%d %H:%M:%S")

                        } for x in hit.process_cloud
                    ]
                }
            )

        return output_query


if __name__ == '__main__':
    print(CompanyProcessLogger.create_new_process(557, 'EVENTS', 'API'))