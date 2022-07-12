from database.cloud_database.core.company_query import CloudLocalDatabaseSync
from elasticsearch_component.core.mixin import convert_string_to_json
from datetime import datetime
from common.mixin.enum_errors import EnumErrorType
from common.urls.urls import elasticsearch_connection_url
from time import sleep
from elasticsearch_dsl import Search
from elasticsearch_component.connection.connection import elastic_conn
import uuid
from elasticsearch_dsl import ValidationException
from common.logging.setup import vend_logger
from elasticsearch_component.models.vend_models import (VendImportProcess, VendImportIndex)


logger_api = vend_logger


class VendImportProcessLogger(object):
    """
    Container class for holding methods pertaining to logging vend imports.
    """
    # Use a vend index prefix and * wildcard to encompass all vend indices
    vend_index = elasticsearch_connection_url.get('index_vend', '') + '*'
    client = elastic_conn

    @classmethod
    def create_new_process(cls, company_id, import_type, import_request_type):
        """
        :param company_id: int, company id for which import is run
        :param import_type: string, type of import(CPI, DJ...)
        :param import_request_type: string, type of request(FTP, API)
        :return: process hash
        """
        import_id = uuid.uuid4().hex
        try:
            # Since index is not static, we need to create an index based on current datetime,
            # or use an existing index.
            import_process = VendImportProcess(
                meta={
                    'id': import_id,
                    'index': VendImportIndex.init(
                        elasticsearch_connection_url['index_vend'] + str(datetime.now().strftime('%Y-%m'))
                    )
                }
            )
        except Exception as e:
            return {'process_created': False, 'message': e}

        import_process.company_id = int(company_id)
        import_process.company_name = CloudLocalDatabaseSync.return_company_name(company_id)
        import_process.import_type = '%s' % import_type
        import_process.status = '%s' % str(EnumErrorType.STARTED.name)
        import_process.import_request_type = '%s' % import_request_type
        import_process.created_at = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        import_process.save()
        sleep(1)
        return {'process_created': True,
                'message': 'Import process created for company: %d' % int(company_id),
                'id': import_id
                }

    @classmethod
    def create_importer_validation_process_flow(cls, process_hash, message, status):
        """

        :param process_hash: uuid main process hash
        :param message: message generated from functions on each step process
        :param status: string type (FAIL, SUCCESS, IN_PROGRESS)
        :return: JSON with results True or False of create process flow

        """
        process_list = cls.search_all_processes(process_hash)

        if len(process_list):
            process = process_list[0]
        else:
            return {
                'process_updated': False,
                'message': 'Process does not exist for hash: %s' % process_hash
            }

        if VendImportProcess.get(id=process['hash'], index=process['index'], ignore=404):
            try:
                vend_import_process_log = VendImportProcess.get(id=process['hash'], index=process['index'], ignore=404)
                vend_import_process_log.add_importer_validation_process(message, status)
                vend_import_process_log.save()

                return {'process_updated': True,
                        'message': 'Process updated with hash: %s' % process_hash
                        }
            except ValidationException as e:
                return {'process_updated': False, 'message': e}
        else:
            return {
                'process_updated': False,
                'message': 'Process does not exist for hash: %s' % process_hash
            }

    @classmethod
    def create_cloud_validation_process_flow(cls, process_hash, status, message):
        """

            :param process_hash: uuid main process hash
            :param message: message generated from functions on each step process
            :param status: string type (FAIL, SUCCESS, IN_PROGRESS)
            :return: JSON with results True or False of create process flow

        """
        process = cls.search_all_processes(process_hash)
        if len(process):
            process = process[0]
            if VendImportProcess.get(id=process['hash'], index=process['index'], ignore=404):
                try:
                    vend_import_process_log = VendImportProcess.get(id=process['hash'], index=process['index'], ignore=404)
                    vend_import_process_log.add_cloud_validation_process(message, status)
                    vend_import_process_log.save()

                    return {'process_updated': True,
                            'message': 'Process updated with hash: %s' % process_hash
                            }
                except ValidationException as e:
                    return {'process_updated': False, 'message': e}
            else:
                return {
                    'process_updated': False,
                    'message': 'Process does not exist for hash: %s' % process_hash
                }
        else:
            logger_api.error('Elastic search could not find specific process')

    @classmethod
    def update_main_vend_process(cls, process_hash, status):
        """

        :param process_hash:  uuid main process hash
        :param status: ERROR, SUCCESS and WARNING
        :return: JSON with results True or False of closing process

        """
        process_list = cls.search_all_processes(process_hash)

        if len(process_list):
            process = process_list[0]
        else:
            return {
                'process_updated': False,
                'message': 'Process does not exist for hash: %s' % process_hash
            }

        if VendImportProcess.get(id=process['hash'], index=process['index'], ignore=404):
            try:
                vend_import_process_log = VendImportProcess.get(id=process['hash'], index=process['index'], ignore=404)
                vend_import_process_log.status = str(status)
                vend_import_process_log.save()

                return {'process_updated': True,
                        'message': 'Process updated with hash: %s' % process_hash
                        }
            except ValidationException as e:
                return {'process_updated': False, 'message': e}
        else:
            return {
                'process_updated': False,
                'message': 'Process does not exist for hash: %s' % process_hash
            }

    @classmethod
    def search_all_processes(cls, process_hash):
        vend_import_process = Search(using=cls.client, index=cls.vend_index).query('match', _id=process_hash)

        output = []

        response = vend_import_process.execute()
        if not response.hits.total:
            return []
        else:
            for hit in response:
                output.append({'hash': hit.meta.id,
                               'index': hit.meta.index})

        return output


class GetVendImportProcessLog(object):
    vend_index = elasticsearch_connection_url.get('index_vend', '') + '*'
    client = elastic_conn

    @classmethod
    def get_process_by_hash(cls, company_id, process_hash):
        """
        :param company_id: id of company for which processes are queried
        :param process_hash: uuid of specific import
        :return: array of JSON objects with results if results exist
        """
        vend_import_process = (Search(using=cls.client, index=cls.vend_index).query(
            'match', company_id=int(company_id)).query('match', _id=process_hash)
        )

        output = []

        response = vend_import_process.execute()
        from dateutil import parser
        if response.hits.total > 0:
            for hit in response:

                try:
                    output.append(
                        {
                            'process_type': hit.import_type,
                            'status': hit.status,
                            'process_request_type': hit.import_request_type,
                            'created_at':  parser.parse(hit.created_at).strftime("%Y-%m-%d %H:%M:%S"),
                            'process_history': [
                                {
                                    'message': convert_string_to_json(x['data_process_message']),
                                    'status': x['data_process_status'],
                                    'date': parser.parse(x['data_process_created_at']).strftime("%Y-%m-%d %H:%M:%S")
                                } for x in hit.import_data_process
                            ],

                            'process_cloud': [
                                {
                                    'message': convert_string_to_json(x['cloud_process_message']),
                                    'status': x['cloud_process_status'],
                                    'date': parser.parse(x['cloud_process_created_at']).strftime("%Y-%m-%d %H:%M:%S")

                                } for x in hit.import_data_cloud
                            ]
                        }
                    )
                except Exception as e:
                    logger_api.error('Elastic search could not find specific process: ' +str(e))

            return {
                'status': True,
                'results': output,
                'message': 'Data success collected.'
            }
        else:
            return {
                'status': False,
                'results': output,
                'message': 'No data for company: %s with hash: %s' % (
                    str(company_id), process_hash
                )
            }

    @classmethod
    def get_process_by_company_id(cls, company_id):
        """
        :param company_id: id of company for which processes are queried
        :return: array of JSON objects with results if results exist
        """
        vend_import_process = Search(using=cls.client, index=cls.vend_index).query('match', company_id=company_id)

        output = []

        response = vend_import_process.execute()
        if not response.hits.total:
            return {'status': False,
                    'results': output,
                    'message': 'No data for company with id: %d' % (
                        company_id)
                    }
        else:
            for hit in response:
                output.append({
                    'import_type': hit.import_type,
                    'import_request_type': hit.import_request_type,
                    'status': hit.status,
                    'company_name': hit.company_name,
                    'created_at': hit.created_at,
                    'updated_at': hit.updated_at,
                    'import_data_process': [
                        {
                            'process_type': x['data_process'],
                            'process_request_type': x['data_process_type'],
                            'message': convert_string_to_json(x['data_process_message']),
                            'status': x['data_process_status'],
                            'date': x['data_process_created_at'].strftime("%Y-%m-%d %H:%M:%S")

                        } for x in hit.import_data_process
                    ]})

        return {'status': True,
                'results': output,
                'message': 'Data returned successfully!'}

    @classmethod
    def get_process_by_vend_type(cls, company_id, vend_type):
        """
        :param company_id: id of company for which processes are queried
        :param vend_type: type of vend import(e.g. CPI, DJ etc.)
        :return: array of JSON objects with results if results exist
        """
        vend_import_process = Search(
            using=cls.client,
            index=cls.vend_index).query('match', company_id=company_id).query('match', import_type=vend_type)

        output = []

        response = vend_import_process.execute()
        if not response.hits.total:
            return {'status': False,
                    'results': output,
                    'message': 'No data for company %d and import type %s' % (
                     company_id, vend_type)
                    }
        else:
            for hit in response:
                output.append({
                    'import_type': hit.import_type,
                    'import_request_type': hit.import_request_type,
                    'status': hit.status,
                    'company_name': hit.company_name,
                    'created_at': hit.created_at,
                    'updated_at': hit.updated_at,
                    'import_data_process': [
                        {
                            'process_type': x['data_process'],
                            'process_request_type': x['data_process_type'],
                            'message': convert_string_to_json(x['data_validation_message']),
                            'status': x['data_validation_status'],
                            'date': x['data_validation_created_at'].strftime("%Y-%m-%d %H:%M:%S")

                        } for x in hit.import_data_process
                    ]})

        return {'status': True,
                'results': output,
                'message': 'Data returned successfully!'}

    @classmethod
    def get_process_by_date_range(cls, company_id, vend_type, date_start, date_end):
        """
        :param company_id: id of company for which processes are queried
        :param vend_type: type of vend import(e.g. CPI, DJ etc.)
        :param date_start: starting date of queried range
        :param date_end: ending date of queried range
        :return: array of JSON objects with results if results exist
        """
        vend_import_process = Search(
            using=cls.client, index=cls.vend_index).query(
            'match', company_id=company_id).query('match', import_type=vend_type).query(
            'range', created_at={'gte': date_start, 'lte': date_end})

        output = []

        response = vend_import_process.execute()
        if not response.hits.total:
            return {'status': False,
                    'results': output,
                    'message': 'No data in range: %s - %s' % (
                        date_start, date_end)
                    }
        else:
            for hit in response:
                output.append({
                    'import_type': hit.import_type,
                    'import_request_type': hit.import_request_type,
                    'status': hit.status,
                    'company_name': hit.company_name,
                    'created_at': hit.created_at,
                    'updated_at': hit.updated_at,
                    'import_data_process': [
                        {
                            'process_type': x['data_process'],
                            'process_request_type': x['data_process_type'],
                            'message': convert_string_to_json(x['data_validation_message']),
                            'status': x['data_validation_status'],
                            'date': x['data_validation_created_at'].strftime("%Y-%m-%d %H:%M:%S")

                        } for x in hit.import_data_process
                    ]})

        return {'status': True,
                'results': output,
                'message': 'Data returned successfully!'}
