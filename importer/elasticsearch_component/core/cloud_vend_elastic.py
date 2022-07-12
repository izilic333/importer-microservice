from elasticsearch_dsl.exceptions import ElasticsearchDslException
from elasticsearch_component.connection.connection import elastic_conn
from elasticsearch_component.core.mixin import convert_string_to_json
from elasticsearch_dsl import Search
from common.mixin.enum_errors import UserEnum

from common.urls.urls import elasticsearch_connection_url


class CloudVendImportQuery(object):
    """
    Class for holding methods that return vend import history.
    This is used for returning messages to the cloud.
    Vend default index is based on ElasticSearch configuration.
    """

    vend_index = elasticsearch_connection_url.get('index_vend', '') + '*'
    client = elastic_conn

    @classmethod
    def get_process_hash_index(cls, process_hash, company_id):
        try:
            search = Search(using=cls.client, index=cls.vend_index)\
                     .query('match', company_id=company_id)\
                     .query('match', _id=process_hash)
        except ElasticsearchDslException as e:
            return {'status': False, 'results': [], 'message': 'Error: %s' % e}
        else:
            output = []

            response = search.execute()

            if not response.hits.total:
                return {'status': False,
                        'results': output,
                        'message': 'No vend import data for company: %s with hash: %s' %
                                   (str(company_id), process_hash)
                        }

            for hit in response:
                output.append({
                    'import_type': hit.import_type,
                    'status': hit.status,
                    'company_name': hit.company_name,
                    'created_at': hit.created_at,
                    'updated_at': hit.updated_at,
                    'process_cloud_insert': [
                        {
                            'process_type': x['data_process'],
                            'process_request_type': x['data_process_type'],
                            'message': convert_string_to_json(x['data_process_message']),
                            'status': x['data_process_status'],
                            'date': x['data_process_created_at'].strftime("%Y-%m-%d %H:%M:%S")

                        } for x in hit.import_data_process if x.data_process_type == UserEnum.USER.value
                    ]
                })

            return {'status': True,
                    'results': output,
                    'message': 'Data returned successfully!'}

    @classmethod
    def get_process_company_id(cls, company_id):
        try:
            search = Search(using=cls.client, index=cls.vend_index) \
                .query('match', company_id=company_id)
        except ElasticsearchDslException as e:
            return {'status': False, 'results': [], 'message': 'Error: %s' % e}
        else:
            output = []

            response = search.execute()

            if not response.hits.total:
                return {'status': False,
                        'results': output,
                        'message': 'No vend import data for company: %s' %
                                   (str(company_id))
                        }

            for hit in response:
                output.append({
                    'import_type': hit.import_type,
                    'status': hit.status,
                    'company_name': hit.company_name,
                    'created_at': hit.created_at,
                    'updated_at': hit.updated_at,
                    'process_cloud_insert': [
                        {
                            'process_type': x['data_process'],
                            'process_request_type': x['data_process_type'],
                            'message': convert_string_to_json(x['data_process_message']),
                            'status': x['data_process_status'],
                            'date': x['data_process_created_at'].strftime("%Y-%m-%d %H:%M:%S")

                        } for x in hit.import_data_process if x.data_process_type == UserEnum.USER.value
                    ]
                })

            return {'status': True,
                    'results': output,
                    'message': 'Data returned successfully!'}

    @classmethod
    def get_process_company_id_vend_type(cls, company_id, vend_type):
        try:
            search = Search(
                using=cls.client,
                index=cls.vend_index).query('match', company_id=company_id).query('match', import_type=vend_type)

        except ElasticsearchDslException as e:
            return {'status': False, 'results': [], 'message': 'Error: %s' % e}
        else:
            output = []

            response = search.execute()

            if not response.hits.total:
                return {'status': False,
                        'results': output,
                        'message': 'No vend import data for company: %s with type: %s' %
                                   (str(company_id), vend_type)
                        }

            for hit in response:
                output.append({
                    'import_type': hit.import_type,
                    'status': hit.status,
                    'company_name': hit.company_name,
                    'created_at': hit.created_at,
                    'updated_at': hit.updated_at,
                    'process_cloud_insert': [
                        {
                            'process_type': x['data_process'],
                            'process_request_type': x['data_process_type'],
                            'message': convert_string_to_json(x['data_process_message']),
                            'status': x['data_process_status'],
                            'date': x['data_process_created_at'].strftime("%Y-%m-%d %H:%M:%S")

                        } for x in hit.import_data_process if x.data_process_type == UserEnum.USER.value
                    ]
                })

            return {'status': True,
                    'results': output,
                    'message': 'Data returned successfully!'}

    @classmethod
    def get_process_date_range(cls, company_id, vend_type, date_start, date_end):
        try:
            search = Search(
                using=cls.client,
                index=cls.vend_index).query(
                'match', company_id=company_id).query(
                'match', import_type=vend_type).query('range', created_at={'gte': date_start, 'lte': date_end})

        except ElasticsearchDslException as e:
            return {'status': False, 'results': [], 'message': 'Error: %s' % e}
        else:
            output = []

            response = search.execute()

            if not response.hits.total:
                return {'status': False,
                        'results': output,
                        'message': 'No data in range: %s - %s' % (
                            date_start, date_end)
                        }

            for hit in response:
                output.append({
                    'import_type': hit.import_type,
                    'status': hit.status,
                    'company_name': hit.company_name,
                    'created_at': hit.created_at,
                    'updated_at': hit.updated_at,
                    'process_cloud_insert': [
                        {
                            'process_type': x['data_process'],
                            'process_request_type': x['data_process_type'],
                            'message': convert_string_to_json(x['data_process_message']),
                            'status': x['data_process_status'],
                            'date': x['data_process_created_at'].strftime("%Y-%m-%d %H:%M:%S")

                        } for x in hit.import_data_process if x.data_process_type == UserEnum.USER.value
                    ]
                })

            return {'status': True,
                    'results': output,
                    'message': 'Data returned successfully!'}