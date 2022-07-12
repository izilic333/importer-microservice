from elasticsearch_component.models.models import CompanyProcess
from elasticsearch_dsl.exceptions import ElasticsearchDslException
from elasticsearch_component.core.mixin import convert_string_to_json


class CloudElasticQuery(object):
    """
            Class serve as multiply classmethods that return specific query results on each function.
            Logic is the same but it depends on parameters witch is passed to specific function.

            The sam logic is here and explanation is on file:
            - elasticsearch_component/core/query_company
    """
    @classmethod
    def get_process_hash_only_main_object(cls, company_id, process_hash):
        try:
            cmp_process = (
                CompanyProcess()
                .search()
                .query('match', company_id=int(company_id))
                .query('match', _id=process_hash)
            )
        except ElasticsearchDslException as e:
            return {'status': False, 'results': [], 'message': 'Error: %s' % e}
        else:
            output_results = []

            response = cmp_process.execute()

            if not response.hits.total > 0:
                return {'status': False,
                        'results': output_results,
                        'message': 'No data for company: %s with hash: %s' %
                                   (str(company_id), process_hash)
                        }

            for hit in response:
                output_results.append(
                    {
                        'process_type': hit.process_type,
                        'status': hit.status,
                        'process_request_type': hit.process_request_type,
                        'created_at': hit.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )

            return {'status': True,
                    'results': output_results,
                    'message': 'Data success collected.'
                    }


    @classmethod
    def get_all_history(cls, company_id):
        try:
            cmp_process = (
                CompanyProcess()
                .search()
                .query('match', company_id=int(company_id))
            )
        except ElasticsearchDslException as e:
            return {'status': False, 'results': [], 'message': 'Error: %s' % e}
        else:
            output_results = []
            response = cmp_process.execute()

            if not response.hits.total > 0:
                return {'status': False,
                        'results': output_results,
                        'message': 'No data for company: %s' %
                                   (str(company_id))
                        }

            for hit in response:
                output_results.append(
                    {
                        'process_type': hit.process_type,
                        'status': hit.status,
                        'process_request_type': hit.process_request_type,
                        'created_at': hit.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                        'process_importer': [
                            {
                                'message': convert_string_to_json(x['cloud_message']),
                                'status': x['cloud_status'],
                                'date': x['cloud_process_created_at'].strftime("%Y-%m-%d %H:%M:%S")

                            } for x in hit.process_cloud
                        ]
                    }
                )

            return {'status': True,
                    'results': output_results,
                    'message': 'Data success collected.'
                    }

    @classmethod
    def get_process_hash_full_object(cls, company_id, process_hash):
        try:
            cmp_process = (
                CompanyProcess()
                .search()
                .query('match', company_id=int(company_id))
                .query('match', _id=process_hash)
            )
        except ElasticsearchDslException as e:
            return {'status': False, 'results': [], 'message': 'Error: %s' % e}
        else:
            output_results = []
            response = cmp_process.execute()

            if not response.hits.total > 0:
                return {'status': False,
                        'results': output_results,
                        'message': 'No data for company: %s with hash: %s' %
                                   (str(company_id), process_hash)
                        }

            for hit in response:
                output_results.append(
                    {
                        'process_type': hit.process_type,
                        'status': hit.status,
                        'process_request_type': hit.process_request_type,
                        'created_at': hit.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                        'process_importer': [
                            {
                                'message': convert_string_to_json(x['cloud_message']),
                                'status': x['cloud_status'],
                                'date': x['cloud_process_created_at'].strftime("%Y-%m-%d %H:%M:%S")

                            } for x in hit.process_cloud
                        ]
                    }
                )

            return {'status': True,
                    'results': output_results,
                    'message': 'Data success collected.'
                    }

    @classmethod
    def return_history_by_hash(cls, process_hash):
        try:
            cmp_process = (
                CompanyProcess()
                .search()
                .query('match', _id=process_hash)
            )
        except ElasticsearchDslException as e:
            return {'status': False, 'results': [], 'message': 'Error: %s' % e}
        else:
            output_results = []
            response = cmp_process.execute()

            if not response.hits.total > 0:
                return {'status': False,
                        'results': output_results,
                        'message': 'No data for company with hash: %s' % process_hash
                        }

            for hit in response:
                output_results.append(
                    {
                        'process_type': hit.process_type,
                        'status': hit.status,
                        'process_request_type': hit.process_request_type,
                        'created_at': hit.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                        'process_importer': [
                            {
                                'message': convert_string_to_json(x['cloud_message']),
                                'status': x['cloud_status'],
                                'date': x['cloud_process_created_at'].strftime("%Y-%m-%d %H:%M:%S")

                            } for x in hit.process_cloud
                        ]
                    }
                )

            return {'status': True,
                    'results': output_results,
                    'message': 'Data success collected.'
                    }
