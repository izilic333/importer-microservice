from elasticsearch_component.core.mixin import convert_string_to_json
from elasticsearch_component.models.models import CompanyProcess
from elasticsearch_component.models.vend_models import VendImportProcess
from elasticsearch_dsl import ValidationException
from dateutil import parser


class GetCompanyProcessLog(object):
    """
        Class serve as multiply classmethods that return specific query results on each function.
        Logic is the same but it depends on parameters witch is passed to specific function.
    """
    @classmethod
    def get_all_logs_based_on_company_id(cls, company_id):
        """

        :param company_id: company_id is company_id from cloud
        :return: array of JSON object with results if exists on query

        """
        try:
            company_process = (CompanyProcess()
                               .search()
                               .query('match', company_id=int(company_id))
                               )
            response = company_process.execute()
        except ValidationException as e:
            return {'status': False, 'results': [], 'message': 'Error: %s' % e}
        else:
            output_query = []
            if response.hits.total > 0:

                for hit in response:
                    output_query.append(
                        {
                            'id': hit._id,
                            'company_id': hit.company_id,
                            'process_type': hit.process_type,
                            'status': hit.status,
                            'process_request_type': hit.process_request_type,
                            'created_at': hit.created_at.strftime("%Y-%m-%d %H:%M:%S"),
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
                                    'date': x['cloud_process_created_at'].strftime(
                                        "%Y-%m-%d %H:%M:%S")

                                } for x in hit.process_cloud
                            ]
                        }
                    )

                return {'status': True,
                        'results': output_query,
                        'message': 'Data success collected.'
                        }
            else:
                return {'status': False,
                        'results': output_query,
                        'message': 'No data for company: %s' % str(company_id)
                        }

    @classmethod
    def get_logs_based_on_company_id_and_date(cls, company_id, date_start, date_end, process_type):
        """

        :param company_id: company_id is company_id from cloud
        :param date_start: 2017-05-11
        :param date_end:  2017-05-12
        :param process_type: MACHINES, LOCATIONS, ...
        :return: array of JSON object with results if exists on query
        """
        date_start = parser.parse(date_start)
        date_end = parser.parse(date_end)

        def return_query():
            if process_type and len(process_type) > 0:
                query = (CompanyProcess().search()
                         .query('match', company_id=int(company_id))
                         .query('match', process_type=process_type)
                         .query('range', created_at={'gte': date_start, 'lte': date_end})
                         .sort('-created_at')
                         )
                return query
            else:
                query = (CompanyProcess().search()
                         .query('match', company_id=int(company_id))
                         .query('range', created_at={'gte': date_start, 'lte': date_end})
                         .sort('-created_at')
                         )
                return query

        try:
            company_process = return_query()
            response = company_process.execute()
        except ValidationException as e:
            return {'status': False, 'message': 'Error: %s' % e}
        else:
            output_query = []

            if response.hits.total > 0:
                for hit in response:
                    output_query.append(
                        {
                            'id': hit._id,
                            'company_id': hit.company_id,
                            'process_type': hit.process_type,
                            'status': hit.status,
                            'process_request_type': hit.process_request_type,
                            'created_at': hit.created_at.strftime("%Y-%m-%d %H:%M:%S"),
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
                                    'date': x['cloud_process_created_at'].strftime(
                                        "%Y-%m-%d %H:%M:%S")

                                } for x in hit.process_cloud
                            ]
                        }
                    )

                return {'status': True,
                        'results': output_query,
                        'message': 'Data success collected.'
                        }
            else:
                return {'status': False,
                        'results': output_query,
                        'message': 'No data for company: %s' % str(company_id)
                        }

    @classmethod
    def get_logs_based_on_type_and_company(cls, company_id, type):
        """

        :param company_id: company_id is company_id from cloud
        :param type: MACHINES ....
        :return:  array of JSON object with results if exists on query
        """
        try:
            company_process = (CompanyProcess().search()
                               .query('match', company_id=int(company_id))
                               .query('match', process_type=type)

                               )
            response = company_process.execute()
        except ValidationException as e:
            return {'status': False, 'message': 'Error: %s' % e}
        else:
            output_query = []

            if response.hits.total > 0:
                for hit in response:
                    output_query.append(
                        {
                            'id': hit._id,
                            'company_id': hit.company_id,
                            'process_type': hit.process_type,
                            'status': hit.status,
                            'process_request_type': hit.process_request_type,
                            'created_at': hit.created_at.strftime("%Y-%m-%d %H:%M:%S"),
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
                                    'date': x['cloud_process_created_at'].strftime(
                                        "%Y-%m-%d %H:%M:%S")

                                } for x in hit.process_cloud
                            ]
                        }
                    )

                return {'status': True,
                        'results': output_query,
                        'message': 'Data success collected.'
                        }
            else:
                return {'status': False,
                        'results': output_query,
                        'message': 'No data for company: %s' % str(company_id)
                        }

    @classmethod
    def get_process_by_hash_only_main_peace(cls, company_id, process_hash):
        """

            :param company_id: company_id is company_id from cloud
            :param process_hash: uuid of specific import
            :return: array of JSON object with results if exists on query
        """
        company_process = (
            CompanyProcess()
                .search()
                .query('match', company_id=int(company_id))
                .query('match', _id=process_hash)
        )

        output_query = []

        response = company_process.execute()
        if response.hits.total > 0:
            for hit in response:
                output_query.append(
                    {
                        'process_type': hit.process_type,
                        'status': hit.status,
                        'process_request_type': hit.process_request_type,
                        'created_at': hit.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )

            return {'status': True,
                    'results': output_query,
                    'message': 'Data success collected.'
                    }
        else:
            return {'status': False,
                    'results': output_query,
                    'message': 'No data for company: %s with hash: %s' % (
                        str(company_id), process_hash)
                    }

    @classmethod
    def get_vend_process_by_hash(cls, company_id, process_hash):
        """

            :param company_id: company_id is company_id from cloud
            :param process_hash: uuid of specific import
            :return: array of JSON object with results if exists on query
        """
        vend_company_process = (
            VendImportProcess()
                .search()
                .query('match', company_id=int(company_id))
                .query('match', _id=process_hash)
        )

        output_query = []

        response = vend_company_process.execute()
        if response.hits.total > 0:
            for hit in response:
                output_query.append(
                    {
                        'status': hit.status,
                        'process_request_type': 'FILE',
                        'created_at': hit.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )

            return {'status': True,
                    'results': output_query,
                    'message': 'Data success collected.'
                    }
        else:
            return {'status': False,
                    'results': output_query,
                    'message': 'No data for company: %s with hash: %s' % (
                        str(company_id), process_hash)
                    }

    @classmethod
    def get_process_by_hash(cls, company_id, process_hash):
        """

        :param company_id: company_id is company_id from cloud
        :param process_hash: uuid of specific import
        :return: array of JSON object with results if exists on query
        """
        company_process = (
            CompanyProcess()
                .search()
                .query('match', company_id=int(company_id))
                .query('match', _id=process_hash)
        )

        output_query = []

        response = company_process.execute()
        if response.hits.total > 0:
            for hit in response:
                output_query.append(
                    {
                        'process_type': hit.process_type,
                        'status': hit.status,
                        'process_request_type': hit.process_request_type,
                        'created_at': hit.created_at.strftime("%Y-%m-%d %H:%M:%S"),
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

            return {'status': True,
                    'results': output_query,
                    'message': 'Data success collected.'
                    }
        else:
            return {'status': False,
                    'results': output_query,
                    'message': 'No data for company: %s with hash: %s' % (
                        str(company_id), process_hash)
                    }

    @classmethod
    def get_process_by_type(cls, company_id, process_type, process_request):
        """

        :param company_id: company_id is company_id from cloud
        :param process_type: MACHINES
        :param process_request: API, FILE,
        :return: array of JSON object with results if exists on query
        """

        company_process = (
            CompanyProcess()
                .search()
                .query('match', company_id=int(company_id))
                .query('match', process_type=process_type)
                .query('match', process_request_type=process_request)
                .sort('-created_at')
        )

        response = company_process.execute()

        output_query = []
        if response.hits.total > 0:
            for hit in response[0:1]:
                output_query.append(
                    {
                        'id': hit._id,
                        'company_id': hit.company_id,
                        'process_type': hit.process_type,
                        'status': hit.status,
                        'process_request_type': hit.process_request_type,
                        'created_at': hit.created_at.strftime("%Y-%m-%d %H:%M:%S")
                    }
                )
            return {'status': True,
                    'results': output_query,
                    'message': 'Data success collected.'
                    }
        else:
            return {'status': False,
                    'results': output_query,
                    'message': 'No data for company: %s' % str(company_id)
                    }