from common.mixin.enum_errors import return_enum_error_name
from common.mixin.mixin import make_response
from common.mixin.validation_const import return_import_type_name, return_import_type_id
from database.cloud_database.common.common import ConnectionForDatabases
from database.company_database.models.models import (cloud_company_process_fail_history,
                                                     cloud_company_history)
from elasticsearch_component.core.cloud_elastic import CloudElasticQuery
from sqlalchemy import select, desc, and_


class CloudHistory(object):
    @staticmethod
    def return_fail_history(company_id, type):
        if type:

            filter_type = return_import_type_id(type)

            history_fail = select([cloud_company_process_fail_history]).where(
                and_(
                    cloud_company_process_fail_history.c.company_id == company_id,
                    cloud_company_process_fail_history.c.import_type == filter_type
                )
            ).order_by(desc(cloud_company_process_fail_history.c.created_at))
            return history_fail
        else:
            history_fail = select([cloud_company_process_fail_history]).where(
                cloud_company_process_fail_history.c.company_id == company_id
            ).order_by(desc(cloud_company_process_fail_history.c.created_at))

            return history_fail

    @staticmethod
    def return_success_history(company_id, type):
        if type:
            filter_type = return_import_type_id(type)
            history_success = select([cloud_company_history]).where(
                and_(
                    cloud_company_history.c.company_id == company_id,
                    cloud_company_history.c.import_type == filter_type
                )
            ).order_by(desc(cloud_company_history.c.created_at))
            return history_success
        else:
            history_success = select([cloud_company_history]).where(
                cloud_company_history.c.company_id == company_id
            ).order_by(desc(cloud_company_history.c.created_at))

            return history_success

    @classmethod
    def retrieve_all_history_fail_and_success(cls, company_id, type=None):
        conn_local = ConnectionForDatabases.get_local_connection().connect()

        result_array = []

        # Retrieve fail history

        out_fail = (
            conn_local.execute(cls.return_fail_history(company_id, type)) if type
            else conn_local.execute(cls.return_fail_history(company_id, type))
        )

        if out_fail.rowcount:
            out_fail_results = out_fail.fetchall()
            for x in out_fail_results:

                elastic = (
                    CloudElasticQuery
                        .get_process_hash_full_object(company_id, x['elastic_hash'])['results']
                )
                if elastic[0]['process_request_type'] == 'FILE':
                    username = '-'

                    result_array.append(
                        {
                            'id': str(x['id']),
                            'import_type': return_import_type_name(x['import_type']),
                            'hash': x['elastic_hash'],
                            'created_at': x['created_at'].strftime("%Y-%m-%d %H:%M"),
                            'error_type': return_enum_error_name(x['import_error_type']),
                            'full_name': username,
                            'elastic': (
                                elastic

                            )
                        }
                    )
                else:
                    username = x['full_name']
                    result_array.append(
                        {
                            'id': str(x['id']),
                            'import_type': return_import_type_name(x['import_type']),
                            'hash': x['elastic_hash'],
                            'created_at': x['created_at'].strftime("%Y-%m-%d %H:%M"),
                            'error_type': return_enum_error_name(x['import_error_type']),
                            'full_name': username,
                            'elastic': (
                                elastic

                            )
                        }
                    )

        out_fail.close()

        # Retrieve success history
        out_success_data = (
            conn_local.execute(cls.return_success_history(company_id, type)) if type
            else conn_local.execute(cls.return_success_history(company_id, type))
        )

        if out_success_data.rowcount:
            out_success_results = out_success_data.fetchall()
            for x in out_success_results:
                elastic = (
                    CloudElasticQuery
                        .get_process_hash_full_object(company_id, x['elastic_hash'])['results']
                )
                if elastic[0]['process_request_type'] == 'FILE':
                    username = '-'
                    result_array.append(
                        {
                            'id': str(x['id']),
                            'error_type': 'SUCCESS',
                            'import_type': return_import_type_name(x['import_type']),
                            'hash': x['elastic_hash'],
                            'created_at': x['created_at'].strftime("%Y-%m-%d %H:%M"),
                            'full_name': username,
                            'elastic': (
                                elastic
                            )

                        }

                    )
                else:
                    username = x['full_name']
                    result_array.append(
                        {
                            'id': str(x['id']),
                            'error_type': 'SUCCESS',
                            'import_type': return_import_type_name(x['import_type']),
                            'hash': x['elastic_hash'],
                            'created_at': x['created_at'].strftime("%Y-%m-%d %H:%M"),
                            'full_name': username,
                            'elastic': (
                                elastic
                            )

                        }

                    )

        out_success_data.close()

        if len(result_array):
            return make_response(
                True, result_array, 'Found history.'
            )

        return make_response(
            False, [], 'History not found.'
        )
