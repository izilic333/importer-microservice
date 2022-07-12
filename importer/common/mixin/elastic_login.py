from common.mixin.handle_file import get_import_type
from database.company_database.core.query_history import CompanyFailHistory
from elasticsearch_component.core.logger import CompanyProcessLogger


"""

    This class will serve only for inserting history for loggers.
    What you need about to know is any ENUM must be STRING in Elastic,
    and any ENUM in local database must be integer.

"""


class ElasticCloudLoginFunctions(object):

    # Create new elastic process
    @classmethod
    def create_process(cls, company_id, import_type, process_request_type):
        elastic = CompanyProcessLogger.create_new_process(
            company_id=company_id,
            process_type=get_import_type(import_type),
            process_request_type=process_request_type,
        )
        return str(elastic['id'])

    # Create flow for our internal purpose
    @classmethod
    def create_process_flow(cls, hash, error, message):
        CompanyProcessLogger.create_process_flow(hash, message, error)
        return

    # Create flow for CLOUD
    @classmethod
    def create_cloud_process_flow(cls, hash, error, message):
        CompanyProcessLogger.create_cloud_process_flow(hash, error, message)
        return

    # Update main elastic process
    @classmethod
    def update_main_process(cls, hash, error):
        CompanyProcessLogger.update_main_process(hash, error)
        return

    # Update database with error flow
    @classmethod
    def update_local_database_error(cls, company_id, import_type, hash, data_hash='', file_path='',
                                    error=None, token=None):
        CompanyFailHistory.insert_fail_history(
            company_id, import_type, hash, data_hash, file_path, error, token
        )
        return

