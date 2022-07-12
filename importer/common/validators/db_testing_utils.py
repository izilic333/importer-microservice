from database.cloud_database.common.common import ConnectionForDatabases
from database.company_database.core.query_history import \
    CompanyHistory, CompanyFailHistory
from database.company_database.models.models import \
    cloud_company_process_fail_history, cloud_company_history


def check_history_data(company_id):
    history = CompanyHistory.get_history_by_company_id(company_id)
    fail_history = CompanyFailHistory.get_fail_history_by_company(company_id)
    result = history['status'] or fail_history['status']
    return result


def clear_db():
    conn_local = ConnectionForDatabases.get_local_connection()
    d = cloud_company_process_fail_history.delete()
    conn_local.execute(d)
    d2 = cloud_company_history.delete()
    conn_local.execute(d2)
