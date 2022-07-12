"""

    This scipt will server only for company setup with monthly calculate API usage.

"""
from time import gmtime, strftime

from common.mixin.enum_errors import EnumAPIType, LimitRequest
from sqlalchemy import func, and_, insert, update
from sqlalchemy.sql import select
from database.cloud_database.common.common import get_local_connection_safe
from database.company_database.models.models import cloud_company, company_statistic

from common.logging.setup import logger


class CompanyMonthlyAPIUsage(object):

    @classmethod
    def insert_or_update_company_api_usage(cls, company_id, api_type, api_name):
        # Company company
        with get_local_connection_safe() as conn_local:

            qv_company = select([cloud_company]).where(cloud_company.c.company_id == company_id)
            ex_qv_company = conn_local.execute(qv_company)

            if not ex_qv_company.rowcount:
                return {'success': False, 'message': "Company not found for API statistic."}

            company_id = ex_qv_company.fetchone().company_id

            # Check if we have any results in table
            date_now = strftime("%Y-%m-%d", gmtime())

            q_check_insert = (
                select([company_statistic])
                .where(
                    and_(
                        company_statistic.c.company_id == company_id,
                        company_statistic.c.api_type == api_type,
                        company_statistic.c.api_name == api_name,
                        func.date(company_statistic.c.date_statistic) == date_now
                    )
                )
            )

            ex_q_check_insert = conn_local.execute(q_check_insert)

            update_result = False

            if ex_q_check_insert.rowcount:
                update_result = True

            # If create and not update
            if not update_result:
                prepare_dict = {
                    'company_id': company_id,
                    'api_type': api_type,
                    'api_name': api_name,
                    'api_count': 1,
                    'date_statistic': date_now
                }
                q_insert = insert(company_statistic).values(prepare_dict)
                try:
                    conn_local.execute(q_insert)
                    conn_local.close()
                    return  {'success': True, 'message': 'Query inserted.'}
                except ValueError as e:
                    conn_local.close()
                    logger.error(
                        'Cant insert company statistic. Company id: {} Error: {}'.format(company_id,e)
                    )
                    return {'success': False, 'message': e}

            else:
                query_results = ex_q_check_insert.fetchone()
                counter = query_results.api_count + 1
                api_request_type = query_results.api_type

                if api_request_type == EnumAPIType.GET.name:
                    if query_results.api_count == LimitRequest.LIMIT_GET.value:
                        return {
                            'success': False, 'message': "Daily limit for API is closed on {}"
                            .format(api_name)
                        }

                instance = query_results.id
                # Now update results
                qv_update_instance = update(company_statistic).where(
                    company_statistic.c.id == instance
                ).values({'api_count':counter})

                try:
                    conn_local.execute(qv_update_instance)
                    return {'success': True, 'message': 'Query updated.'}
                except ValueError as e:
                    logger.error(
                        'Cant update company statistic. Company id: {} Error: {}'.format(company_id, e)
                    )
                    return {'success': False, 'message': e}

    @classmethod
    def return_company_api_status(cls, company_id, date, api_type=str, api_name=str):
       pass