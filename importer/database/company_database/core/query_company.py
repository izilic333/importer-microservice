from common.logging.setup import logger
from database.cloud_database.common.common import get_local_connection_safe, get_cloud_connection_safe
from database.cloud_database.models.models import user, custom_user, users_assigned_companies, company
from database.company_database.models.models import cloud_company
from sqlalchemy.sql import select


class GetCompanyFromDatabase(object):


    @classmethod
    def check_company_assignment(cls, email, company_request_id):

        with get_cloud_connection_safe() as conn_cloud:

            assignment_company = []
            super_admin = False

            # Check if user is super-admin
            super_user = select([user]).where(user.email == email)
            ex_super = conn_cloud.execute(super_user)
            q_super = ex_super.fetchone()

            if not q_super:
                return {'success': False}

            user_id = q_super.id

            if q_super['is_superuser']:
                super_admin = True

            # Get user profile
            usr_q = select([custom_user]).where(custom_user.user_ptr_id == user_id)
            ex_usr_q = conn_cloud.execute(usr_q)

            user_profile = ex_usr_q.fetchone()
            if user_profile.company_id:
                assignment_company.append(int(user_profile.company_id))

            # Check multi-company for user
            qv_multicompany = (
                select([users_assigned_companies])
                .where(users_assigned_companies.customuser_id == user_id)
            )
            ex_multicompany = conn_cloud.execute(qv_multicompany)

            if ex_multicompany.rowcount > 0:
                for x in ex_multicompany.fetchall():
                    logger.info(
                        "Fetch company: {} Company requested {}"
                            .format(x.vendingcompany_id, company_request_id)
                    )
                    assignment_company.append(int(x.vendingcompany_id))

            logger.info(
                "All company for user: {} Company requested {}"
                .format(assignment_company, company_request_id)
            )

        if int(company_request_id) in set(assignment_company) or super_admin:
            return {'success': True}

        return {'success': False}

    @classmethod
    def get_company_by_id(cls, company_id):
        with get_local_connection_safe() as conn_local:
            company_local_query = select([cloud_company]).where(
                cloud_company.c.company_id == company_id
            )
            if conn_local.execute(company_local_query).fetchone():
                return {'success': True}

            return {'success': False}

    @classmethod
    def insert_or_update(cls, args):
        if cls.get_company_by_id(company_id=args['company_id']):
            query = cloud_company.update().values(args)
        else:
            query = cloud_company.insert().values(args)

        with get_local_connection_safe() as conn_local:
            conn_local.execute(query)

    @classmethod
    def routing_microservice_enabled(cls, company_id):
        with get_cloud_connection_safe() as conn_cloud:

            company_query = select([company]).where(company.id == company_id)
            vending_company = conn_cloud.execute(company_query).fetchone()
            return vending_company.route_microservice
