from database.cloud_database.common.common import  get_local_connection_safe
from database.company_database.models.models import company_parameters

from sqlalchemy import select, and_, insert,  update


class CompanyParameters(object):
    @classmethod
    def get_parameter(cls, company_id, key, default=None):
        if key:

            with get_local_connection_safe() as conn_local:

                query = select([company_parameters.c.value]).where(
                    and_(
                        company_parameters.c.company_id == company_id,
                        company_parameters.c.key == key
                        )
                )

                result = conn_local.execute(query)
                if result.rowcount:
                    value = result.fetchall()[0][0]
                else:
                    value = default

                return value
        else:

            return

    @staticmethod
    def set_parameter(company_id, key, value):
        old_value = CompanyParameters.get_parameter(company_id, key)

        with get_local_connection_safe() as conn_local:

            if old_value:
                query = update(company_parameters).\
                    where(and_(company_parameters.c.key==key, company_parameters.c.company_id==company_id)).\
                    values(value=value)
            else:
                query = insert(company_parameters).values(company_id=company_id, key=key, value=value)

            conn_local.execute(query)
            conn_local.close()
            return

    @staticmethod
    def get_all_parameters():
        with get_local_connection_safe() as conn_local:

            query = select([company_parameters])

            parameters = conn_local.execute(query).fetchall()

            config = {}

            for parameter in parameters:
                company_config = config.get(parameter.company_id, {})
                company_config[parameter.key] = parameter.value
                config[parameter.company_id] = company_config

            return config
