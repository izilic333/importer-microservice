from common.mixin.validation_const import ImportType
from database.cloud_database.common.common import ConnectionForDatabases, get_cloud_connection_safe, get_local_connection_safe
from sqlalchemy import func, join, insert, and_
from sqlalchemy.sql import select, update
import time

from database.cloud_database.models.models import (
    company, importer_company, importer_company_ftp,
    importer_company_ftp_details, importer_type)
from database.company_database.models.models import cloud_company
from common.mixin.validation_const import return_import_type_name, return_active_vend_import_type


class CloudLocalDatabaseSync(object):
    @classmethod
    def query_cloud_company_initial_insert(cls):

        with get_cloud_connection_safe() as conn_cloud, get_local_connection_safe() as conn_local:
            company_query = select([company])
            result = conn_cloud.execute(company_query)
            company_results = [
                {
                    'company_id': int(x.id),
                    'company_name': x.caption
                }
                for x in result
            ]

            company_local = select([cloud_company])
            run_company = conn_local.execute(company_local)

            company_local_id = [int(x.company_id) for x in run_company.fetchall()]

            for cmp in company_results:
                if int(cmp['company_id']) not in company_local_id:
                    ins = cloud_company.insert().values(cmp)
                    conn_local.execute(ins)

        return

    @classmethod
    def return_company_name(cls, company_id):

        with get_local_connection_safe() as conn_local:

            company_local = select([cloud_company]).where(cloud_company.c.company_id == company_id)

            run_q = conn_local.execute(company_local)
            if not run_q.rowcount:
                return ''

            results = run_q.fetchone()

        return results['company_name']

    @classmethod
    def create_new_local_company(cls, company_id, company_name):

        with get_local_connection_safe() as conn_local:

            company_local = select([cloud_company])
            run_company = conn_local.execute(company_local)

            company_local_id = [x.id for x in run_company.fetchall()]

            if int(company_id) in company_local_id:
                return {
                    'status': False,
                    'message': 'Company already exists in local database. Company id: %s' % company_id
                }
            else:
                prepare_ins = {'company_id': int(company_id), 'company_name': company_name}
                ins = cloud_company.insert().values(prepare_ins)
                conn_local.execute(ins)
                return {
                    'status': True,
                    'message': 'Company inserted. Company id: %s' % company_id
                }

    @classmethod
    def setup_cron_job(cls):

        with get_cloud_connection_safe() as conn_cloud:
            # Get all active company importers
            comp_q = select([importer_company]).where(importer_company.active.is_(True))
            result_q = conn_cloud.execute(comp_q)

            # Active company importer
            activated_importer = [
                {'id': x['id'], 'company': x['vending_company_id']} for x in result_q
            ]

            results = []

            # Get results for company importer settings
            ids = [cm['id'] for cm in activated_importer]

            qv = (
                select([importer_company_ftp])
                .where(
                    and_(
                    importer_company_ftp.active == True,
                    importer_company_ftp.company_importer_id == func.any(ids)
                    )
                )
            )

            def search(id):
                for p in activated_importer:
                    if p['id'] == id:
                        return p['company']

            ex_qv = conn_cloud.execute(qv)
            for res in ex_qv:
                first_q = (
                    join(importer_company_ftp_details, importer_type,
                         importer_company_ftp_details.category_import_id == importer_type.id)
                )

                join_q = select([importer_company_ftp_details]).where(
                        importer_company_ftp_details.vending_company_ftp_id == res['id']
                    ).select_from(first_q).order_by(importer_type.ordering_by)

                qv_out = conn_cloud.execute(join_q)

                for cm in qv_out:
                    results.append({
                        'id': cm['company_importer_id'],
                        'company': search(cm['company_importer_id']),
                        'url': res['url'],
                        'username': res['username'],
                        'user_id': cm['changed_by_id'],
                        'password': res['password'],
                        'port': res['port'],
                        'cron_hour': cm['cron_time_hour'],
                        'cron_min': cm['cron_time_min'],
                        'category_import': cm['category_import_id'],
                        'email': cm['email'],
                        'ftp_path': cm['ftp_path'],
                        'file_delimiters': cm['file_delimiters']

                    })

        if len(results):
            return results
        return

    @classmethod
    def setup_vends_cron_jobs(cls):
        """
        This is importer vend configuration for:
            a) companies that are active on cloud
            b) companies with activated importer
            c) configuration only for vends import

        :return: importer vend configuration
        """

        with get_cloud_connection_safe() as conn_cloud:

            # First get all active company from cloud and get all active company id
            active_company = select([importer_company]).where(importer_company.active.is_(True))
            result_active_company = conn_cloud.execute(active_company)
            activate_company = [
                {'id': x['id'], 'company': x['vending_company_id']} for x in result_active_company
            ]
            company_ids = [company_id['id'] for company_id in activate_company]

            # Get all company with activated importer for active company ids
            get_company_with_active_importer = (
                select([importer_company_ftp]).where(and_(
                    importer_company_ftp.active == True,
                    importer_company_ftp.company_importer_id == func.any(company_ids)
                ))
            )

            # Get all import type from cloud
            import_types = select([importer_type])
            import_types_query = conn_cloud.execute(import_types)
            activated_importer = [
                {'id': x['id'], 'import_name': x['name']} for x in import_types_query
            ]

            def search(id):
                """
                Get company_id by id of company
                :param id: id of company
                :return: company_id
                """
                for p in activate_company:
                    if p['id'] == id:

                        return p['company']
                    else:
                        return None

            # When we have all importer type from cloud, get all import type from importer with active vends import
            main_active_vends_import_type = []
            if activated_importer:
                for x in activated_importer:
                    import_name = x.get('import_name')
                    vend_import_type = return_import_type_name(import_name)
                    if vend_import_type:
                        active_vend_import_type = return_active_vend_import_type(vend_import_type)
                        if active_vend_import_type.get('vend_status'):
                            main_active_vends_import_type.append(active_vend_import_type.get('id'))

            # This is main importer vend configuration
            importer_vends_configuration = []
            importer_configuration_query = conn_cloud.execute(get_company_with_active_importer)
            for result in importer_configuration_query:
                main_active_vend_configurations = select([importer_company_ftp_details]).where(and_(
                        importer_company_ftp_details.vending_company_ftp_id == result['id'],
                        importer_company_ftp_details.category_import_id == func.any(main_active_vends_import_type))
                )

                main_result = conn_cloud.execute(main_active_vend_configurations)

                for x in main_result:
                    importer_vends_configuration.append({
                        'id': x['company_importer_id'],
                        'company': search(x['company_importer_id']),
                        'url': result['url'],
                        'username': result['username'],
                        'user_id': x['changed_by_id'],
                        'password': result['password'],
                        'port': result['port'],
                        'cron_hour': x['cron_time_hour'],
                        'cron_min': x['cron_time_min'],
                        'category_import': x['category_import_id'],
                        'email': x['email'],
                        'ftp_path': x['ftp_path'],
                        'file_delimiters': x['file_delimiters']

                    })

        return importer_vends_configuration

    @classmethod
    def get_company_importer_email(cls, company_id, ftp_import_name, import_type):

        with get_cloud_connection_safe() as conn_cloud:

            query_comp = select([importer_company_ftp]).where(
                func.lower(importer_company_ftp.name) == ftp_import_name.lower() and
                importer_company_ftp.active == True
            )
            result_q = conn_cloud.execute(query_comp)
            if not result_q.fetchone():
                return {
                    'success': False, 'message': 'There is no valid FTP for name: %s' % ftp_import_name
                }

            data = [x['id'] for x in conn_cloud.execute(query_comp)]

            importer_id = data[0]

            # Get details
            q_details = select([importer_company_ftp_details]).where(
                importer_company_ftp_details.vending_company_ftp_id == importer_id
                and str(importer_company_ftp_details.category_import_id) == str(import_type)
            ).distinct(importer_company_ftp_details.category_import_id)

            q_det = conn_cloud.execute(q_details)

            if not q_det.fetchone():
                return {
                    'success': False, 'message': 'There is no valid FTP details provided.'
                }

            get_details = conn_cloud.execute(q_details).fetchone()
            ftp_details = {
                    'email': get_details.email,
                    'file_delimiters': get_details.file_delimiters,
                    'category_import_id': import_type,
                    'company': company_id
            }

        return {
            'success': True, 'message': 'Data is success collected.', 'results': ftp_details
        }

    @classmethod
    def update_main_importer_status_on_cloud(cls):

        with get_cloud_connection_safe() as conn_cloud:

            for x in ImportType:
                values_dict = {
                    'name': x.name,
                    'active': x.value['active'],
                    'ordering_by': int(x.value['order']),
                    'description': x.name
                }
                upd_q = update(importer_type).where(importer_type.id == int(x.value['id'])).values(
                    values_dict
                )
                res = conn_cloud.execute(upd_q)
                if not res.rowcount:
                    ins_q = insert(importer_type).values(**values_dict)
                    res = conn_cloud.execute(ins_q)
                    if not res.inserted_primary_key:
                        print('not inserted')


def run_sync():
    CloudLocalDatabaseSync.query_cloud_company_initial_insert()
    time.sleep(5)
    CloudLocalDatabaseSync.update_main_importer_status_on_cloud()


if __name__ == '__main__':
    """For running sync from command line."""
    run_sync()
