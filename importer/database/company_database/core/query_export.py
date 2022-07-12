import csv
import pandas
import time
from time import gmtime, strftime
from common.email.send_email import send_email_error_on_file_parse
from database.cloud_database.core.query import (LocationQueryOnCloud, RegionQueryOnCloud, PackingsQueryOnCloud,
                                                ClientQueryOnCloud, MachineQueryOnCloud, MachineTypeQueryOnCloud,
                                                ProductQueryOnCloud, PlanogramQueryOnCloud, UserQueryOnCloud)
from elasticsearch_component.core.query_company import GetCompanyProcessLog
from sqlalchemy import desc, and_, func, update
from sqlalchemy.sql import insert, select

from common.mixin.enum_errors import return_enum_error_name
from common.mixin.mixin import make_response, generate_json
from common.mixin.validation_const import (return_import_type_name, return_import_type_id,
    return_import_object_type, ImportType, get_import_type_by_id)
from common.logging.setup import logger, export_csv_path, export_email_path, vend_logger

from database.cloud_database.common.common import ConnectionForDatabases
from database.company_database.models.models import (company_export_history, cloud_company_history,
                                                     cloud_company_process_fail_history, vend_fail_history,
                                                     vend_success_history)


class ExportHistoryQuery(object):

    @classmethod
    def return_file_path(cls, id):
        conn_local = ConnectionForDatabases.get_local_connection()
        history_query = select([company_export_history]).where(
            company_export_history.c.id == id
        )

        result = conn_local.execute(history_query)
        if not result.rowcount:
            return make_response(False, [], 'No file.')

        files = result.fetchone()

        return make_response(True, {'file_path': files.file_path}, 'File found.')

    @classmethod
    def get_all_history_export_log(cls, company_id):
        conn_local = ConnectionForDatabases.get_local_connection()
        date_now = time.strftime("%Y-%m-%d")
        history_query = select([company_export_history]).where(
            and_(
                company_export_history.c.company_id == company_id,
                company_export_history.c.deleted.is_(False),
                func.date(company_export_history.c.created_at) == date_now
            )
        )

        result = conn_local.execute(history_query)
        if not result.rowcount:
            return make_response(False, [], 'No export history found.')

        history = result.fetchall()

        export_data = []
        for x in history:
            export_typ = 'ALL' if int(x['export_type']) == 0 else return_import_type_name(
                int(x['export_type']))
            export_data.append({
                'id': str(x['id']),
                'company_id': x['company_id'],
                'hash': x['query_hash'],
                'file_path': x['file_path'],
                'user_full_name': x['full_name'],
                'user_id': x['user_id'],
                'export_type': export_typ,
                'created_at': x['created_at'].strftime("%Y-%m-%d %H:%M:%S"),
                'updated_at': x['updated_at'].strftime("%Y-%m-%d %H:%M:%S")
            })

        return make_response(True, export_data, 'Export history found.')

    @classmethod
    def get_history_by_hash(cls, hash):
        conn_local = ConnectionForDatabases.get_local_connection()

        date_now = time.strftime("%Y-%m-%d")

        history_query = select([company_export_history]).where(
            and_(
                company_export_history.c.query_hash == hash,
                func.date(cloud_company_history.c.created_at) == date_now
            )
        )

        result = conn_local.execute(history_query)
        history = result.fetchone()

        if history and not history.exported:
            return make_response(False, [], 'No export history found.')
        elif not history or history.exported:
            return make_response(True, [], 'Export can start.')

    @classmethod
    def insert_history_export(cls, data, path, export_type, json_data):
        conn_local = ConnectionForDatabases.get_local_connection()

        qv_check = select([company_export_history]).where(
            and_(
                company_export_history.c.company_id == int(data['company_id']),
                company_export_history.c.export_type==export_type,
                func.date(company_export_history.c.created_at) == time.strftime("%Y-%m-%d"),
                company_export_history.c.query_hash==data['export_hash']
            )

        )
        check_inserted = conn_local.execute(qv_check)
        if not check_inserted.rowcount:
            insert_query = insert(company_export_history).values(
                company_id=int(data['company_id']),
                query_hash=data['export_hash'],
                file_path=path,
                full_name=data['user_name'],
                user_id=int(data['user_id']),
                export_type = export_type,
                exported=True,
                export_data=json_data
            )
            result = conn_local.execute(insert_query)

            if result.is_insert:
                logger.info('Result is inserted: {}'.format(data))
                return make_response(True, [], "History inserted.")

            logger.error('History inserted: {}'.format(data))
            return make_response(False, [], "Failed to insert new export history!")
        else:
            import datetime
            upd = update(company_export_history).where(
                and_(
                company_export_history.c.company_id == int(data['company_id']),
                company_export_history.c.export_type == export_type,
                func.date(company_export_history.c.created_at) == time.strftime("%Y-%m-%d"),
                company_export_history.c.query_hash == data['export_hash']
                )
            ).values({'updated_at':datetime.datetime.now(), 'export_data':json_data})
            conn_local.execute(upd)
        return make_response(False, [], "History is exported already, and started again!")


class ExportEmailHistory(object):

    @staticmethod
    def create_file_name(export_data, type):
        date = strftime("%Y-%m-%d", gmtime())
        name = return_import_type_name(int(type))
        file_path = export_email_path+'/'+export_data+'_'+name.lower()+'_'+date+'.csv'

        return file_path

    @classmethod
    def export_daily(cls, data):


        date = strftime("%Y-%m-%d", gmtime())

        def send_email_after_generated_request(file_path, emails, import_type, date_exp):
            send_email_error_on_file_parse(file_path, emails, import_type, date_exp)


        def generate_query(file_path, company_id, category_import, email):
            conn_local = ConnectionForDatabases.get_local_connection().connect()

            history_success = select([cloud_company_history]).where(
                and_(cloud_company_history.c.company_id == company_id,
                     func.date(cloud_company_history.c.created_at) == date,
                     cloud_company_history.c.import_type == category_import
                     )
            ).order_by(desc(cloud_company_history.c.created_at))

            out_success_data = conn_local.execute(history_success)


            # Success history
            import_type = return_import_type_name(category_import)

            if out_success_data.rowcount:
                out_fail_results = out_success_data.fetchall()
                open_file = open(file_path, 'a+')
                writer = csv.writer(open_file, quoting=csv.QUOTE_ALL, delimiter=';')
                writer.writerow(('id', 'import_type', 'status', 'created_at'))
                try:
                    for x in out_fail_results:
                        id = x['elastic_hash']
                        type_resp = 'SUCCESS'
                        created_at =x['created_at'].strftime("%Y-%m-%d %H:%M")
                        writer.writerow((id, type_resp, import_type, created_at))
                finally:
                    open_file.close()


            # Fail history
            history_fail = select([cloud_company_process_fail_history]).where(
                and_(cloud_company_process_fail_history.c.company_id == company_id,
                     func.date(cloud_company_process_fail_history.c.created_at) == date,
                     cloud_company_process_fail_history.c.import_type == category_import)
            ).order_by(desc(cloud_company_process_fail_history.c.created_at))

            out_fail = conn_local.execute(history_fail)

            if out_fail.rowcount:
                out_fail_results = out_fail.fetchall()
                open_file = open(file_path, 'a+')
                writer = csv.writer(open_file, quoting=csv.QUOTE_ALL, delimiter=';')
                try:
                    for x in out_fail_results:
                        id = x['elastic_hash']
                        type_resp = return_enum_error_name(x['import_error_type'])
                        created_at = x['created_at'].strftime("%Y-%m-%d %H:%M")
                        import_type = return_import_type_name(x['import_type'])
                        writer.writerow((id, type_resp, import_type, created_at))
                finally:
                    open_file.close()

            # Send email
            send_email_after_generated_request(file_path, email, import_type, date)

            conn_local.close()

        for x in data:
            company_id = x['company']
            category_import = x['category_import']
            emails = x['email']
            file_path_r = cls.create_file_name(
                (str(x['company'])+'_'+str(x['category_import'])), category_import
            )
            generate_query(file_path_r, company_id, category_import, emails)
            time.sleep(1)


class ExportHistory(object):

    @classmethod
    def call_method_based_on_type(cls, data):
        if data['export_type'] == 'ALL':
            cls.export_all_history(data)
        else:
            cls.export_specific_history(data)

    @classmethod
    def check_results(cls, data):
        conn_local = ConnectionForDatabases.get_local_connection()

        company_id = data['company_id']
        import_type = int(return_import_type_id(data['export_type']))
        date_today = time.strftime("%Y-%m-%d")

        history_success = select([cloud_company_history]).where(
            and_(cloud_company_history.c.company_id == company_id,
                 func.date(cloud_company_history.c.created_at) >= date_today,
                 cloud_company_history.c.import_type == import_type
                 )
        ).order_by(desc(cloud_company_history.c.created_at))

        out_success_data = conn_local.execute(history_success)

        if out_success_data.rowcount == 0:
            return False

        return True

    @classmethod
    def export_all_history(cls, data):
        conn_local = ConnectionForDatabases.get_local_connection()

        company_id = data['company_id']
        date_today = time.strftime("%Y-%m-%d")

        # Generate hash for file
        file_path = export_csv_path+'/'+str(data['export_hash'])+'.csv'

        # Data for CSV write
        prepare_data_for_csv = []

        # Execute fail history
        history_fail = select([cloud_company_process_fail_history]).where(
            and_(cloud_company_process_fail_history.c.company_id == company_id,
                 func.date(cloud_company_process_fail_history.c.created_at) >= date_today,
                 )
        ).order_by(desc(cloud_company_process_fail_history.c.created_at))

        out_fail = conn_local.execute(history_fail)

        if out_fail.rowcount:
            out_fail_results = out_fail.fetchall()
            for x in out_fail_results:
                prepare_data_for_csv.append({
                    'id': x['elastic_hash'],
                    'type': return_enum_error_name(x['import_error_type']),
                    'import_type': return_import_type_name(x['import_type']),
                    'created_at': x['created_at'].strftime("%Y-%m-%d %H:%M")
                })

        history_success = select([cloud_company_history]).where(
            and_(cloud_company_history.c.company_id == company_id,
                 cloud_company_history.c.cloud_inserted == True)
        ).order_by(desc(cloud_company_history.c.created_at))

        out_success_data = conn_local.execute(history_success)

        if out_success_data.rowcount:
            out_success_results = out_success_data.fetchall()
            for x in out_success_results:
                prepare_data_for_csv.append({
                    'id': x['elastic_hash'],
                    'type': 'SUCCESS',
                    'import_type': return_import_type_name(x['import_type']),
                    'created_at': x['created_at'].strftime("%Y-%m-%d %H:%M")
                })

        if len(prepare_data_for_csv):
            logger.info('<<< Start to making file: {}'.format(file_path))
            open_file = open(file_path, 'w+')
            try:
                writer = csv.writer(open_file, quoting=csv.QUOTE_ALL, delimiter=';')
                writer.writerow(('ID', 'Import type', 'Status', 'Created at'))
                for x in prepare_data_for_csv:
                    writer.writerow((x['id'], x['import_type'], x['type'], x['created_at']))

            finally:
                logger.info('>>> File closed and added to database. {}'.format(file_path))
                open_file.close()

            # Now call database logger and insert data
            #ExportHistoryQuery.insert_history_export(data, file_path, 0)

    @classmethod
    def export_specific_hash(cls, data):
        conn_local = ConnectionForDatabases.get_local_connection()
        company_id = data['company_id']
        elastic_hash = data['hash']
        emails = data['email']
        import_type = data['import_type']

        # Generate hash for file
        file_path = export_csv_path + '/' + str(data['hash']) + '.csv'

        # Data for CSV write
        prepare_data_for_csv = []

        # Execute fail history
        history_fail = select([cloud_company_process_fail_history]).where(
            and_(cloud_company_process_fail_history.c.company_id == company_id,
                 cloud_company_process_fail_history.c.elastic_hash == elastic_hash
                 )
        ).order_by(desc(cloud_company_process_fail_history.c.created_at))

        out_fail = conn_local.execute(history_fail)

        if out_fail.rowcount:
            out_fail_results = out_fail.fetchall()
            for x in out_fail_results:
                prepare_data_for_csv.append({
                    'id': x['elastic_hash'],
                    'type': return_enum_error_name(x['import_error_type']),
                    'import_type': return_import_type_name(x['import_type']),
                    'created_at': x['created_at'].strftime("%Y-%m-%d %H:%M"),
                    'elastic_query': GetCompanyProcessLog.get_process_by_hash_only_main_peace(
                        company_id, elastic_hash
                    )
                })

        history_success = select([cloud_company_history]).where(
            and_(cloud_company_history.c.company_id == company_id,
                 cloud_company_history.c.elastic_hash == elastic_hash,
                 cloud_company_history.c.cloud_inserted == True
                 )
        ).order_by(desc(cloud_company_history.c.created_at))

        out_success_data = conn_local.execute(history_success)

        if out_success_data.rowcount:
            out_success_results = out_success_data.fetchall()
            for x in out_success_results:
                prepare_data_for_csv.append({
                    'id': x['elastic_hash'],
                    'type': 'SUCCESS',
                    'import_type': return_import_type_name(x['import_type']),
                    'created_at': x['created_at'].strftime("%Y-%m-%d %H:%M"),
                    'elastic_query' : GetCompanyProcessLog.get_process_by_hash_only_main_peace(
                        company_id, elastic_hash
                    ),
                    'statistics': x['statistics']
                })


        if len(prepare_data_for_csv):
            logger.info('<<< Start to making file: {}'.format(file_path))
            open_file = open(file_path, 'w+')
            try:
                writer = csv.writer(open_file, quoting=csv.QUOTE_ALL, delimiter=';')
                writer.writerow(('import_id', 'import_name', 'import_type', 'status', 'statistic', 'created_at'))
                for x in prepare_data_for_csv:
                    sta = x.get('statistics', None)
                    if not sta and not len(x['elastic_query']['results']):
                        writer.writerow(
                            (
                                x['id'],
                                x['import_type'],
                                '',
                                x['type'], '-', x['created_at']
                            )
                        )
                    elif not sta and len(x['elastic_query']['results']) > 0:
                        writer.writerow(
                            (
                                x['id'],
                                x['import_type'], x['elastic_query']['results'][0]['process_request_type'],
                                x['type'], '-' ,x['created_at']
                            )
                        )
                    else:
                        inserted = x['statistics']['inserted_count']
                        updated = x['statistics']['updated_count']
                        deleted = x['statistics']['deleted_count']
                        string_to_insert = (
                            'Inserted: {} Updated: {} Deleted: {}'.format(inserted, updated, deleted)
                        )
                        writer.writerow(
                            (
                                x['id'],
                                x['import_type'],
                                x['elastic_query']['results'][0]['process_request_type'],
                                x['type'], string_to_insert, x['created_at']
                            )
                        )

            finally:
                logger.info('>>> File closed and added to database. {}'.format(file_path))
                open_file.close()

        # Now call email to send
        date = strftime("%Y-%m-%d", gmtime())
        send_email_error_on_file_parse(file_path, emails, import_type, date)

    @classmethod
    def vend_export_specific_hash(cls, data):
        conn_local = ConnectionForDatabases.get_local_connection()
        company_id = data['company_id']
        elastic_hash = data['hash']
        emails = data['email']
        import_type = data['import_type']
        file_path = export_csv_path + '/' + str(data['hash']) + '.csv'
        prepare_data_for_csv = []
        history_fail = select([vend_fail_history]).where(
            and_(vend_fail_history.c.company_id == company_id,
                 vend_fail_history.c.main_elastic_hash == elastic_hash
                 )
        ).order_by(desc(vend_fail_history.c.created_at))

        out_fail = conn_local.execute(history_fail)

        if out_fail.rowcount:
            out_fail_results = out_fail.fetchall()
            for x in out_fail_results:
                prepare_data_for_csv.append({
                    'id': x['elastic_hash'],
                    'type': return_enum_error_name(x['import_error_type']),
                    'import_type': return_import_type_name(x['import_type']),
                    'created_at': x['created_at'].strftime("%Y-%m-%d %H:%M"),
                    'elastic_query': GetCompanyProcessLog.get_vend_process_by_hash(
                        company_id, elastic_hash
                    )
                })

        history_success = select([vend_success_history]).where(
            and_(vend_success_history.c.company_id == company_id,
                 vend_success_history.c.elastic_hash == elastic_hash,
                 vend_success_history.c.cloud_inserted == True
                 )
        ).order_by(desc(vend_success_history.c.created_at))

        out_success_data = conn_local.execute(history_success)

        if out_success_data.rowcount:
            out_success_results = out_success_data.fetchall()
            for x in out_success_results:
                prepare_data_for_csv.append({
                    'id': x['elastic_hash'],
                    'type': 'SUCCESS',
                    'import_type': return_import_type_name(x['import_type']),
                    'created_at': x['created_at'].strftime("%Y-%m-%d %H:%M"),
                    'elastic_query' : GetCompanyProcessLog.get_vend_process_by_hash(
                        company_id, elastic_hash
                    ),
                    'statistics': x['statistics']
                })
        if len(prepare_data_for_csv):
            vend_logger.info('<<< Start to making file: {}'.format(file_path))
            open_file = open(file_path, 'w+')
            try:
                writer = csv.writer(open_file, quoting=csv.QUOTE_ALL, delimiter=';')
                writer.writerow(('import_id', 'import_name', 'import_type', 'status', 'statistic', 'created_at'))
                for x in prepare_data_for_csv:
                    sta = x.get('statistics', None)
                    if not sta and not len(x['elastic_query']['results']):
                        writer.writerow(
                            (
                                x['id'],
                                x['import_type'],
                                '',
                                x['type'], '-', x['created_at']
                            )
                        )
                    elif not sta and len(x['elastic_query']['results']) > 0:
                        writer.writerow(
                            (
                                x['id'],
                                x['import_type'], x['elastic_query']['results'][0]['process_request_type'],
                                x['type'], '-' ,x['created_at']
                            )
                        )
                    else:
                        inserted = x['statistics']['inserted_count']
                        string_to_insert = (
                            'Inserted: {}'.format(inserted)
                        )
                        writer.writerow(
                            (
                                x['id'],
                                x['import_type'],
                                x['elastic_query']['results'][0]['process_request_type'],
                                x['type'], string_to_insert, x['created_at']
                            )
                        )

            finally:
                vend_logger.info('>>> File closed and added to database. {}'.format(file_path))
                open_file.close()

        # Now call email to send
        date = strftime("%Y-%m-%d", gmtime())
        send_email_error_on_file_parse(file_path, emails, import_type, date)

    @classmethod
    def vend_export_specific_hash_only_fail_history(cls, data):
        conn_local = ConnectionForDatabases.get_local_connection()
        company_id = data['company_id']
        elastic_hash = data['hash']
        emails = data['email']
        import_type = data['import_type']
        message = data['message']
        file_path = export_csv_path + '/' + str(data['hash']) + '.csv'
        prepare_data_for_csv = []

        history_fail = select([vend_fail_history]).where(
            and_(vend_fail_history.c.company_id == company_id,
                 vend_fail_history.c.main_elastic_hash == elastic_hash
                 )
        ).order_by(desc(vend_fail_history.c.created_at))

        out_fail = conn_local.execute(history_fail)
        if out_fail.rowcount:
            out_fail_results = out_fail.fetchall()
            for x in out_fail_results:
                prepare_data_for_csv.append({
                    'id': x['elastic_hash'],
                    'type': return_enum_error_name(x['import_error_type']),
                    'import_type': return_import_type_name(x['import_type']),
                    'created_at': x['created_at'].strftime("%Y-%m-%d %H:%M"),
                    'elastic_query': GetCompanyProcessLog.get_vend_process_by_hash(
                        company_id, elastic_hash
                    )
                })
        if len(prepare_data_for_csv):
            vend_logger.info('<<< Start to making file: {}'.format(file_path))
            open_file = open(file_path, 'w+')
            try:
                writer = csv.writer(open_file, quoting=csv.QUOTE_ALL, delimiter=';')
                writer.writerow(('import_id', 'import_name', 'import_type', 'status', 'statistic', 'created_at'))
                for x in prepare_data_for_csv:

                    string_to_insert = (
                        'message: {}'.format(message)
                    )
                    writer.writerow(
                        (
                            x['id'],
                            x['import_type'],
                            x['elastic_query']['results'][0]['process_request_type'],
                            x['type'],
                            string_to_insert,
                            x['created_at']
                        )
                    )
            finally:
                vend_logger.info('>>> File closed and added to database. {}'.format(file_path))
                open_file.close()
        date = strftime("%Y-%m-%d", gmtime())
        send_email_error_on_file_parse(file_path, emails, import_type, date)

    @classmethod
    def select_and_return_type_export(cls, company_id, import_type_id):

        import_type = get_import_type_by_id(import_type_id)

        order = False
        if import_type == ImportType.MACHINES:
            return MachineQueryOnCloud.export_machines(company_id), 'MACHINES', order

        elif import_type == ImportType.LOCATIONS:
            return LocationQueryOnCloud.export_locations(company_id), 'LOCATIONS', order

        elif import_type == ImportType.REGIONS:
            return RegionQueryOnCloud.export_region(company_id), 'REGIONS', order

        elif import_type == ImportType.MACHINE_TYPES:
            return MachineTypeQueryOnCloud.export_machine_type(company_id), 'MACHINE_TYPE', order

        elif import_type == ImportType.PRODUCTS:
            return ProductQueryOnCloud.export_product(company_id), 'PRODUCTS', order

        elif import_type == ImportType.CLIENTS:
            return ClientQueryOnCloud.export_client(company_id), 'CLIENTS', order
        elif import_type == ImportType.PACKINGS:
            return PackingsQueryOnCloud.export_packing(company_id), 'PACKINGS', order

        elif import_type == ImportType.PLANOGRAMS:
            order = ['planogram_name', 'planogram_id', 'planogram_action', 'multiple_pricelists',
                     'product_warning_percentage', 'component_warning_percentage', 'mail_notification',
                     'column_number', 'recipe_id', 'tags', 'capacity', 'minimum_route_pickup', 'warning', 'fill_rate',
                     'product_id',
                     ]
            planogram_export, company_price_list_definition, product_rotation_group_access = PlanogramQueryOnCloud.export_planogram(company_id)

            if product_rotation_group_access:
                order.append('product_rotation_group_id')

            for x in company_price_list_definition:
                order.insert(len(order), x)

            return planogram_export, ImportType.PLANOGRAMS.name, order,
        elif import_type == ImportType.USERS:
            order = ['user_id', 'first_name', 'last_name', 'email', 'user_role', 'timezone',
                     'phone', 'language', 'service_email_notification',	'service_sms_notification',
                     'service_staff_mobile_app', 'service_staff_mobile_technical_view',
                     'assign_filling_route', 'assign_event']
            return UserQueryOnCloud.export_user(company_id), 'USERS', order

    @classmethod
    def return_all_exports(cls, company_id):
        # Return all data
        mch = MachineQueryOnCloud.export_machines(company_id)
        loc = LocationQueryOnCloud.export_locations(company_id)
        reg = RegionQueryOnCloud.export_region(company_id)
        mch_type = MachineTypeQueryOnCloud.export_machine_type(company_id)
        return mch, loc, reg, mch_type

    @classmethod
    def return_history_by_date(cls, export_type, company_id):
        from datetime import datetime, timedelta
        import itertools

        conn_local = ConnectionForDatabases.get_local_connection().connect()

        date_start = (datetime.now() + timedelta(days=-1)).strftime("%Y-%m-%d")
        date_end = time.strftime("%Y-%m-%d")

        query_start = select([company_export_history]).where(
            and_(
                company_export_history.c.company_id == company_id,
                company_export_history.c.export_type == export_type,
                func.date(company_export_history.c.created_at) == date_start

            )
        )

        query_end = select([company_export_history]).where(
            and_(
                company_export_history.c.company_id == company_id,
                company_export_history.c.export_type == export_type,
                func.date(company_export_history.c.created_at) == date_end

            )
        )

        # Execute both query
        query_results_start = conn_local.execute(query_start)
        query_results_end = conn_local.execute(query_end)

        if query_results_start.rowcount and query_results_end.rowcount:

            sort_dict_start = generate_json(query_results_start.fetchone().export_data)
            sort_dict_end =   generate_json(query_results_end.fetchone().export_data)

            output_data = (
                list(itertools.filterfalse(lambda x: x in sort_dict_start, sort_dict_end))+
                list(itertools.filterfalse(lambda x: x in sort_dict_end, sort_dict_start))
            )
            if len(output_data):
                conn_local.close()
                return {'success': True, 'data': output_data, 'sheet_name': 'DIFF'}
            else:
                conn_local.close()
                return {'success': False, 'data': []}

        else:
            conn_local.close()
            return {'success': False, 'data':[]}

    @classmethod
    def export_specific_history(cls, data):
        company_id = data['company_id']
        import_type = int(return_import_type_id(data['export_type']))
        date_now = time.strftime("%Y-%m-%d")
        logger.info(
            'Export history for company: {} Type: {}. Date: {}'
                .format(company_id, import_type, date_now)
        )
        # Generate hash for file
        file_path = (
            export_csv_path + '/' + '{}_{}_{}.xlsx'.format(
                company_id, data['export_type'], date_now
            )
        )

        # Data for CSV write
        prepare_data_for_csv, name_export, order = cls.select_and_return_type_export(company_id, import_type)
        if len(prepare_data_for_csv):
            logger.info('<<< Start to making file: {}'.format(file_path))
            try:
                data_check = cls.return_history_by_date(import_type, company_id)
                if data_check['success']:
                    df = pandas.DataFrame(prepare_data_for_csv)
                    if name_export == ImportType.PLANOGRAMS.name:
                        if order:
                            df = df[order]
                    ds = pandas.DataFrame(data_check['data'])
                    out_stream = file_path
                    writer = pandas.ExcelWriter(out_stream, engine='xlsxwriter')
                    df.to_excel(writer, sheet_name=name_export, startrow=0, index=False)
                    ds.to_excel(writer, sheet_name=data_check['sheet_name'], startrow=0, index=False)
                    writer.save()
                    writer.close()

                else:
                    df = pandas.DataFrame(prepare_data_for_csv)
                    if order:
                        df = df[order]
                    out_stream = file_path
                    writer = pandas.ExcelWriter(out_stream, engine='xlsxwriter')
                    df.to_excel(writer, sheet_name=name_export, startrow=0, index=False)
                    writer.save()
                    writer.close()
            finally:
                logger.info('>>> File closed and added to database. {}'.format(file_path))
                ExportHistoryQuery.insert_history_export(data, file_path, import_type, prepare_data_for_csv)
                date = strftime("%Y-%m-%d", gmtime())
                send_email_error_on_file_parse(
                    file_path, data['email'], return_import_object_type(import_type), date, delete=False
                )

