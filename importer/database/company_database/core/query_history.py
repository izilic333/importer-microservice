from common.mixin.enum_errors import return_enum_error_name
from common.mixin.mixin import make_response, custom_hash, custom_hash_vends
from common.mixin.validation_const import (return_import_type_id, return_import_type_name,
    return_import_type_id_custom_validation)
from core.flask.redis_store.redis_managment import RedisManagement
from core.flask.sessions.session import AuthorizeUser
from database.cloud_database.common.common import ConnectionForDatabases, get_local_connection_safe
from database.company_database.models.models import (
    cloud_company_history, cloud_company_process_fail_history,
    vend_success_history, vend_fail_history, vend_device_history
)
from elasticsearch_component.core.query_company import GetCompanyProcessLog
from elasticsearch_component.core.query_vends import GetVendImportProcessLog
from sqlalchemy import desc, and_, func, true, delete, asc
from sqlalchemy.sql import insert, select, update
from sqlalchemy.ext.serializer import dumps
from datetime import datetime, timedelta
import os

class CompanyHistory(object):

    @classmethod
    def get_all_users(cls, company_id):
        conn_local = ConnectionForDatabases.get_local_connection().connect()

        users_init_array = []

        history_success_query = select([cloud_company_history]).where(
            cloud_company_history.c.company_id == company_id
        ).distinct(cloud_company_history.c.full_name)

        result_success = conn_local.execute(history_success_query)

        if result_success.rowcount:
            out = result_success.fetchall()
            for x in out:
                users_init_array.append( {
                   'id': x['user_id'],
                   'full_name': x['full_name']
                    }
                )

        result_success.close()

        history_fail_query = select([cloud_company_process_fail_history]).where(
            cloud_company_process_fail_history.c.company_id == company_id
        ).distinct(cloud_company_process_fail_history.c.full_name)

        result_fail = conn_local.execute(history_fail_query)

        if result_fail.rowcount:
            out = result_fail.fetchall()
            for x in out:
                users_init_array.append(
                    {
                   'id': x['user_id'],
                   'full_name': x['full_name']
                    }
                )

        result_fail.close()


        user_array = []

        for item in users_init_array:
            if item not in user_array:
                user_array.append(item)

        return make_response(
            True, user_array,
            'Found users length'
        )


    @classmethod
    def get_history_by_company_id(cls, company_id):
        conn_local = ConnectionForDatabases.get_local_connection()

        history_query = select([cloud_company_history]).where(
            cloud_company_history.c.company_id == company_id
        ).order_by(desc('updated_at'))

        result = conn_local.execute(history_query)

        if not result.rowcount:
            return make_response(False, [], 'No data found')

        out = result.fetchall()

        user_data = [
            {
                'id': str(x['id']),
                'import_type': return_import_type_name(x['import_type']),
                'hash': x['elastic_hash'],
                'created_at': x['created_at'].strftime("%Y-%m-%d %H:%M"),
                'data_hash': x['data_hash'],
                'file_path': x['file_path'],
                'elastic': (
                    GetCompanyProcessLog
                        .get_process_by_hash_only_main_peace(company_id, x['elastic_hash'])[
                        'results']
                )

            } for x in out
        ]

        return make_response(
            True, user_data,
            'Found process fail history length: {}'.format(result.rowcount)
        )

    @classmethod
    def get_history_by_datetime(cls, company_id, import_type, from_datetime):
        conn_local = ConnectionForDatabases.get_local_connection()

        history_query = select([cloud_company_history]).where(and_(
            cloud_company_history.company_id == company_id,
            import_type == import_type,
            func.date(cloud_company_history.updated_at) >= from_datetime
        )).order_by(desc(cloud_company_history.updated_at))

        result = conn_local.execute(history_query)

        if not result.rowcount:
            return make_response(False, [], 'No data found')

        user_data = [dumps(result)]

        return make_response(
            True, user_data,
            'Found history from datetime: {} data length: {}'.format(
                from_datetime, result.rowcount)
        )

    @classmethod
    def get_history_by_hash(cls, company_id, elastic_hash):
        conn_local = ConnectionForDatabases.get_local_connection()

        history_query = select([cloud_company_history]).where(and_(
            cloud_company_history.c.company_id == company_id,
            cloud_company_history.c.elastic_hash == elastic_hash
        )).order_by(cloud_company_history.c.updated_at)

        result = conn_local.execute(history_query)

        if not result.rowcount:
            return make_response(False, [], 'No data found')

        user_data = [result.first()]

        user_data = [dict(
            id=x.id,
            company_id=x.company_id,
            import_type=x.import_type,
            elastic_hash=x.elastic_hash,
            data_hash=x.data_hash,
            import_data=x.import_data,
            file_path=x.file_path,
            cloud_inserted=x.cloud_inserted,
            full_name=x.full_name,
            user_id=x.user_id,
            created_at=x.created_at,
            updated_at=x.updated_at,
            statistics=x.statistics,
            active_history=x.active_history,
            cloud_results=x.cloud_results,
            partial=x.partial,

        ) for x in user_data]

        return make_response(
            True, user_data,
            'Found history by hash: {} data length: {}'.format(
                elastic_hash, result.rowcount)
        )

    @classmethod
    def insert_history(cls, company_id, import_json, import_type, elastic_hash, data_hash,
                       file_path, token, cloud_inserted=False):
        old_entry = cls.get_history_by_hash(company_id, elastic_hash)
        conn_local = ConnectionForDatabases.get_local_connection()

        if old_entry['status']:
            if old_entry['results'][0]['import_data'] == "csv_validator":
                query = (
                    update(cloud_company_history).where(cloud_company_history.c.elastic_hash == elastic_hash).values(
                        {
                            'import_data': import_json,
                            'file_path': file_path,
                            'data_hash': data_hash,
                        }
                    ).returning(cloud_company_history.c.id)
                )
                conn_local.execute(query)

            return make_response(
                False, [],
                'Fail history with this hash already exists in table {}'.format(
                    old_entry['results'][0]['elastic_hash']
                )
            )

        extract_token = AuthorizeUser.verify_user_token(token.replace('JWT ', ''))

        insert_query = cloud_company_history.insert().values(
            company_id=company_id,
            import_type=return_import_type_id_custom_validation(import_type),
            elastic_hash=elastic_hash,
            data_hash=data_hash,
            import_data=import_json,
            cloud_inserted=cloud_inserted,
            file_path=file_path,
            full_name=extract_token['response']['full_name'],
            user_id=int(extract_token['response']['user_id'])
        ).returning(cloud_company_history.c.id)

        result = conn_local.execute(insert_query)
        all_results = result.fetchone()

        if result.is_insert and all_results.id:
            return make_response(
                True,
                [],
                'Inserted new fail history with primary key {}'.format(all_results.id)
            )

        return make_response(False, [], "Failed to insert new fail history!")

    @classmethod
    def update_history(cls, company_id, elastic_hash, file_path, cloud_inserted=False):
        old_entry = cls.get_history_by_hash(company_id, elastic_hash)
        if old_entry['status']:
            return make_response(
                False, [],
                'Fail history by this hash already exists in table {}'.format(
                    old_entry['results'][0]['elastic_hash']
                )
            )

        history = old_entry['results'][0]

        update_queue = (
            update(cloud_company_history)
            .where(cloud_company_history.c.id == history.id)
            .values(file_path=file_path, cloud_inserted=cloud_inserted)
        )

        conn_local = ConnectionForDatabases.get_local_connection()
        result = conn_local.execute(update_queue).fetchone()

        if result:
            return make_response(
                True,
                [dumps(result)],
                'Inserted new fail history with primary key {}'.format(result.inseted_primary_key)
            )

        return make_response(False, [], "Failed to insert new fail history!")

    @classmethod
    def exists_in_history(cls, company_id, data_hash):
        conn_local = ConnectionForDatabases.get_local_connection()
        query = (
            select([cloud_company_history])
            .where(and_(
                cloud_company_history.c.cloud_inserted == true(),
                cloud_company_history.c.company_id == company_id,
                cloud_company_history.c.data_hash == data_hash
            ))
        )
        result = conn_local.execute(query)
        return result.rowcount > 0, result.first()

    @classmethod
    def exists_in_vend_history(cls, company_id, data_hash):
        conn_local = ConnectionForDatabases.get_local_connection()
        query = (
            select([vend_success_history])
            .where(and_(
                vend_success_history.c.cloud_inserted == true(),
                vend_success_history.c.company_id == company_id,
                vend_success_history.c.data_hash == data_hash
            ))
        )
        result = conn_local.execute(query)
        response1 = result.rowcount > 0
        response2 = result.first()
        return response1, response2

    @classmethod
    def update_local_finish_process(cls, elastic_hash, statistics, statistic_data, partial):
        conn_local = ConnectionForDatabases.get_local_connection()
        query = (
            update(cloud_company_history)
            .where(cloud_company_history.c.elastic_hash == elastic_hash)
            .values(
                {
                    'cloud_inserted': True,
                    'statistics': statistics,
                    'cloud_results': statistic_data,
                    'partial': partial
                }
            )
            .returning(cloud_company_history.c.id)
        )
        result = conn_local.execute(query)

        if result.fetchone():
            return True
        return False

    @classmethod
    def vend_update_local_finish_process(cls, elastic_hash, statistics, statistic_data, partial):
        conn_local = ConnectionForDatabases.get_local_connection()
        query = (
            update(vend_success_history)
            .where(vend_success_history.c.elastic_hash == elastic_hash)
            .values(
                {
                    'cloud_inserted': True,
                    'statistics': statistics,
                    'cloud_results': statistic_data,
                    'partial': partial
                }
            )
            .returning(vend_success_history.c.id)
        )
        result = conn_local.execute(query)

        if result.fetchone():
            return True
        return False

    @classmethod
    def return_local_fail_history(cls, company_id, data_hash):
        conn_local = ConnectionForDatabases.get_local_connection().connect()
        query = (
            select([cloud_company_process_fail_history])
            .where(
                cloud_company_process_fail_history.c.company_id == company_id
            ).order_by(desc(cloud_company_process_fail_history.c.created_at)).limit(3)
        )
        result = conn_local.execute(query)

        total = 0

        ret = {'success': False}

        if result.rowcount:
            out = result.fetchall()
            for x in out:
                if x.data_hash == data_hash:
                    total += 1
            if total >= 3:
                ret['success'] = True
                ret['res'] = out[-1].created_at

        result.close()

        return ret

    @classmethod
    def return_local_vend_fail_history(cls, company_id, data_hash, file_path):
        """
        Checking is specific file already processed!
        :param company_id: company_id
        :param data_hash: data_hash
        :param file_path: fail file_path
        :return: True if exists in local history, False if not.
        """
        conn_local = ConnectionForDatabases.get_local_connection().connect()
        query = (
            select([vend_fail_history])
            .where(and_(vend_fail_history.c.company_id == company_id,
                        vend_fail_history.c.file_path == file_path))
        )
        result = conn_local.execute(query)

        if result.rowcount:
            out = result.fetchall()
            for x in out:
                if x.data_hash == data_hash and x.file_path == file_path:
                    return True
                else:
                    return False

        result.close()

    @classmethod
    def return_local_vend_fail_history_machine_paired(cls, company_id, data_hash, file_path, device_pid):
        conn_local = ConnectionForDatabases.get_local_connection().connect()
        query = (
            select([vend_fail_history])
            .where(and_(vend_fail_history.c.company_id == company_id,
                        vend_fail_history.c.machine_paired is not True))
        )
        result = conn_local.execute(query)

        if result.rowcount:
            out = result.fetchall()
            for x in out:
                base_pid = os.path.basename(x.file_path).split('_')[0]
                if base_pid == device_pid:
                    return True
                else:
                    return False

        result.close()

    @classmethod
    def get_vend_history_by_hash(cls, company_id, elastic_hash):
        """
        This method select all data from vend_success_history per specific
        company_id and elastic_hash
        :param company_id:
        :param elastic_hash:
        :return: inserted vends per specific company and elastic hash
        """

        importer_conn = ConnectionForDatabases.get_local_connection()

        vend_history = select([vend_success_history]).where(
            and_(vend_success_history.c.company_id == company_id,
                 vend_success_history.c.elastic_hash == elastic_hash)).order_by(
            vend_success_history.c.updated_at)

        result = importer_conn.execute(vend_history)

        if not result.rowcount:
            return make_response(False, [], 'No vends data found')

        vends_data = [result.first()]

        return make_response(True, vends_data,
                             'Found vends history by hash: {} data length: {}'.format(
                                 elastic_hash, result.rowcount))

    @classmethod
    def vend_insert_history(cls, company_id, vend_import_json, import_type, elastic_hash, file_path,
                            token, cloud_result, partial, statistics, cloud_inserted=False):
        """
        This is main function for vend insert into importer local database
        :param company_id:
        :param vend_import_json:
        :param import_type:
        :param elastic_hash:
        :param statistics:
        :param file_path:
        :param cloud_result:
        :param token:
        :param partial:
        :param cloud_inserted:
        :return: success inserted vends data into local importer database
        """
        importer_conn = ConnectionForDatabases.get_local_connection()
        extract_token = AuthorizeUser.verify_user_token(token.replace('JWT ', ''))
        vend_insert_query = vend_success_history.insert().values(
            company_id=company_id,
            import_type=return_import_type_id_custom_validation(import_type),
            elastic_hash=elastic_hash,
            data_hash='',
            statistics=statistics,
            partial=partial,
            cloud_results=cloud_result,
            import_data=vend_import_json,
            cloud_inserted=cloud_inserted,
            file_path=file_path,
            full_name=extract_token['response']['full_name'],
            user_id=int(extract_token['response']['user_id'])).returning(vend_success_history.c.id)

        result = importer_conn.execute(vend_insert_query)
        all_results = result.fetchone()

        if result.is_insert and all_results.id:
            return make_response(
                True,
                [],
                'Inserted new fail history with primary key {}'.format(all_results.id)
            )

        return make_response(False, [], "Failed to insert new fail history!")


class CompanyFailHistory(object):

    @classmethod
    def get_fail_history_by_company(cls, company_id):
        conn_local = ConnectionForDatabases.get_local_connection()
        history_query = select([cloud_company_process_fail_history]).where(
            cloud_company_process_fail_history.c.company_id == company_id
        ).order_by(desc(cloud_company_process_fail_history.c.created_at))

        result = conn_local.execute(history_query)

        if not result.rowcount:
            return make_response(False, [], 'No data found')

        out = result.fetchall()

        user_data = [
            {
                'id': str(x['id']),
                'import_type': return_import_type_name(x['import_type']),
                'hash': x['elastic_hash'],
                'created_at': x['created_at'].strftime("%Y-%m-%d %H:%M"),
                'data_hash': x['data_hash'],
                'file_path': x['file_path'],
                'error_type': return_enum_error_name(x['import_error_type']),
                'elastic': (
                    GetCompanyProcessLog
                    .get_process_by_hash(company_id, x['elastic_hash'])['results']
                )

            } for x in out
        ]

        return make_response(
            True, user_data,
            'Found process fail history length: {}'.format(result.rowcount)
        )

    @classmethod
    def get_fail_history_company_id_and_date_from(cls, company_id, from_datetime):

        conn_local = ConnectionForDatabases.get_local_connection()

        history_query = select([cloud_company_process_fail_history]).where(and_(
            cloud_company_process_fail_history.c.company_id == company_id,
            func.date(cloud_company_process_fail_history.c.updated_at) >= from_datetime)
        ).order_by(desc(cloud_company_process_fail_history.c.created_at))

        result = conn_local.execute(history_query)

        if not result.rowcount:
            return make_response(False, [], 'No data found')

        out = result.fetchall()

        user_data = [
            {
                'id': str(x['id']),
                'import_type': return_import_type_name(x['import_type']),
                'hash': x['elastic_hash'],
                'created_at': x['created_at'].strftime("%Y-%m-%d %H:%M"),
                'data_hash': x['data_hash'],
                'file_path': x['file_path'],
                'error_type': return_enum_error_name(x['import_error_type']),
                'elastic': (
                    GetCompanyProcessLog.get_process_by_hash(company_id, x['elastic_hash'])['results']
                )
            } for x in out
        ]

        return make_response(
            True, user_data,
            'Found process fail history from datetime: {} data length: {}'.format(
                from_datetime, result.rowcount)
        )

    @classmethod
    def get_fail_history_id_and_import_type(cls, company_id, import_type):
        conn_local = ConnectionForDatabases.get_local_connection()

        history_query = select([cloud_company_process_fail_history]).where(and_(
            cloud_company_process_fail_history.c.company_id == company_id,
            cloud_company_process_fail_history.c.import_type == import_type)
        ).order_by(desc(cloud_company_process_fail_history.c.created_at))

        result = conn_local.execute(history_query)

        if not result.rowcount:
            return make_response(False, [], 'No data found')

        out = result.fetchall()

        user_data = [
            {
                'id': str(x['id']),
                'import_type': return_import_type_name(x['import_type']),
                'hash': x['elastic_hash'],
                'created_at': x['created_at'].strftime("%Y-%m-%d %H:%M"),
                'data_hash': x['data_hash'],
                'file_path': x['file_path'],
                'error_type': return_enum_error_name(x['import_error_type']),
                'elastic': (
                    GetCompanyProcessLog
                        .get_process_by_hash(company_id, x['elastic_hash'])['results']
                )
            } for x in out
        ]

        return make_response(
            True, user_data,
            'Found process fail history data length: {}'.format(result.rowcount)
        )

    @classmethod
    def get_fail_history_by_datetime(cls, company_id, import_type, from_datetime):
        conn_local = ConnectionForDatabases.get_local_connection()

        history_query = select([cloud_company_process_fail_history]).where(and_(
            cloud_company_process_fail_history.c.company_id == company_id,
            cloud_company_process_fail_history.c.import_type == import_type,
            func.date(cloud_company_process_fail_history.c.updated_at) >= from_datetime)
        ).order_by(desc(cloud_company_process_fail_history.c.created_at))

        result = conn_local.execute(history_query)

        if not result.rowcount:
            return make_response(False, [], 'No data found')

        out = result.fetchall()

        user_data = [
            {
                'id': str(x['id']),
                'import_type': return_import_type_name(x['import_type']),
                'hash': x['elastic_hash'],
                'created_at': x['created_at'].strftime("%Y-%m-%d %H:%M"),
                'data_hash': x['data_hash'],
                'file_path': x['file_path'],
                'error_type': return_enum_error_name(x['import_error_type']),
                'elastic': (
                    GetCompanyProcessLog
                        .get_process_by_hash(company_id, x['elastic_hash'])['results']
                )
            } for x in out
        ]

        return make_response(
            True, user_data,
            'Found process fail history from datetime: {} data length: {}'.format(
                from_datetime, result.rowcount)
        )

    @classmethod
    def get_fail_history_by_process_hash(cls, company_id, elastic_hash):
        conn_local = ConnectionForDatabases.get_local_connection()

        history_query = select([cloud_company_process_fail_history]).where(
            cloud_company_process_fail_history.c.elastic_hash == elastic_hash
        ).order_by(desc(cloud_company_process_fail_history.c.created_at))

        result = conn_local.execute(history_query)

        if not result or not result.rowcount:
            return make_response(False, [], 'No data found')

        out = result.fetchall()

        user_data = [
            {
                'id': str(x['id']),
                'import_type': return_import_type_name(x['import_type']),
                'hash': x['elastic_hash'],
                'created_at': x['created_at'].strftime("%Y-%m-%d %H:%M"),
                'data_hash': x['data_hash'],
                'file_path': x['file_path'],
                'error_type': return_enum_error_name(x['import_error_type']),
                'elastic': (
                    GetCompanyProcessLog
                        .get_process_by_hash(company_id, x['elastic_hash'])['results']
                )
            } for x in out
        ]

        return make_response(
            True, user_data, 'Found process fail history by hash: {}'.format(elastic_hash)
        )

    @classmethod
    def get_vend_fail_history_by_process_hash(cls, company_id, elastic_hash):
        conn_local = ConnectionForDatabases.get_local_connection()

        history_query = select([vend_fail_history]).where(
            vend_fail_history.c.elastic_hash == elastic_hash
        ).order_by(desc(vend_fail_history.c.created_at))

        result = conn_local.execute(history_query)

        if not result or not result.rowcount:
            return make_response(False, [], 'No data found')

        out = result.fetchall()

        user_data = [
            {
                'id': str(x['id']),
                'import_type': return_import_type_name(x['import_type']),
                'hash': x['elastic_hash'],
                'created_at': x['created_at'].strftime("%Y-%m-%d %H:%M"),
                'data_hash': x['data_hash'],
                'file_path': x['file_path'],
                'error_type': return_enum_error_name(x['import_error_type']),
                'elastic': GetCompanyProcessLog.get_process_by_hash(company_id, x['elastic_hash'])['results']
            } for x in out
        ]

        return make_response(
            True, user_data, 'Found process fail history by hash: {}'.format(elastic_hash)
        )

    @classmethod
    def get_fail_history_by_data_hash(cls, company_id, data_hash):
        conn_local = ConnectionForDatabases.get_local_connection()

        history_query = select([cloud_company_process_fail_history]).where(and_(
            cloud_company_process_fail_history.c.company_id == company_id,
            cloud_company_process_fail_history.c.data_hash == data_hash)
        ).order_by(desc(cloud_company_process_fail_history.c.created_at))

        result = conn_local.execute(history_query).first()

        if not result:
            return make_response(False, [], 'No data found')

        out = result.fetchall()

        user_data = [
            {
                'id': str(x['id']),
                'import_type': return_import_type_name(x['import_type']),
                'hash': x['elastic_hash'],
                'created_at': x['created_at'].strftime("%Y-%m-%d %H:%M"),
                'data_hash': x['data_hash'],
                'file_path': x['file_path'],
                'error_type': return_enum_error_name(x['import_error_type']),
                'elastic': (
                    GetCompanyProcessLog
                        .get_process_by_hash(company_id, x['elastic_hash'])['results']
                )
            } for x in out
        ]

        result.close()

        return make_response(
            True, user_data, 'Found process fail history by hash: {}'.format(
                result.elastic_hash)
        )

    @staticmethod
    def get_history_by_hash_success_db(company_id, elastic_hash):
        conn_local = ConnectionForDatabases.get_local_connection()

        history_query = select([cloud_company_history]).where(and_(
            cloud_company_history.c.company_id == company_id,
            cloud_company_history.c.elastic_hash == elastic_hash
        )).order_by(cloud_company_history.c.updated_at)

        result = conn_local.execute(history_query)

        if not result.rowcount:
            return make_response(False, [], 'No data found')

        user_data = [result.first()]

        user_data = [dict(
            id=x.id,
            company_id=x.company_id,
            import_type=x.import_type,
            elastic_hash=x.elastic_hash,
            data_hash=x.data_hash,
            import_data=x.import_data,
            file_path=x.file_path,
            cloud_inserted=x.cloud_inserted,
            full_name=x.full_name,
            user_id=x.user_id,
            created_at=x.created_at,
            updated_at=x.updated_at,
            statistics=x.statistics,
            active_history=x.active_history,
            cloud_results=x.cloud_results,
            partial=x.partial,

        ) for x in user_data]

        return user_data

    @classmethod
    def insert_fail_history(cls, company_id, import_type, elastic_hash, data_hash, file_path,
                            import_error_type, token):
        old_entry = cls.get_fail_history_by_process_hash(int(company_id), elastic_hash)
        conn_local = ConnectionForDatabases.get_local_connection().connect()
        if old_entry['status']:
            return make_response(
                False, [],
                'Fail history by this hash already exists in table {}'.format(
                    elastic_hash
                )
            )

        # Same elastic hash can be present only in one table (CLOUD-6588 for details)!
        success_db_record = cls.get_history_by_hash_success_db(company_id, elastic_hash)
        if success_db_record:
            query = delete(cloud_company_history).where(cloud_company_history.c.elastic_hash == elastic_hash)
            conn_local.execute(query)

        extract_token = AuthorizeUser.verify_user_token(token.replace('JWT ', ''))

        insert_query = insert(cloud_company_process_fail_history).values(
            company_id=company_id,
            import_type=return_import_type_id(import_type),
            elastic_hash=elastic_hash,
            data_hash=data_hash,
            file_path=file_path,
            import_error_type=import_error_type,
            full_name=extract_token['response']['full_name'],
            user_id=int(extract_token['response']['user_id'])
        )
        result = conn_local.execute(insert_query)

        if result.is_insert and result.inserted_primary_key:
            return make_response(
                True,
                [dumps(result.inserted_primary_key)],
                'Inserted new fail history with primary key {}'.format(result.inserted_primary_key)
            )
        result.close()
        return make_response(False, [], "Failed to insert new fail history!")

    @classmethod
    def insert_vend_fail_history(cls, company_id, import_type, elastic_hash, data_hash,
                                 file_path, import_error_type, token, main_elastic_hash=None):
        already_processed = cls.get_vend_fail_history_by_process_hash(int(company_id), elastic_hash)
        if already_processed['status']:
            return make_response(
                False, [],
                'Fail history by this hash already exists in table {}'.format(
                    elastic_hash
                )
            )
        importer_conn = ConnectionForDatabases.get_local_connection().connect()
        extract_token = AuthorizeUser.verify_user_token(token.replace('JWT ', ''))
        insert_vend_fail_query = insert(vend_fail_history).values(
            company_id=int(company_id),
            import_type=int(return_import_type_id(import_type)),
            elastic_hash=str(elastic_hash),
            data_hash=str(data_hash),
            file_path=file_path,
            main_elastic_hash=main_elastic_hash,
            import_error_type=int(import_error_type),
            full_name=str(extract_token['response']['full_name']),
            user_id=int(extract_token['response']['user_id'])
        )
        result = importer_conn.execute(insert_vend_fail_query)

        if result.is_insert and result.inserted_primary_key:
            return make_response(True, [dumps(result.inserted_primary_key)],
                                 'Inserted new fail history with primary key {}'.format(
                                     result.inserted_primary_key
                                 ))
        result.close()

        return make_response(False, [], "Failed to insert new fail history")

    @classmethod
    def get_if_last(cls, company_id, data_hash):
        conn_local = ConnectionForDatabases.get_local_connection()
        query = (
            select([cloud_company_process_fail_history])
            .where(cloud_company_process_fail_history.c.company_id == company_id)
            .order_by(cloud_company_process_fail_history.c.created_at.desc())
        )
        result = conn_local.execute(query).first()

        return result if result and result.data_hash == data_hash else None


class CloudRequestHistory(object):

    @classmethod
    def retrieve_all_history_fail_and_success(cls, company_id, date_from=None, date_to=None):

        hsh = custom_hash(str(company_id) + str(date_from) + str(date_to))

        with get_local_connection_safe() as conn_local:

            result_array = []

            date_from_query = None
            date_to_query = None

            if not date_from:
                history_fail = select([cloud_company_process_fail_history]).where(
                    cloud_company_process_fail_history.c.company_id == company_id
                ).order_by(desc(cloud_company_process_fail_history.c.created_at))

            else:
                date_from_query = datetime.strptime(date_from, "%Y-%m-%d").date()
                date_to_query = datetime.strptime(date_to, "%Y-%m-%d").date()
                history_fail = select([cloud_company_process_fail_history]).where(and_(
                    cloud_company_process_fail_history.c.company_id == company_id,
                    func.date(cloud_company_process_fail_history.c.created_at) >= func.date(date_from_query),
                    func.date(cloud_company_process_fail_history.c.created_at) <= func.date(date_to_query)
                )
                ).order_by(desc(cloud_company_process_fail_history.c.created_at))

            out_fail = conn_local.execute(history_fail)

            if out_fail.rowcount:
                out_fail_results = out_fail.fetchall()
                for x in out_fail_results:

                    elastic = (
                        GetCompanyProcessLog
                        .get_process_by_hash(company_id, x['elastic_hash'])['results']
                    )
                    if elastic:
                        if elastic[0]['process_request_type'] in ['FILE', 'API', 'VENDON_API']:
                            username = '-'
                        else:
                            username = x['full_name']
                        result_array.append(
                            {
                                'id': str(x['id']),
                                'import_type': return_import_type_name(x['import_type']),
                                'hash': x['elastic_hash'],
                                'created_at': x['created_at'].strftime("%Y-%m-%d %H:%M:%S"),
                                'data_hash': x['data_hash'],
                                'file_path': x['file_path'],
                                'error_type': return_enum_error_name(x['import_error_type']),
                                'full_name': username,
                                'elastic': (
                                    elastic

                                ),
                                'cloud_results': None
                            }
                            )


            if not date_from:
                # Retrieve success history
                history_success = select([cloud_company_history]).where(
                    cloud_company_history.c.company_id == company_id
                ).order_by(desc(cloud_company_history.c.created_at))
            else:
                history_success = select([cloud_company_history]).where(and_(
                    cloud_company_history.c.company_id == company_id,
                    func.date(cloud_company_history.c.created_at) >= func.date(date_from_query),
                    func.date(cloud_company_history.c.created_at) <= func.date(date_to_query)
                )
                ).order_by(desc(cloud_company_history.c.created_at))

            out_success_data = conn_local.execute(history_success)

            if out_success_data.rowcount:
                out_success_results = out_success_data.fetchall()
                for x in out_success_results:
                    elastic = (
                        GetCompanyProcessLog
                        .get_process_by_hash(company_id, x['elastic_hash'])['results']
                    )
                    if elastic:
                        if elastic[0]['process_request_type'] in ['FILE', 'API', 'VENDON_API']:
                            username = '-'
                        else:
                            username = x['full_name']

                        if x.partial:
                            status = 'WARNING'
                        else:
                            status = 'SUCCESS'

                        result_array.append(
                            {
                                'id': str(x['id']),
                                'error_type': status,
                                'import_type': return_import_type_name(x['import_type']),
                                'hash': x['elastic_hash'],
                                'file_path': x['file_path'],
                                'created_at': x['created_at'].strftime("%Y-%m-%d %H:%M:%S"),
                                'data_hash': x['data_hash'],
                                'full_name': username,
                                'elastic': (
                                    elastic
                                ),
                                'cloud_results': x['cloud_results']

                            }

                        )

            RedisManagement.set_or_get_redis_data(hsh, result_array)
            return make_response(
                True, [], 'Found history.'
            )


class CloudVendRequestHistory(object):
    """
    This class display vend elastic logging, this is main part of representation elastic vends data on frontend, because
    this class basically make query on elastic vend model (using specific method "get_process_by_hash") and display
    all elastic message on frontend using 'company_dashboard_history_all' views method.
    """

    @classmethod
    def retrieve_all_vends_history_fail_and_success(cls, company_id, date_from=None, date_to=None):
        """
        This method get all elastic logging data fail/success per specific company.
        :param company_id:
        :return: elastic logging data
        """

        vends_hash_key = custom_hash_vends(company_id+str(date_from)+str(date_to))
        if not date_from:
            date_from = "1970-01-01"
        if not date_to:
            date_to = (datetime.today() + timedelta(days=1)).strftime('%Y-%m-%d')

        with get_local_connection_safe() as importer_local:
            result_array = []

            date_from_query = datetime.strptime(date_from, "%Y-%m-%d").date()
            date_to_query = datetime.strptime(date_to, "%Y-%m-%d").date()

            history_fail = select([vend_fail_history]).where(
                and_(vend_fail_history.c.company_id == company_id,
                func.date(vend_fail_history.c.created_at) >= func.date(date_from_query),
                func.date(vend_fail_history.c.created_at) <= func.date(date_to_query))
            ).order_by(desc(vend_fail_history.c.created_at))

            fail_history = importer_local.execute(history_fail)

            if fail_history.rowcount:
                history_fail_results = fail_history.fetchall()
                for x in history_fail_results:
                    elastic = GetVendImportProcessLog.get_process_by_hash(int(company_id), x['elastic_hash'])['results']

                    if len(elastic) == 0:
                        continue

                    if elastic[0]['process_request_type'] in ['FILE', 'API', 'VENDON_API']:
                        username = '-'
                    else:
                        username = x['full_name']

                    result_array.append(
                        {
                            'id': str(x['id']),
                            'import_type': return_import_type_name(x['import_type']),
                            'hash': x['elastic_hash'],
                            'created_at': x['created_at'].strftime("%Y-%m-%d %H:%M:%S"),
                            'data_hash': x['data_hash'],
                            'file_path': x['file_path'],
                            'error_type': return_enum_error_name(x['import_error_type']),
                            'full_name': username,
                            'elastic': (
                                elastic
                            ),
                            'cloud_results': None
                        }
                    )

            history_success = select([vend_success_history]).where(
                and_(vend_success_history.c.company_id == company_id,
                func.date(vend_success_history.c.created_at) >= func.date(date_from_query),
                func.date(vend_success_history.c.created_at) <= func.date(date_to_query))
            )
            out_success_data = importer_local.execute(history_success)

            if out_success_data.rowcount:
                success_results = out_success_data.fetchall()

                for x in success_results:
                    elastic = GetVendImportProcessLog.get_process_by_hash(int(company_id), x['elastic_hash'])['results']

                    if len(elastic) == 0:
                        continue

                    if elastic[0]['process_request_type'] == 'FILE' in ['FILE', 'API', 'VENDON_API']:
                        username = '-'
                    else:
                        username = x['full_name']

                    if x.partial:
                        status = 'WARNING'
                    else:
                        status = 'SUCCESS'

                    result_array.append(
                        {
                            'id': str(x['id']),
                            'error_type': status,
                            'import_type': return_import_type_name(x['import_type']),
                            'hash': x['elastic_hash'],
                            'file_path': x['file_path'],
                            'created_at': x['created_at'].strftime("%Y-%m-%d %H:%M:%S"),
                            'data_hash': x['data_hash'],
                            'full_name': username,
                            'elastic': (
                                elastic
                            ),
                            'cloud_results': x['cloud_results']
                        })

            RedisManagement.set_or_get_redis_data(vends_hash_key, result_array)
            return make_response(
                True, [], 'Found history.'
            )


class OldDevicePidHistory(object):
    @classmethod
    def get_old_device_pid(cls, device_pid, company_id, file_timestamp, import_type):
        importer_local = ConnectionForDatabases.get_local_connection().connect()
        # Get old device pid
        history_fail = select([vend_device_history]).where(and_(
            vend_device_history.c.company_id == company_id,
            vend_device_history.c.device_pid == device_pid,
            vend_device_history.c.import_type == import_type,
            vend_device_history.c.file_timestamp < file_timestamp)
        ).order_by(desc(vend_device_history.c.file_timestamp)).limit(1)

        # Get first bigger device
        first_bigger_timestamp = select([vend_device_history]).where(and_(
            vend_device_history.c.company_id == company_id,
            vend_device_history.c.device_pid == device_pid,
            vend_device_history.c.import_type == import_type,
            vend_device_history.c.file_timestamp > file_timestamp)
        ).order_by(asc(vend_device_history.c.file_timestamp)).limit(1)

        history_first_bigger_timestamp = []
        history_first_lower_timestamp = []

        result_query_first_bigger = importer_local.execute(first_bigger_timestamp)

        if result_query_first_bigger.rowcount:
            first_bigger_response = result_query_first_bigger.fetchone()

            if first_bigger_response.actual_machine:
                first_bigger_timestamp_results = [{
                    'owner_id': first_bigger_response.company_id,
                    'device_pid': first_bigger_response.device_pid,
                    'company_id': first_bigger_response.company_id,
                    'import_filename': first_bigger_response.import_filename,
                    'file_timestamp': first_bigger_response.file_timestamp,
                    'zip_filename': first_bigger_response.zip_filename,
                    'import_type': first_bigger_response.import_type,
                    'data': first_bigger_response.data,
                    'machine_id': first_bigger_response.machine_id
                }]

            else:
                first_bigger_timestamp_results = None

            history_first_bigger_timestamp.append(first_bigger_timestamp_results)

        result = importer_local.execute(history_fail)

        if result.rowcount:

            response = result.fetchone()
            if response.actual_machine:
                result_data = [{
                    'owner_id': response.company_id,
                    'device_pid': response.device_pid,
                    'company_id': response.company_id,
                    'import_filename': response.import_filename,
                    'file_timestamp': response.file_timestamp,
                    'zip_filename': response.zip_filename,
                    'import_type': response.import_type,
                    'data': response.data,
                    'machine_id': response.machine_id
                }]
            else:
                result_data = None
            history_first_lower_timestamp.append(result_data)
        result.close()
        return history_first_lower_timestamp, history_first_bigger_timestamp

    @classmethod
    def check_is_file_already_processing(cls, filename, company_id, import_type):
        importer_local = ConnectionForDatabases.get_local_connection().connect()
        # Get old device pid
        history_fail = select([vend_device_history]).where(and_(
            vend_device_history.c.company_id == company_id,
            vend_device_history.c.zip_filename == filename,
            vend_device_history.c.import_type == import_type)
        )

        result = importer_local.execute(history_fail)
        if result.rowcount:
            response = result.fetchone()

            if response.zip_filename:
                if response.actual_machine:
                    return response.zip_filename
            else:
                return None
        else:
            return None

        result.close()

    @classmethod
    def check_archive_machine_id(cls, cloud_machine_id, device_pid, company_id, import_type):
        importer_local = ConnectionForDatabases.get_local_connection().connect()
        history_fail = select([vend_device_history]).where(and_(
            vend_device_history.c.device_pid == device_pid,
            vend_device_history.c.company_id == company_id,
            vend_device_history.c.import_type == import_type,
        )).order_by(desc(vend_device_history.c.file_timestamp)).limit(1)
        result = importer_local.execute(history_fail)

        if result.rowcount:
            response = result.fetchone()
            if int(response.machine_id) == int(cloud_machine_id):
                return {'status': True, 'old_machine_id': response.machine_id}
            else:
                update_local_history = (
                    update(vend_device_history).where(and_(
                        vend_device_history.c.company_id == company_id,
                        vend_device_history.c.device_pid == device_pid)
                    ).values(actual_machine=False)
                )
                importer_local.execute(update_local_history)

                return {'status': False, 'old_machine_id': response.machine_id}
        result.close()
