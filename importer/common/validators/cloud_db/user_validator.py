import copy

from common.mixin.validation_const import ImportAction
from common.validators.cloud_db.common_validators import get_duplicated, DuplicateRowsError, get_alive_and_dead
from common.importers.cloud_db.common import DbQueryError, get_upsert_if_needed
from database.cloud_database.core.query import UserQueryOnCloud
from common.mixin.enum_errors import EnumValidationMessage as Const


class UserValidator(object):
    __name__ = 'user'

    def __init__(self, company_id, rows):
        self.id_fld = 'user_id'
        self.action_fld = 'user_action'
        self.identifier_fld = 'email'
        self.unique_fields = {
            'ext_id': 'user_id',
            'email': 'email'
        }
        self.bool_fields = ['service_email_notification', 'service_sms_notification',
                            'service_staff_mobile_app', 'service_staff_mobile_technical_view',
                            'assign_filling_route', 'assign_event']

        self.company_id = company_id
        self.rows = copy.deepcopy(rows)

    def get_cloud_users(self):
        cloud_users = UserQueryOnCloud.get_users(self.company_id)
        if not cloud_users['status']:
            raise DbQueryError(self.__name__)

        return cloud_users['results']

    def check_duplicates(self):
        duplicates = get_duplicated(self.unique_fields, self.rows)
        if duplicates:
            raise DuplicateRowsError(self.__name__, duplicates)

    def add_actions(self, alive_cloud_items, external_ids, all_ids, dead_ids):
        action_50 = int(self.rows[0][self.action_fld]) == 50
        entity_id = self.id_fld

        if action_50:
            for m in [m for m in self.rows if m[entity_id] in all_ids]:
                m[self.action_fld] = ImportAction.UPDATE.value
                m['resurrect_entity'] = True if m[entity_id] in dead_ids else False
            for m in [m for m in self.rows if m[entity_id] not in all_ids]:
                m[self.action_fld] = ImportAction.CREATE.value

            # all machine IDs which are not to be updated or inserted are to be
            # deleted and we need to add them
            for m in [m for m in alive_cloud_items if
                      m['ext_id'] not in external_ids]:
                self.rows = [{
                    self.id_fld: m['ext_id'],
                    self.action_fld: ImportAction.DELETE.value,
                    self.identifier_fld: m['email'],
                    'cloud_id': m['id']
                }] + self.rows
        else:
            for m in [m for m in self.rows if m[entity_id] in all_ids]:
                if m[self.action_fld] != ImportAction.DELETE.value and \
                        m[self.id_fld] in dead_ids:
                    m[self.action_fld] = ImportAction.UPDATE.value
                    m['resurrect_entity'] = True

    @staticmethod
    def validate_app_rights(row):
        staff_app = row['service_staff_mobile_app']
        technical_view = row['service_staff_mobile_technical_view']
        role = row['user_role']
        filling_route = row['assign_filling_route']

        errors = []
        if technical_view == 'true' and staff_app == 'false':
            errors.append(Const.INVALID_APP_RIGHTS)

        if role != 'Filler' and filling_route == 'false' and \
                (technical_view == 'true' or staff_app == 'true'):
            errors.append(Const.APP_NOT_ALLOWED)

        return errors

    def validate(self):
        self.check_duplicates()

        cloud_items = self.get_cloud_users()
        alive_cloud_items = [m for m in cloud_items if m['alive'] is True]
        alive_cloud_items_ids = [m['ext_id'] for m in alive_cloud_items]

        external_ids = set([m[self.id_fld] for m in self.rows])
        alive, dead = get_alive_and_dead(external_ids, cloud_items)

        self.add_actions(alive_cloud_items, external_ids, alive + dead, dead)

        external_ids = set([m[self.id_fld] for m in self.rows])
        alive, dead = get_alive_and_dead(external_ids, cloud_items)

        errors = []
        for w_item in self.rows:
            entity_id = w_item[self.id_fld]
            entity_name = w_item[self.identifier_fld]
            entity_name = entity_name.lower()
            w_item[self.identifier_fld] = entity_name

            action = ImportAction(int(w_item[self.action_fld]))

            if action in [ImportAction.CREATE, ImportAction.UPDATE]:
                app_errors = self.validate_app_rights(w_item)
                for err in app_errors:
                    errors.append((entity_id, err, self.__name__, entity_name,
                                   action.name))

            if not w_item.get('resurrect_entity'):
                action, w_item[self.action_fld] = get_upsert_if_needed(action, entity_id, alive)

            if action == ImportAction.UPDATE:
                if w_item.get('resurrect_entity'):
                    if entity_id not in dead:
                        errors.append((entity_id, Const.DATABASE_NOT_FOUND,
                                       self.__name__, entity_name, action.name))
                else:
                    if entity_id not in alive:
                        errors.append((entity_id, Const.DATABASE_NOT_FOUND,
                                       self.__name__, entity_name, action.name))
            if action == ImportAction.DELETE:
                if entity_id not in alive_cloud_items_ids:
                    errors.append((entity_id, Const.DATABASE_NOT_FOUND,
                                   self.__name__, entity_name, action.name))
            elif action is ImportAction.CREATE:
                if entity_id in alive_cloud_items_ids:
                    errors.append((entity_id, Const.DATABASE_FOUND, self.__name__,
                                   entity_name, action.name))
        return self.rows, errors
