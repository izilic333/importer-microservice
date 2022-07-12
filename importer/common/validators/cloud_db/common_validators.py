from collections import defaultdict

from common.mixin.validation_const import ImportAction, ImportType
from database.cloud_database.core.query import MachineQueryOnCloud, \
    MachineTypeQueryOnCloud
from common.mixin.enum_errors import EnumValidationMessage as Const


def get_duplicated(unique_fields, rows):
    duplicates = defaultdict(list)
    for f in unique_fields.values():
        occurred_values = set()
        for r in rows:
            val = r.get(f, None)
            if val:
                if val in occurred_values:
                    duplicates[f].append(val)
                else:
                    occurred_values.add(val)
    return duplicates


class DuplicateRowsError(Exception):
    def __init__(self, import_name, duplicates):
        self.import_name = import_name
        self.duplicates = duplicates


def get_alive_and_dead(external_ids, cloud_items):
    alive = [e['ext_id'] for e in cloud_items
             if e['ext_id'] in external_ids and e['alive']]
    dead = [e['ext_id'] for e in cloud_items
            if e['ext_id'] in external_ids and not e['alive'] and e['ext_id'] not in alive]

    return alive, dead


def get_upsert_if_needed(action, entity_id, alive_ids):
    entity_exists = entity_id in alive_ids

    if action == ImportAction.UPDATE and not entity_exists:
        return ImportAction.CREATE, ImportAction.CREATE.value

    if action == ImportAction.CREATE and entity_exists:
        return ImportAction.UPDATE, ImportAction.UPDATE.value

    return action, action.value


class BaseValidator(object):
    UNIQUE_FIELDS = {}

    def check_duplicates_in_import_file(self):
        duplicates = get_duplicated(self.UNIQUE_FIELDS, self.rows)
        if duplicates:
            raise DuplicateRowsError(self.__name__, duplicates)

    def validate(self):
        self.check_duplicates_in_import_file()

        return self.rows


class MachineTypeValidator(BaseValidator):
    __name__ = "machine_type"
    UNIQUE_FIELDS = {
        "ext_id": "machine_type_id",
        "name": "machine_type_name"
    }

    def __init__(self, company_id, rows):
        self.rows = rows
        self.company_id = company_id

    def filter_used_machine_types(self, rows):
        warnings = []
        filtered_rows = []
        machines = MachineQueryOnCloud.get_machines_for_company(self.company_id)
        machine_types_used = [m.get('type_id') for m in machines['results']
                              if m['alive'] and m.get('type_id')]
        machine_types = MachineTypeQueryOnCloud.get_machine_types_for_company(self.company_id)

        used_external_ids = [mt.get('ext_id') for mt in machine_types['results']
                             if mt['id'] in machine_types_used and mt.get('ext_id') and mt['alive']]

        used_machine_type_name = [mt.get('name') for mt in machine_types['results'] if mt['alive'] and mt.get('name')]

        default_machine_types = [m.get('ext_id') for m in machine_types['results']
                              if m['alive'] and m.get('is_default')]

        # extra business logic request for validation_statistic_error ...
        validation_statistic_error = 0
        machine_type_ext_ids = [mt.get('ext_id') for mt in machine_types['results'] if mt['alive'] and mt['ext_id']]

        for r in rows:
            work_on = ImportType.MACHINE_TYPES.value['capitalised_name']
            action_fld = work_on.lower() + '_action'
            external_id, action, machine_type_name = r['machine_type_id'], r['machine_type_action'], r[
                'machine_type_name']

            try:
                # add upsert on machine type
                action = ImportAction(int(r[action_fld]))
                action_name, r[action_fld] = get_upsert_if_needed(action, r['machine_type_id'], machine_type_ext_ids)
            except ValueError:
                warnings.append((Const.DATABASE_WRONG_ACTION, work_on, r[action_fld]))
                continue
            action = action_name.value
            if action == ImportAction.DELETE.value:
                if external_id in used_external_ids:
                    validation_statistic_error += 1
                    warnings.append((Const.MACHINE_TYPE_IS_USED, external_id))
                    continue
                if external_id in default_machine_types:
                    validation_statistic_error += 1
                    warnings.append((Const.MACHINE_TYPE_IS_DEFAULT, external_id))
                    continue

            search_machine_type = list(
                filter(lambda x: x['name'] == machine_type_name and x['ext_id'] == external_id,
                       machine_types["results"])
            )

            if action in [ImportAction.CREATE.value, ImportAction.UPDATE.value]:
                if not search_machine_type:
                    if machine_type_name in used_machine_type_name:
                        validation_statistic_error += 1
                        warnings.append((Const.MACHINE_TYPE_NAME_IS_USED, machine_type_name))
                        continue
                    elif external_id in machine_type_ext_ids and action == ImportAction.CREATE.value:
                        validation_statistic_error += 1
                        warnings.append((Const.MACHINE_TYPE_NAME_IS_USED, machine_type_name))
                        continue

            filtered_rows.append(r)

        used_machine_type_ext_ids_and_name = []

        for mt in machine_types['results']:
            if mt['id'] in machine_types_used and mt.get('ext_id') and mt['alive']:
                used_machine_type_ext_ids_and_name.append(mt.get('ext_id'))
                used_machine_type_ext_ids_and_name.append(mt.get('name'))

        return filtered_rows, warnings, used_machine_type_ext_ids_and_name, validation_statistic_error

    def validate(self):
        self.check_duplicates_in_import_file()
        self.rows, warnings, used_machine_type, validation_statistic_error = self.filter_used_machine_types(self.rows)
        return self.rows, warnings, used_machine_type, validation_statistic_error
