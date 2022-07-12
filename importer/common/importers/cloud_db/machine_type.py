from collections import OrderedDict
from common.mixin.enum_errors import EnumValidationMessage as Const
from common.importers.cloud_db.common import Field, \
    DbQueryError, BaseImporter, BaseImportHandler, BaseImportObject, IdField
from database.cloud_database.core.query import (
    MachineTypeQueryOnCloud as MachineTypeQuery,
)


class MachineTypeObject(BaseImportObject):
    db_table = "machine_types"

    def __init__(self, obj, company_id, alive, db_obj=None):
        code = obj["machine_type_id"] if obj else db_obj["ext_id"]
        caption = obj["machine_type_name"] if obj else db_obj["name"]

        self.fields = OrderedDict(
            [
                ("id", IdField(db_obj["id"] if db_obj else None)),
                ("caption", Field(mandatory=True, value=caption)),
                ("owner_id", Field(mandatory=True, value=company_id)),
                ("code", Field(mandatory=True, value=code)),
                ("alive", Field(mandatory=False, default=True, value=alive)),
                ("cash_collection_duration", Field(mandatory=False, default=0)),
                ("cleaning_duration", Field(mandatory=False, default=0)),
                ("refill_duration", Field(mandatory=False, default=0)),
            ]
        )

        self.db_obj = db_obj


class MachineTypeImportHandler(BaseImportHandler):
    DB_TABLE = "machine_types"
    SEQ_NAME = 'machine_types_id_seq'

    IMPORT_TYPE = MachineTypeObject
    ACTION = "machine_type_action"
    DB_ID = "ext_id"
    IMPORT_ID = "machine_type_id"
    CAPTION = "machine_type_name"

    def get_columns_to_update(self):
        return ["id", "code", "caption", "alive"]

    def get_columns_to_insert(self):
        return [
            "id",
            "caption",
            "alive",
            "owner_id",
            "code",
            "cash_collection_duration",
            "cleaning_duration",
            "refill_duration",
        ]

    def get_all_objs_from_database(self, company_id, external_ids=None):
        cloud_types = MachineTypeQuery.get_machine_types_for_company(company_id)
        if not cloud_types['status']:
            raise DbQueryError(self.DB_TABLE)

        return cloud_types['results']

    def fill_objects_for_action_50(self, data):
        for obj in data:
            db_obj = self.get_db_object(obj[self.IMPORT_ID])
            row = self.IMPORT_TYPE(obj, self.company_id, True, db_obj=db_obj)
            if db_obj:
                self.objs_to_update.append(row)
            else:
                self.objs_to_insert.append(row)

        import_ids = [i[self.IMPORT_ID] for i in data]
        for db_obj in self.db_objects:
            if db_obj[self.DB_ID] not in import_ids and db_obj['alive']:
                row = self.IMPORT_TYPE(None, self.company_id, False, db_obj=db_obj)
                if db_obj['name'] not in self.external_ids or db_obj['ext_id'] not in self.external_ids:
                    if not db_obj['is_default']:
                        self.objs_to_delete.append(row)
                else:
                    self.validation_database_errors_count += 1
                    self.validation_database_errors_message.append((Const.MACHINE_TYPE_IS_USED, db_obj['name']))


class MachineTypeImporter(BaseImporter):
    def populate(self, data, used_machine_type=None):
        import_objects = MachineTypeImportHandler(data, self.company_id, used_machine_type)
        import_objects.populate_insert_objs_ids()
        self.all_import_handlers.append(import_objects)
        self.validation_database_errors_count = import_objects.validation_database_errors_count
        self.validation_database_errors_message = import_objects.validation_database_errors_message

    def get_stats(self):
        return self.stats['machine_types']
