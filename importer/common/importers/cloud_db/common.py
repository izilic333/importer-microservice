from io import StringIO

from common.mixin.validation_const import ImportAction
from common.mixin.enum_errors import EnumValidationMessage as Const
from database.cloud_database.common.common import get_cloud_connection_safe
from database.cloud_database.connection.connection import cloud_database_engine


CREATE_TABLE = """
select * from import.create_staging_table(
    p_table_schema := '{}',
    p_table_name := '{}',
    p_run_id := {},
    p_operations := ARRAY[{}],
    p_upd_columns := ARRAY[{}],
    p_dry_run := false
    );"""

CREATE_INDICES = """
select * from import.generate_seq_vals(
    p_sequence_schema := '{}',
    p_sequence_name := '{}',
    p_number_of_vals := {}
    )
"""


class FieldNotDefinedError(Exception):
    pass


class DbQueryError(Exception):
    def __init__(self, import_name):
        self.import_name = import_name
        self.msg_const = Const.DATABASE_QUERY_ERROR


class InvalidImportData(Exception):
    def __init__(self, import_name, errors):
        self.import_name = import_name
        self.errors = errors


class FieldEmptyError(Exception):
    pass


class ReferenceObjectNotFound(Exception):
    pass


def is_none(val):
    return val in ["<null>", None]


def get_alive_and_dead(external_ids, cloud_items):
    alive = [e['ext_id'] for e in cloud_items if e['ext_id'] in external_ids and e['alive']]
    dead = [e['ext_id'] for e in cloud_items if e['ext_id'] in external_ids and not e['alive']]

    return alive, dead


def get_values_from_dict_arr(in_dict, key):
    return [m[key] for m in in_dict]


def get_upsert_if_needed(action, entity_id, alive_ids):
    entity_exists = entity_id in alive_ids

    if action == ImportAction.UPDATE and not entity_exists:
        return ImportAction.CREATE, ImportAction.CREATE.value

    if action == ImportAction.CREATE and entity_exists:
        return ImportAction.UPDATE, ImportAction.UPDATE.value

    return action, action.value


class Field(object):
    def __init__(self, mandatory=False, default=None, value=None, original_value=None):
        if all(is_none(val) for val in (default, value, original_value)):
            raise FieldNotDefinedError(
                "Field has to have value, default or original_value."
            )

        if mandatory and is_none(value):
            raise FieldNotDefinedError("For mandatory fields value has to be provided.")

        self.mandatory = mandatory
        self.default = default
        self.value = value
        self.original_value = original_value

    def __str__(self):
        if not is_none(self.value):
            return str(self.value)
        if self.original_value:
            return str(self.original_value)
        return str(self.default)


class UndefinedField(object):
    def __str__(self):
        if not self.value:
            raise FieldEmptyError(
                "UndefinedField was not properly populated. "
                "Call populate_insert_objs_ids or populate_ref_fields to populate your field."
            )
        return str(self.value)


class IdField(UndefinedField):
    """
        This field enables the data to be inserted later. It's used for ids that aren't
        determined before the row is inserted.
    """

    def __init__(self, value=None):
        self.value = value

    def set_id(self, value):
        self.value = value


class ReferenceField(UndefinedField):
    """
        ReferenceField is used to model foreign key relationship between tables.

        When inserting in two tables in the same import, id of one entity won't be
        available when creating an ImportObject. With ReferenceField user can describe
        that relationship and enable the id to be inserted later in the process.

        In case of update ReferenceField should be instanced with value, if not
        arguments for matching with the correct entity should be provided.
        ref_type    - any string; it allows ImportObject to have more ReferencedFields
                    - unique in one ImportObject
        ref_field   - field from referenced entity that will be copied to value
        ref_data    - list of (field_name, field_value) pairs
                    - entity that is referenced has to have corresponding data values
    """

    def __init__(self, value=None, ref_type=None, ref_field=None, ref_data=None):
        if value is None and any(f is None for f in (ref_type, ref_field, ref_data)):
            raise FieldNotDefinedError(
                "ReferenceField is not properly defined. "
                "The field's value or ref_type, ref_field and ref_data must be defined."
            )

        self.value = value
        self.ref_type = ref_type
        self.ref_data = ref_data
        self.ref_field = ref_field

    def reference(self, ref_obj):
        self.value = ref_obj.fields[self.ref_field].value


class BaseImportObject(object):
    """
        A representation of import data that will be saved in a single table.

        To write your ImportObject inherit this class and define self.fields OrderedDict
        that describes your data.
        self.fields can contain Field, IdField and ReferenceField objects.

    """
    fields = None

    def get_str(self, field_names=None):
        if not field_names:
            return ";".join([str(fl) for fl in self.fields.values()])

        return ";".join([str(self.fields[fl]) for fl in field_names])


class BaseImportHandler(object):
    """
        This is a type that can be imported to db from Importer object

        Inherit this to suit your type, fill:
            - DB_TABLE(name of db table),
            - SEQ_NAME(sequence in db used for ids of DB_TABLE),
            - IMPORT_TYPE(type inherited from BaseImportObject),
            - ACTION(action field in import data),
            - DB_ID(id field in data from get_all_objs_from_database()),
            - IMPORT_ID(id field in import data),
            - CAPTION(field from import data that will be used as name; used in errors)
        and override:
            - get_all_objs_from_database
            - get_columns_to_update
            - get_columns_to_insert

        ImportHandler can't be imported until all ReferenceFields and IdFields have
        populated values.
    """

    DB_TABLE = None
    SEQ_NAME = None
    IMPORT_TYPE = None
    ACTION = None
    DB_ID = None
    IMPORT_ID = None
    CAPTION = None

    def __init__(self, data, company_id, external_ids=None):
        self.objs_to_insert = []
        self.objs_to_update = []
        self.objs_to_delete = []
        self.company_id = company_id
        self.validation_database_errors_count = 0
        self.validation_database_errors_message = []
        self.external_ids = external_ids
        self.db_objects = self.get_all_objs_from_database(company_id, external_ids)

        action_50 = int(data[0][self.ACTION]) == 50
        if action_50:
            self.fill_objects_for_action_50(data)
        else:
            errors = self.fill_objects(data)
            if errors:
                raise InvalidImportData(self.DB_TABLE, errors)

    def get_db_object(self, id):
        for obj in self.db_objects:
            if obj.get(self.DB_ID) == id and obj.get("alive"):
                return obj
        return None

    def fill_objects(self, data):
        errors = []
        for obj in data:
            db_obj = self.get_db_object(obj[self.IMPORT_ID])
            action = ImportAction(int(obj[self.ACTION]))

            if action == ImportAction.DELETE and not db_obj:
                data = (
                    obj[self.IMPORT_ID],
                    Const.DATABASE_NOT_FOUND,
                    self.DB_TABLE,
                    obj[self.CAPTION],
                    ImportAction.DELETE.name,
                )
                errors.append(data)
                continue

            alive = action != ImportAction.DELETE
            row = self.IMPORT_TYPE(obj, self.company_id, alive, db_obj=db_obj)
            if db_obj and action != ImportAction.DELETE:
                self.objs_to_update.append(row)
            elif db_obj and action == ImportAction.DELETE:
                self.objs_to_delete.append(row)
            else:
                self.objs_to_insert.append(row)

        return errors

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
                self.objs_to_delete.append(row)

    def get_import_operations(self):
        operations = []
        if len(self.objs_to_insert):
            operations.append("'INSERT'::import.operation")
        if len(self.objs_to_update) or len(self.objs_to_delete):
            operations.append("'UPDATE'::import.operation")

        return ", ".join(operations)

    def get_available_ids(self):
        num_of_inserts = len(self.objs_to_insert)

        with get_cloud_connection_safe() as conn_cloud:
            query = CREATE_INDICES.format("public", self.SEQ_NAME, num_of_inserts)
            ids = conn_cloud.execute(query).fetchall()
        for id in ids:
            yield id[0]

    def populate_insert_objs_ids(self):
        """
            Call to populate all id fields for rows to insert

            It calls a db function to reserve ids for entities to insert and fills
            their IdFields.
        """
        all_ids = self.get_available_ids()
        for obj, id in zip(self.objs_to_insert, all_ids):
            obj.fields["id"].set_id(id)

    def populate_ref_fields(self, ref_type, ref_objects):
        """
            Populates values of ReferencedFields

            To populate field of IMPORT_TYPE call the function with the ref_type to
            populate and the corresponding ref_objects.
            ref_objects is a list of ImportObjects of a type you want to reference.
            ref_objects have to have the ref_field already populated for this to work,
            usually by calling populate_insert_objs_ids on them first.
        """
        def get_ref_field_data(ref_type, first_object):
            for name, field in first_object.fields.items():
                if type(field) == ReferenceField and field.ref_type == ref_type:
                    return name, [f[0] for f in field.ref_data]

        def get_ref_obj_dict(fields, ref_objects):
            ref_dict = {}
            for obj in ref_objects:
                value_tuple = tuple(obj.fields[fname].value for fname in fields)
                ref_dict[value_tuple] = obj
            return ref_dict

        if not self.objs_to_insert:
            return
        ref_field, original_fields = get_ref_field_data(
            ref_type, self.objs_to_insert[0]
        )
        ref_obj_dict = get_ref_obj_dict(original_fields, ref_objects)

        for obj in self.objs_to_insert:
            if obj.fields[ref_field].value is not None:
                continue
            value_tuple = tuple(f[1] for f in obj.fields[ref_field].ref_data)
            ref_obj = ref_obj_dict.get(value_tuple)
            if not ref_obj:
                value_pairs = ",".join(
                    ["{} = {}".format(n, v) for n, v in obj.fields[ref_field].ref_data]
                )
                msg = (
                    "A referenced object with {} is not provided. "
                    "Check your ref_objects list.".format(value_pairs)
                )
                raise ReferenceObjectNotFound(msg)
            obj.fields[ref_field].reference(ref_obj)

    def get_all_objs_from_database(self, company_id, external_ids=None):
        raise NotImplementedError

    def get_columns_to_update(self):
        raise NotImplementedError

    def get_columns_to_insert(self):
        raise NotImplementedError


class BaseImporter(object):
    """
        This is inherited once for every import type.

        Override populate and get_stats methods.
        populate -  here ImportHandlers for all tables are instanced, fields that have
                    to be populated are populated with populate_insert_objs_ids and
                    populate_ref_fields and handlers are added to
                    self.all_import_handlers
        get_stats - choose statistic one of the tables as representable for the whole
                    import
    """

    def __init__(self, company_id):
        self.company_id = company_id
        self.all_import_handlers = []
        self.stats = {}
        self.validation_database_errors_count = 0
        self.validation_database_errors_message = []

    def populate(self, data, external_ids=None):
        raise NotImplementedError

    def save(self):
        session_cloud = cloud_database_engine.connect()
        conn = session_cloud.connection.connection
        cursor = conn.cursor()

        for import_handler in self.all_import_handlers:
            import_stats = self.save_import_data(import_handler, cursor)
            self.stats[import_handler.DB_TABLE] = import_stats

        conn.commit()

        return self.get_stats()

    def get_stats(self):
        raise NotImplementedError

    @staticmethod
    def copy_to_temp_table(cursor, table, columns, objects_to_copy):
        if not objects_to_copy:
            return

        insert_table_name = """{}.\"{}\"""".format(table[0][0], table[0][1])

        f = StringIO("\n".join([r.get_str(columns) for r in objects_to_copy]))
        cursor.copy_from(f, insert_table_name, sep=";", null="/N", columns=columns)

    @staticmethod
    def save_import_data(handler, cursor):
        table_name = handler.DB_TABLE
        import_operations = handler.get_import_operations()
        columns = ",".join(["'%s'" % c for c in handler.get_columns_to_update()])

        cursor.callproc("import.get_run_id")
        run_id = cursor.fetchone()[0]

        query = CREATE_TABLE.format(
            "public", table_name, run_id, import_operations, columns
        )

        cursor.execute(query)
        tables = cursor.fetchall()
        insert_table = [(t[0], t[1]) for t in tables if t[2] == "INSERT"]
        update_table = [(t[0], t[1]) for t in tables if t[2] == "UPDATE"]

        insert_columns = handler.get_columns_to_insert()
        update_columns = handler.get_columns_to_update()

        BaseImporter.copy_to_temp_table(
            cursor, insert_table, insert_columns, handler.objs_to_insert
        )
        BaseImporter.copy_to_temp_table(
            cursor, update_table, update_columns, handler.objs_to_update
        )
        BaseImporter.copy_to_temp_table(
            cursor, update_table, update_columns, handler.objs_to_delete
        )

        cursor.callproc(
            "import.merge", ["public", handler.DB_TABLE, int(run_id), False]
        )

        return {
            "inserted": len(handler.objs_to_insert),
            "updated": len(handler.objs_to_update),
            "deleted": len(handler.objs_to_delete),
            "errors": 0,
        }
