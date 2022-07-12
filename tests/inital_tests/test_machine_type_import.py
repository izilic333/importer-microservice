import unittest
from unittest import TestCase

from common.importers.cloud_db.common import InvalidImportData
from common.importers.cloud_db.machine_type import (
    MachineTypeImporter,
    MachineTypeImportHandler,
)
from common.mixin.enum_errors import EnumValidationMessage as Const
from common.mixin.validation_const import ImportAction
from common.validators.cloud_db.common_validators import (
    MachineTypeValidator,
    DuplicateRowsError,
)


def mock_get_available_ids(import_handler):
    ids = [obj["id"] for obj in import_handler.get_all_objs_from_database(0)]
    next_id = max(ids) + 1 if ids else 1
    num_of_inserts = len(import_handler.objs_to_insert)

    for index in range(next_id, next_id + num_of_inserts):
        yield index


class MachineTypeImportTest(TestCase):
    def test_duplicates(self):
        import_data = [
            {
                "machine_type_name": "Machine type A",
                "machine_type_id": "TYPE_A",
                "machine_type_action": "0",
            },
            {
                "machine_type_name": "Machine type B",
                "machine_type_id": "TYPE_A",
                "machine_type_action": "0",
            },
            {
                "machine_type_name": "Machine type C",
                "machine_type_id": "TYPE_C",
                "machine_type_action": "1",
            },
            {
                "machine_type_name": "Machine type C",
                "machine_type_id": "TYPE_D",
                "machine_type_action": "2",
            },
            {
                "machine_type_name": "Machine type A",
                "machine_type_id": "TYPE_D",
                "machine_type_action": "0",
            },
        ]

        validator = MachineTypeValidator(None, import_data)
        with self.assertRaises(DuplicateRowsError) as context:
            validator.validate()

        error = context.exception
        self.assertEqual(error.duplicates["machine_type_id"], ["TYPE_A", "TYPE_D"])
        self.assertEqual(
            error.duplicates["machine_type_name"], ["Machine type C", "Machine type A"]
        )

    @staticmethod
    def mock_get_all_objs_from_database(import_handler, company_id, external_ids=None):
        return [
            {"id": 1, "ext_id": "TYPE_A", "name": "Machine type A", "alive": True},
            {"id": 2, "ext_id": "TYPE_B", "name": "Machine type B", "alive": True},
            {"id": 3, "ext_id": "TYPE_C", "name": "Machine type C", "alive": True},
            {"id": 4, "ext_id": "TYPE_D", "name": "Machine type D", "alive": False},
        ]

    def test_delete_of_undefined_item(self):
        import_data = [
            {
                "machine_type_name": "Machine type D",
                "machine_type_id": "TYPE_D",
                "machine_type_action": "2",
            },
            {
                "machine_type_name": "Machine type E",
                "machine_type_id": "TYPE_E",
                "machine_type_action": "2",
            },
        ]

        importer = MachineTypeImporter(0)
        MachineTypeImportHandler.get_all_objs_from_database = (
            self.mock_get_all_objs_from_database
        )
        with self.assertRaises(InvalidImportData) as context:
            importer.populate(import_data)

        errors = context.exception.errors
        self.assertEqual(
            errors[0],
            (
                "TYPE_D",
                Const.DATABASE_NOT_FOUND,
                "machine_types",
                "Machine type D",
                ImportAction.DELETE.name,
            ),
        )
        self.assertEqual(
            errors[1],
            (
                "TYPE_E",
                Const.DATABASE_NOT_FOUND,
                "machine_types",
                "Machine type E",
                ImportAction.DELETE.name,
            ),
        )

    def test_populate_insert_objs_ids(self):
        import_data = [
            {
                "machine_type_name": "Machine type D",
                "machine_type_id": "TYPE_D",
                "machine_type_action": "0",
            }
        ]

        importer = MachineTypeImporter(0)
        MachineTypeImportHandler.get_all_objs_from_database = (
            self.mock_get_all_objs_from_database
        )
        MachineTypeImportHandler.get_available_ids = mock_get_available_ids
        importer.populate(import_data)

        import_handler = importer.all_import_handlers[0]
        el_to_insert = import_handler.objs_to_insert[0]

        self.assertEqual(el_to_insert.get_str(), "5;Machine type D;0;TYPE_D;True;0;0;0")

    def test_action_50(self):
        import_data = [
            {
                "machine_type_name": "Machine type A",
                "machine_type_id": "TYPE_A",
                "machine_type_action": "50",
            },
            {
                "machine_type_name": "Machine type D",
                "machine_type_id": "TYPE_D",
                "machine_type_action": "50",
            },
            {
                "machine_type_name": "Machine type E",
                "machine_type_id": "TYPE_E",
                "machine_type_action": "50",
            },
        ]

        importer = MachineTypeImporter(0)
        MachineTypeImportHandler.get_all_objs_from_database = (
            self.mock_get_all_objs_from_database
        )
        MachineTypeImportHandler.get_available_ids = mock_get_available_ids
        importer.populate(import_data)

        import_handler = importer.all_import_handlers[0]

        elems_to_insert = [el.get_str() for el in import_handler.objs_to_insert]
        elems_to_update = [el.get_str() for el in import_handler.objs_to_update]
        elems_to_delete = [el.get_str() for el in import_handler.objs_to_delete]

        self.assertEqual(
            elems_to_insert,
            [
                "5;Machine type D;0;TYPE_D;True;0;0;0",
                "6;Machine type E;0;TYPE_E;True;0;0;0"
            ],
        )
        self.assertEqual(
            elems_to_update,
            [
                "1;Machine type A;0;TYPE_A;True;0;0;0",
            ],
        )
        self.assertEqual(
            elems_to_delete,
            [
                "2;Machine type B;0;TYPE_B;False;0;0;0",
                "3;Machine type C;0;TYPE_C;False;0;0;0",
            ],
        )


if __name__ == "__main__":
    unittest.main()
