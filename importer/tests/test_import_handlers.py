import unittest
from collections import OrderedDict
from unittest import TestCase

from common.importers.cloud_db.common import (
    BaseImportObject,
    BaseImportHandler,
    IdField,
    Field,
    ReferenceField,
    FieldEmptyError,
    ReferenceObjectNotFound,
    FieldNotDefinedError,
)
from tests.test_machine_type_import import mock_get_available_ids


class TestObject(BaseImportObject):
    def __init__(self, obj, company_id, alive, db_obj=None):
        self.fields = OrderedDict(
            [
                ("id", IdField(db_obj["id"] if db_obj else None)),
                ("caption", Field(mandatory=True, value=obj["test_name"])),
                ("external_id", Field(mandatory=True, value=obj["test_id"])),
            ]
        )

        self.db_obj = db_obj


class TestImportHandler(BaseImportHandler):
    DB_TABLE = "test"

    IMPORT_TYPE = TestObject
    ACTION = "test_action"
    IMPORT_ID = "test_id"
    CAPTION = "test_name"

    def get_all_objs_from_database(self, company_id, external_ids=None):
        return []

    def get_columns_to_update(self):
        return []

    def get_columns_to_insert(self):
        return []


class TestObjectWithReference(BaseImportObject):
    def __init__(self, obj, company_id, alive, db_obj=None):
        self.fields = OrderedDict(
            [
                ("id", IdField(db_obj["id"] if db_obj else None)),
                (
                    "owner_id",
                    ReferenceField(
                        value=db_obj["owner_id"] if db_obj else None,
                        ref_type="owner",
                        ref_field="id",
                        ref_data=[
                            ("caption", obj["owner_caption"]),
                            ("external_id", obj["owner_external_id"]),
                        ],
                    ),
                ),
            ]
        )

        self.db_obj = db_obj


class TestObjectWithReferenceImportHandler(BaseImportHandler):
    DB_TABLE = "test"

    IMPORT_TYPE = TestObjectWithReference
    ACTION = "test_with_ref_action"
    IMPORT_ID = "test_with_ref_id"
    CAPTION = "test_with_ref_name"

    def get_all_objs_from_database(self, company_id, external_ids=None):
        return []

    def get_columns_to_update(self):
        return []

    def get_columns_to_insert(self):
        return []


class ImportHandlerTest(TestCase):
    def test_populate_ref_fields(self):
        import_data1 = [
            {"test_name": "Test A", "test_id": "TEST_A", "test_action": "0"},
            {"test_name": "Test B", "test_id": "TEST_B", "test_action": "0"},
        ]

        import_data2 = [
            {
                "test_with_ref_name": "Test with ref A",
                "test_with_ref_id": "TEST_REF_A",
                "owner_caption": "Test B",
                "owner_external_id": "TEST_B",
                "test_with_ref_action": "0",
            }
        ]

        TestImportHandler.get_available_ids = mock_get_available_ids
        TestObjectWithReferenceImportHandler.get_available_ids = mock_get_available_ids

        import_handler1 = TestImportHandler(import_data1, 0)
        import_handler1.populate_insert_objs_ids()

        import_handler2 = TestObjectWithReferenceImportHandler(import_data2, 0)
        import_handler2.populate_insert_objs_ids()
        import_handler2.populate_ref_fields("owner", import_handler1.objs_to_insert)

        self.assertEqual(import_handler2.objs_to_insert[0].get_str(), "1;2")

    def test_undefined_fields(self):
        import_data = [{"test_name": "Test A", "test_id": "TEST_A", "test_action": "0"}]

        TestImportHandler.get_available_ids = mock_get_available_ids
        TestObjectWithReferenceImportHandler.get_available_ids = mock_get_available_ids

        import_handler = TestImportHandler(import_data, 0)

        with self.assertRaises(FieldEmptyError):
            _ = [r.get_str() for r in import_handler.objs_to_insert]

        import_handler.populate_insert_objs_ids()
        rows = [r.get_str() for r in import_handler.objs_to_insert]
        self.assertEqual(rows, ["1;Test A;TEST_A"])

        import_data2 = [
            {
                "test_with_ref_name": "Test with ref A",
                "test_with_ref_id": "TEST_REF_A",
                "owner_caption": "Test B",
                "owner_external_id": "TEST_B",
                "test_with_ref_action": "0",
            }
        ]

        import_handler2 = TestObjectWithReferenceImportHandler(import_data2, 0)
        import_handler2.populate_insert_objs_ids()

        with self.assertRaises(ReferenceObjectNotFound):
            import_handler2.populate_ref_fields("owner", import_handler.objs_to_insert)

    def test_create_undefined_fields(self):
        with self.assertRaises(FieldNotDefinedError):
            Field(mandatory=True, value=None)

        with self.assertRaises(FieldNotDefinedError):
            Field(mandatory=False, default=None, value=None, original_value=None)

        with self.assertRaises(FieldNotDefinedError):
            ReferenceField(
                value=None,
                ref_type=None,
                ref_field="id",
                ref_data=("caption", "Test B"),
            )

    def test_populate_already_populated_ref_fields(self):
        import_data2 = [
            {
                "test_with_ref_name": "Test with ref A",
                "test_with_ref_id": "TEST_REF_A",
                "owner_caption": "Test B",
                "owner_external_id": "TEST_B",
                "test_with_ref_action": "0",
            }
        ]

        TestObjectWithReferenceImportHandler.get_available_ids = mock_get_available_ids

        import_handler2 = TestObjectWithReferenceImportHandler(import_data2, 0)
        import_handler2.populate_insert_objs_ids()

        self.assertEqual(import_handler2.objs_to_insert[0].fields['owner_id'].value, None)

        import_handler2.objs_to_insert[0].fields['owner_id'].value = 2

        try:
            import_handler2.populate_ref_fields("owner", [])
        except:
            self.assertTrue(False)


if __name__ == "__main__":
    unittest.main()
