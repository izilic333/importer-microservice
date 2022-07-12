from unittest import TestCase
from common.mixin.validation_const import machineParser, productParser


class TestMachineParser(TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_required_fields(self):
        result = machineParser['required']
        expected = [
            "machine_name", "machine_id",
            "machine_action", "machine_location_id",
            "machine_type_id"
        ]
        self.assertEqual(expected, result)
        self.assertEqual(len(result), 5)


class TestProductParser(TestCase):
    def test_required_fields(self):
        result = productParser['required']
        expected = [
            'product_name', 'product_id', 'product_action', 'price'
        ]
        self.assertEqual(expected, result)

    def test_all_fields(self):
        all_fields = productParser['all_fields']
        total = len(all_fields)
        expected = 16
        self.assertEqual(total, expected)
