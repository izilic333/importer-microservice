import json
import os
import unittest
from unittest import TestCase
from common.importers.cloud_db.planogram_helpers import PlanogramHandler
from common.mixin.mixin import mandatory_geo_location


TEST_DATA = json.loads(os.environ['TEST'])
COMPANY_ID = TEST_DATA['company_id']
USER_ID = TEST_DATA['user_id']


class StaticMethodsTestCase(TestCase):
    def test_planogram_name_repeat(self):

        # case 1
        planogram_data = [
            {"planogram_name": "test1", "planogram_id": "789456"},
            {"planogram_name": "test1", "planogram_id": "789456"},
            {"planogram_name": "test1", "planogram_id": "789456"}
        ]
        errors = PlanogramHandler.check_planogram_external_id_per_planogram_name(planogram_data)
        expected_errors = {}
        self.assertEqual(errors, expected_errors, "Fail case 1")

        # case 2
        planogram_data = [
            {"planogram_name": "test1", "planogram_id": "789456"},
            {"planogram_name": "test1", "planogram_id": "789456"},
            {"planogram_name": "test2", "planogram_id": "789456"}
        ]
        errors = PlanogramHandler.check_planogram_external_id_per_planogram_name(planogram_data)
        expected_errors = {'test2': '789456'}
        self.assertEqual(errors, expected_errors, "Fail case 2")

        # case 3
        planogram_data = [
            {"planogram_name": "test1", "planogram_id": "789s456"},
            {"planogram_name": "test1", "planogram_id": "789456"},
            {"planogram_name": "test2", "planogram_id": "789456"}
        ]
        errors = PlanogramHandler.check_planogram_external_id_per_planogram_name(planogram_data)
        test1 = errors['test1']
        test1.sort()
        expected_errors = {'test2': '789456', 'test1': ['789456', '789s456']}
        self.assertListEqual(test1, expected_errors["test1"])
        self.assertEqual(errors["test2"], expected_errors["test2"])

        # case 4
        planogram_data = [
            {"planogram_name": "test1", "planogram_id": "789456"},
            {"planogram_name": "test1", "planogram_id": "789456"},
            {"planogram_name": "test2", "planogram_id": "789456"},
            {"planogram_name": "test2", "planogram_id": "789456"}
        ]
        errors = PlanogramHandler.check_planogram_external_id_per_planogram_name(planogram_data)
        expected_errors = {'test2': '789456'}
        self.assertEqual(errors, expected_errors, "Fail case 4")

    def test_planogram_external_id_repeat(self):

        # case 1
        planogram_data = [
            {"planogram_name": "test1", "planogram_id": "789456"},
            {"planogram_name": "test1", "planogram_id": "789456"},
            {"planogram_name": "test1", "planogram_id": "789456"}
        ]
        errors = PlanogramHandler.check_planogram_name_per_external_id(planogram_data)
        expected_errors = {}
        self.assertEqual(errors, expected_errors, "Fail case 1")
        # case 2
        planogram_data = [
            {"planogram_name": "test1", "planogram_id": "789456"},
            {"planogram_name": "test1", "planogram_id": "789456"},
            {"planogram_name": "test2", "planogram_id": "789456"}
        ]
        errors = PlanogramHandler.check_planogram_name_per_external_id(planogram_data)
        expected_errors = {'789456': ['test1', 'test2']}
        errors = errors['789456']
        errors.sort()
        self.assertListEqual(errors, expected_errors['789456'], "Fail case 2")

        # case 3
        planogram_data = [
            {"planogram_name": "test1", "planogram_id": "789s456"},
            {"planogram_name": "test1", "planogram_id": "789456"},
            {"planogram_name": "test2", "planogram_id": "789456"}
        ]
        errors = PlanogramHandler.check_planogram_name_per_external_id(planogram_data)
        errors = errors['789456']
        errors.sort()
        expected_errors = {'789456': ['test1', 'test2']}
        self.assertListEqual(errors, expected_errors['789456'])

        # case 4
        planogram_data = [
            {"planogram_name": "test1", "planogram_id": "789456"},
            {"planogram_name": "test1", "planogram_id": "789456"},
            {"planogram_name": "test2", "planogram_id": "789456"},
            {"planogram_name": "test2", "planogram_id": "789456"}
        ]
        errors = PlanogramHandler.check_planogram_name_per_external_id(planogram_data)
        errors = errors['789456']
        errors.sort()
        expected_errors = {'789456': ['test1', 'test2']}
        self.assertListEqual(errors, expected_errors['789456'])

    def test_mandatory_geo_location_with_address_latitude_longitude(self):

        location_address = 'Test_location'
        latitude = '13.3848259'
        longitude = '54'

        errors = mandatory_geo_location(address=location_address, latitude=latitude, longitude=longitude)

        expected_errors = {
            'mandatory_fields': ['latitude', 'longitude'],
            'valid': True,
            'message': "location_address, latitude and longitude are defined, latitude and longitude become mandatory!"
        }

        self.assertEqual(errors, expected_errors, "Fail test_mandatory_geo_location_with_address_latitude_longitude")

    def test_mandatory_geo_location_with_latitude_longitude(self):

        location_address = ''
        latitude = '13.3848259'
        longitude = '54'

        errors = mandatory_geo_location(address=location_address, latitude=latitude, longitude=longitude)

        expected_errors = {
            'mandatory_fields': ['latitude', 'longitude'],
            'valid': True,
            'message': "latitude and longitude are defined, so this fields become mandatory fields!"
        }

        self.assertEqual(errors, expected_errors, "Fail test_mandatory_geo_location_with_latitude_longitude")

    def test_mandatory_geo_location_with_address(self):
        location_address = 'Test_location'
        latitude = ''
        longitude = ''

        errors = mandatory_geo_location(address=location_address, latitude=latitude, longitude=longitude)

        expected_errors = {
            'mandatory_fields': ['location_address'],
            'valid': True,
            'message': 'location_address are defined, so this field becomes mandatory field!'
        }

        self.assertEqual(errors, expected_errors, "Fail test_mandatory_geo_location_with_address")

    def test_mandatory_geo_location_without_address_latitude_longitude(self):

        location_address = ''
        latitude = ''
        longitude = ''

        errors = mandatory_geo_location(address=location_address, latitude=latitude, longitude=longitude)

        expected_errors = {
            'mandatory_fields': ['location_address', 'latitude', 'longitude'],
            'valid': False,
            'message': 'location_address or latitude and longitude are mandatory!'
        }

        self.assertEqual(errors, expected_errors, "Fail test_mandatory_geo_location_without_address_latitude_longitude")

    def test_mandatory_geo_location_with_address_longitude(self):

        location_address = 'Test_location'
        latitude = ''
        longitude = '54'

        errors = mandatory_geo_location(address=location_address, latitude=latitude, longitude=longitude)

        expected_errors = {
            'mandatory_fields': ['location_address'],
            'valid': True,
            'message': 'location_address are defined, so this field becomes mandatory field!'
        }

        self.assertEqual(errors, expected_errors, "Fail test_mandatory_geo_location_with_address_longitude")

    def test_mandatory_geo_location_with_longitude(self):

        location_address = ''
        latitude = ''
        longitude = '54'

        errors = mandatory_geo_location(address=location_address, latitude=latitude, longitude=longitude)

        expected_errors = {
            'mandatory_fields': ['location_address', 'latitude', 'longitude'],
            'valid': False,
            'message': 'location_address or latitude and longitude are mandatory!'
        }

        self.assertEqual(errors, expected_errors, "Fail test_mandatory_geo_location_with_longitude")


if __name__ == '__main__':
    unittest.main()
