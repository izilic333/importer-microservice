import json
import os
from sqlalchemy.sql import select
from unittest import TestCase

from database.cloud_database.common.common import ConnectionForDatabases
from database.cloud_database.core.query import (
    MachineQueryOnCloud, ProductQueryOnCloud
)


TEST_DATA = json.loads(os.environ['TEST'])
COMPANY_ID = TEST_DATA['company_id']


class TestMachineQueryOnCloud(TestCase):
    def test_export_machines__field_events_tracking(self):
        results = MachineQueryOnCloud.export_machines(COMPANY_ID)

        self.assertTrue(type(results[0]['events_tracking']) == int)
        self.assertTrue(type(results[0]['routing']) == int)
        self.assertTrue(type(results[0]['stock_tracking']) == int)


class TestProductQueryOnCloud(TestCase):
    def test_export_product(self):
        results = ProductQueryOnCloud.export_product(COMPANY_ID)

        self.assertTrue('product_name' in results[0])
        self.assertTrue('product_id' in results[0])
        self.assertTrue('product_action' in results[0])
        self.assertTrue('price' in results[0])
        self.assertTrue('tax_rate' in results[0])
        self.assertTrue('product_category_id' in results[0])
        self.assertTrue('barcode' in results[0])
        self.assertTrue('weight' in results[0])
        self.assertTrue('packing' in results[0])
        self.assertTrue('product_packing_id' in results[0])
        self.assertTrue('description' in results[0])
        self.assertTrue('short_shelf_life' in results[0])
        self.assertTrue('age_verification' in results[0])
        self.assertTrue('capacity' in results[0])
        self.assertTrue('minimum_route_pickup' in results[0])
        self.assertTrue('blacklisted' in results[0])

        self.assertTrue(type(results[0]['price']) == str)
        self.assertTrue(type(results[0]['tax_rate']) == str)
        self.assertTrue(type(results[0]['blacklisted']) == int)

    def test_get_tax_rates_for_company(self):
        results = ProductQueryOnCloud.get_tax_rates_for_company(COMPANY_ID)
        self.assertTrue('value' in results[0])

    def test_get_products_for_company(self):
        result = ProductQueryOnCloud.get_products_for_company(COMPANY_ID)
        self.assertTrue('id' in result['results'][0])
        self.assertTrue('ext_id' in result['results'][0])
        self.assertTrue('name' in result['results'][0])
        self.assertTrue('alive' in result['results'][0])
        self.assertTrue('barcode' in result['results'][0])

    def test_products_machine_status(self):
        company_id = 46
        product_ext_ids = [
            'B123'
        #    'Q645', 'C598', 'C947', 'C948', 'C422',
        #    'J9987', 'B436', 'Q892', 'Q878', 'Q852',
        #    'Q277', 'Q862', 'A110', 'C881', 'C916',
        #    'C951', 'C568', 'C952', 'Q246'
        ]
        result = ProductQueryOnCloud.products_machine_status(
            company_id, product_ext_ids
        )
        #products_with_machines = result['results']
        #print(products_with_machines)
        self.assertFalse(result['allowed'])

    def test_products_planogram_status(self):
        """This is hardcoded test to my local cloud db data"""
        company_id = 46
        product_ext_ids = [
            'B123'
        ]
        result = ProductQueryOnCloud.products_planogram_status(company_id, product_ext_ids)
        self.assertFalse(result['allowed'])
        self.assertEqual({'B123': ['test_product_template']}, result['results'])
