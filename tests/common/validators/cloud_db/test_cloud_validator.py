import random
from decimal import Decimal
import json
import os
import unittest
from unittest import TestCase
from unittest.mock import patch
import pandas as pd

from importer.common.importers.cloud_db.planogram_helpers import PlanogramHandler, planogram_processor
from importer.common.mixin.enum_errors import EnumValidationMessage as Const
from importer.common.mixin.validation_const import get_import_type_by_name, ImportAction
from importer.common.validators.cloud_db.cloud_validator import (
    FileOnCloudValidator, tax_rate_doesnt_exist, tax_rate_duplicates
)
from importer.common.validators.csv.csv_validator import generate_elastic_process
from importer.database.cloud_database.core.query import (
    PlanogramQueryOnCloud, ProductQueryOnCloud, CustomUserQueryOnCloud,
    ProductRotationGroupQueryOnCloud)


TEST_DATA = json.loads(os.environ['TEST'])
COMPANY_ID = TEST_DATA['company_id']
USER_ID = TEST_DATA['user_id']
PRODUCT_ID = TEST_DATA['product_id']


class TestCloudValidator(TestCase):
    @patch.object(ProductQueryOnCloud, 'get_tax_rates_for_company')
    def test_tax_rate_doesnt_exist(self, mock_get_tax_rates_for_company):
        mock_get_tax_rates_for_company.return_value = [
            {'value': Decimal('0.0000')},
            {'value': Decimal('5.5000')},
            {'value': Decimal('20.0000')}
        ]

        # case tax rate exists
        tax_rates_from_file = [
            '0', '0.00', '20', '5.5',
            '5.50', '5.500', '5.5000'
        ]
        result = tax_rate_doesnt_exist(COMPANY_ID, tax_rates_from_file)
        self.assertFalse(result)

        # case tax rate doesn't exist
        tax_rates_from_file = ['100']
        result = tax_rate_doesnt_exist(COMPANY_ID, tax_rates_from_file)
        self.assertEqual(result, ['100'])

    @patch.object(ProductQueryOnCloud, 'get_tax_rates_for_company')
    def test_tax_rate_duplicates(self, mock_get_tax_rates_for_company):
        mock_get_tax_rates_for_company.return_value = [
            {'value': Decimal('0.0000')},
            {'value': Decimal('5.5000')},
            {'value': Decimal('5.5000')},
            {'value': Decimal('20.0000')}
        ]

        # case tax rate duplicate found
        result = tax_rate_duplicates(COMPANY_ID)
        self.assertTrue(result)
        self.assertEqual(result, [Decimal('5.5000')])

        # case tax rate duplicate not found
        mock_get_tax_rates_for_company.return_value = [
            {'value': Decimal('0.0000')},
            {'value': Decimal('5.5000')},
            {'value': Decimal('20.0000')}
        ]
        result = tax_rate_duplicates(COMPANY_ID)
        self.assertFalse(result)
        self.assertEqual(result, [])

    @patch.object(ProductQueryOnCloud, 'get_tax_rates_for_company')
    def test_tax_rate_duplicates__one_or_zero_tax_rate_in_db(
        self,
        mock_get_tax_rates_for_company
    ):
        # case one existing tax rate
        mock_get_tax_rates_for_company.return_value = [
            {'value': Decimal('0.0000')}
        ]

        result = tax_rate_duplicates(COMPANY_ID)
        self.assertFalse(result)
        self.assertEqual(result, [])

        # case no existing tax rate
        mock_get_tax_rates_for_company.return_value = []

        result = tax_rate_duplicates(COMPANY_ID)
        self.assertFalse(result)
        self.assertEqual(result, [])


class TestFileOnCloudValidator(TestCase):
    def setUp(self):
        user_token = CustomUserQueryOnCloud.get_auth_token_from_user_id(int(USER_ID))
        token = self.token = user_token['token']
        elastic_hash = self.elastic_hash = generate_elastic_process(
            COMPANY_ID,
            'products',
            'FILE'
        )
        self.body = {
            'elastic_hash': elastic_hash,
            'company_id': COMPANY_ID,
            'data': [],
            'type': '',
            'input_file': '',
            'token': token,
            'email': '',
            'language': ''
        }

    @patch.object(ProductQueryOnCloud, 'get_products_for_company')
    @patch.object(FileOnCloudValidator, 'write_errors')
    def test_validate_product__update_fail_not_exists_on_cloud(
        self,
        mock_write_errors,
        mock_get_products_for_company
    ):
        # data from .csv file
        data = [
            {
                'capacity': '8', 'weight': '0.23',
                'packing': '1', 'product_id': 'B449',
                'minimum_route_pickup': '0', 'short_shelf_life': '120',
                'description': 'Belvita Dark Chocolate Bar',
                'product_name': 'BELVITA CHOCOLAT 50GR X30',
                'default_barcode': 'ABC123', 'age_verification': '100',
                'product_category_id': '13', 'product_action': '1',
                'product_packing_id': 'P456', 'price': '1.3',
                'tax_rate': '5.5'
            }
        ]

        body = {
            'elastic_hash': self.elastic_hash,
            'company_id': COMPANY_ID,
            'data': data,
            'type': 'products',
            'input_file': '',
            'token': self.token,
            'email': '',
            'language': ''
        }
        focv = FileOnCloudValidator(body)

        # mock what is in the cloud database
        mock_get_products_for_company = {
            'status': True,
            'result': [],
            'message': ''
        }

        focv.validate_product()

        mock_write_errors.assert_called_with(error_type=2)

    @patch('common.validators.cloud_db.cloud_validator.tax_rate_doesnt_exist')
    @patch.object(ProductQueryOnCloud, 'get_products_for_company')
    @patch.object(FileOnCloudValidator, 'write_history_and_publish')
    @patch.object(FileOnCloudValidator, 'write_errors')
    def test_validate_product__update_success(
        self,
        mock_write_errors,
        mock_write_history_and_publish,
        mock_get_products_for_company,
        mock_tax_rate_doesnt_exist,
    ):
        # data from .csv file
        data = [
            {
                'capacity': '8', 'weight': '0.23',
                'packing': '1', 'product_id': 'B449',
                'minimum_route_pickup': '0', 'short_shelf_life': '120',
                'description': 'Belvita Dark Chocolate Bar',
                'product_name': 'BELVITA CHOCOLAT 50GR X30',
                'default_barcode': 'ABC123', 'age_verification': '100',
                'product_category_id': '13', 'product_action': '1',
                'product_packing_id': 'P456', 'price': '1.3',
                'tax_rate': '5.5'
            }
        ]

        body = {
            'elastic_hash': self.elastic_hash,
            'company_id': COMPANY_ID,
            'data': data,
            'type': 'products',
            'input_file': '',
            'token': self.token,
            'email': '',
            'language': ''
        }
        focv = FileOnCloudValidator(body)
        # mock what is in the cloud database
        mock_get_products_for_company.return_value = {
            'status': True,
            'results': [
                {
                    'ext_id': 'B449',
                    'alive': True,
                    'id': 1,
                    'name': 'BELVITA CHOCOLAT 50GR X30',
                    'barcode': '789',
                    'barcode1': '',
                    'barcode2': '',
                    'barcode3': '',
                    'barcode4': '',
                    'packing_id': '',
                }
            ],
            'message': ''
        }
        mock_tax_rate_doesnt_exist.return_value = []

        result = focv.validate_product()

        self.assertTrue(mock_write_history_and_publish.called)
        self.assertFalse(mock_write_errors.called)
        self.assertTrue(result)

    @patch('common.validators.cloud_db.cloud_validator.tax_rate_doesnt_exist')
    @patch.object(ProductQueryOnCloud, 'get_products_for_company')
    @patch.object(FileOnCloudValidator, 'write_history_and_publish')
    @patch.object(FileOnCloudValidator, 'write_errors')
    def test_validate_product__create_success(
        self,
        mock_write_errors,
        mock_write_history_and_publish,
        mock_get_products_for_company,
        mock_tax_rate_doesnt_exist
    ):
        # data from .csv file
        data = [
            {
                'capacity': '8', 'weight': '0.100',
                'packing': '1', 'product_id': 'B999',
                'minimum_route_pickup': '0', 'short_shelf_life': '120',
                'description': 'Belvita Dark Chocolate Bar',
                'product_name': 'Milka 100g x10',
                'default_barcode': 'ABC123', 'age_verification': '100',
                'product_category_id': '13', 'product_action': '0',
                'product_packing_id': 'P456', 'price': '1.3',
                'tax_rate': '5.5'
            }
        ]

        # prepare class
        body = {
            'elastic_hash': self.elastic_hash,
            'company_id': COMPANY_ID,
            'data': data,
            'type': 'products',
            'input_file': '',
            'token': self.token,
            'email': '',
            'language': ''
        }
        focv = FileOnCloudValidator(body)

        # mock what is in the cloud database
        mock_get_products_for_company.return_value = {
            'status': True,
            'results': [
                {
                    'ext_id': 'B449',
                    'alive': True,
                    'id': 1,
                    'name': 'BELVITA CHOCOLAT 50GR X30',
                    'barcode': '789',
                    'barcode1': '',
                    'barcode2': '',
                    'barcode3': '',
                    'barcode4': '',
                    'packing_id': ''
                }
            ],
            'message': ''
        }
        mock_tax_rate_doesnt_exist.return_value = []
        # call tested method
        result = focv.validate_product()
        
        # assert results
        self.assertTrue(mock_write_history_and_publish.called)
        self.assertFalse(mock_write_errors.called)
        self.assertTrue(result)

    @patch('common.validators.cloud_db.cloud_validator.tax_rate_doesnt_exist')
    @patch.object(ProductQueryOnCloud, 'get_products_for_company')
    @patch.object(FileOnCloudValidator, 'write_history_and_publish')
    @patch.object(FileOnCloudValidator, 'write_errors')
    def test_validate_product__create_fail_already_exists(
        self,
        mock_write_errors,
        mock_write_history_and_publish,
        mock_get_products_for_company,
        mock_tax_rate_doesnt_exist
    ):
        # data from .csv file
        data = [
            {
                'capacity': '8', 'weight': '0.100',
                'packing': '1', 'product_id': 'B999',
                'minimum_route_pickup': '0', 'short_shelf_life': '120',
                'description': 'Belvita Dark Chocolate Bar',
                'product_name': 'Milka 100g x10',
                'default_barcode': 'ABC123', 'age_verification': '100',
                'product_category_id': '13', 'product_action': '0',
                'product_packing_id': 'P456', 'price': '1.3',
                'tax_rate': '5.5'
            }
        ]

        # prepare class
        body = {
            'elastic_hash': self.elastic_hash,
            'company_id': COMPANY_ID,
            'data': data,
            'type': 'products',
            'input_file': '',
            'token': self.token,
            'email': '',
            'language': ''
        }
        focv = FileOnCloudValidator(body)

        # mock what is in the cloud database
        mock_get_products_for_company.return_value = {
            'status': True,
            'results': [
                {
                    'ext_id': 'B999',
                    'alive': True,
                    'id': 1,
                    'name': 'BELVITA CHOCOLAT 50GR X30',
                    'barcode': '789',
                    'barcode1': '',
                    'barcode2': '',
                    'barcode3': '',
                    'barcode4': '',
                    'packing_id': ''
                }
            ],
            'message': ''
        }
        mock_tax_rate_doesnt_exist.return_value = []
        # call tested method
        result = focv.validate_product()

        # assert results

        # because csv validator don't publish file to cloud validator if file already processed
        # so every file sent for cloud validator will be processed, (this test with this case
        # fits better for csv validator)
        self.assertTrue(mock_write_history_and_publish.called)
        self.assertFalse(mock_write_errors.called)
        self.assertTrue(result)

    @patch('common.validators.cloud_db.cloud_validator.tax_rate_doesnt_exist')
    @patch.object(ProductQueryOnCloud, 'get_products_for_company')
    @patch.object(FileOnCloudValidator, 'write_history_and_publish')
    @patch.object(FileOnCloudValidator, 'write_errors')
    def test_validate_product__delete_success(
        self,
        mock_write_errors,
        mock_write_history_and_publish,
        mock_get_products_for_company,
        mock_tax_rate_doesnt_exist
    ):
        # data from .csv file
        data = [
            {
                'capacity': '8', 'weight': '0.100',
                'packing': '1', 'product_id': 'B999',
                'minimum_route_pickup': '0', 'short_shelf_life': '120',
                'description': 'Belvita Dark Chocolate Bar',
                'product_name': 'Milka 100g x10',
                'barcode': 'ABC123', 'age_verification': '100',
                'product_category_id': '13', 'product_action': '2',
                'product_packing_id': 'P456', 'price': '1.3',
                'tax_rate': '5.5'
            }
        ]

        # prepare class
        body = {
            'elastic_hash': self.elastic_hash,
            'company_id': COMPANY_ID,
            'data': data,
            'type': 'products',
            'input_file': '',
            'token': self.token,
            'email': '',
            'language': ''
        }
        focv = FileOnCloudValidator(body)

        # mock what is in the cloud database
        mock_get_products_for_company.return_value = {
            'status': True,
            'results': [
                {
                    'ext_id': 'B999',
                    'alive': True,
                    'id': 1,
                    'name': 'BELVITA CHOCOLAT 50GR X30',
                    'barcode': '789',
                    'barcode1': '',
                    'barcode2': '',
                    'barcode3': '',
                    'barcode4': '',

                    'packing_id': ''
                }
            ],
            'message': ''
        }
        mock_tax_rate_doesnt_exist.return_value = []
        # call tested method
        result = focv.validate_product()

        # assert results
        self.assertTrue(mock_write_history_and_publish.called)
        self.assertFalse(mock_write_errors.called)
        self.assertTrue(result)

    @patch('common.validators.cloud_db.cloud_validator.tax_rate_doesnt_exist')
    @patch.object(ProductQueryOnCloud, 'get_products_for_company')
    @patch.object(FileOnCloudValidator, 'write_history_and_publish')
    @patch.object(FileOnCloudValidator, 'write_errors')
    def test_validate_product__delete_fail_not_exists(
        self,
        mock_write_errors,
        mock_write_history_and_publish,
        mock_get_products_for_company,
        mock_tax_rate_doesnt_exist
    ):
        # data from .csv file
        data = [
            {
                'capacity': '8', 'weight': '0.100',
                'packing': '1', 'product_id': 'B999',
                'minimum_route_pickup': '0', 'short_shelf_life': '120',
                'description': 'Belvita Dark Chocolate Bar',
                'product_name': 'Milka 100g x10',
                'barcode': 'ABC123', 'age_verification': '100',
                'product_category_id': '13', 'product_action': '2',
                'product_packing_id': 'P456', 'price': '1.3',
                'tax_rate': '5.5'
            }
        ]

        # prepare class
        body = {
            'elastic_hash': self.elastic_hash,
            'company_id': COMPANY_ID,
            'data': data,
            'type': 'products',
            'input_file': '',
            'token': self.token,
            'email': '',
            'language': ''
        }
        focv = FileOnCloudValidator(body)

        # mock what is in the cloud database
        mock_get_products_for_company.return_value = {
            'status': True,
            'results': [
                {
                    'ext_id': 'B991',
                    'alive': True,
                    'id': 1,
                    'name': 'BELVITA CHOCOLAT 50GR X30',
                    'barcode': '789',
                    'barcode1': '',
                    'barcode2': '',
                    'barcode3': '',
                    'barcode4': '',
                    'packing_id': ''
                }
            ],
            'message': ''
        }
        mock_tax_rate_doesnt_exist.return_value = []
        # call tested method
        result = focv.validate_product()

        # assert results
        self.assertFalse(mock_write_history_and_publish.called)
        self.assertTrue(mock_write_errors.called)
        self.assertFalse(result)

    @patch('common.validators.cloud_db.cloud_validator.tax_rate_doesnt_exist')
    @patch.object(ProductQueryOnCloud, 'get_products_for_company')
    @patch.object(FileOnCloudValidator, 'write_history_and_publish')
    @patch.object(FileOnCloudValidator, 'write_errors')
    def test_validate_product__action_50_product_exists_alive(
        self,
        mock_write_errors,
        mock_write_history_and_publish,
        mock_get_products_for_company,
        mock_tax_rate_doesnt_exist
    ):
        # data from .csv file
        data = [
            {
                'capacity': '8', 'weight': '0.100',
                'packing': '1', 'product_id': 'B999',
                'minimum_route_pickup': '0', 'short_shelf_life': '120',
                'description': 'Belvita Dark Chocolate Bar',
                'product_name': 'Milka 100g x10',
                'default_barcode': 'ABC123', 'age_verification': '100',
                'product_category_id': '13', 'product_action': '50',
                'product_packing_id': 'P456', 'price': '1.3',
                'tax_rate': '5.5'
            }
        ]

        # prepare class
        body = {
            'elastic_hash': self.elastic_hash,
            'company_id': COMPANY_ID,
            'data': data,
            'type': 'products',
            'input_file': '',
            'token': self.token,
            'email': '',
            'language': ''
        }
        focv = FileOnCloudValidator(body)

        # mock what is in the cloud database
        mock_get_products_for_company.return_value = {
            'status': True,
            'results': [
                {
                    'ext_id': 'B999',
                    'alive': True,
                    'id': 1,
                    'name': 'BELVITA CHOCOLAT 50GR X30',
                    'barcode': '789',
                    'barcode1': '',
                    'barcode2': '',
                    'barcode3': '',
                    'barcode4': '',
                    'packing_id': ''
                }
            ],
            'message': ''
        }
        mock_tax_rate_doesnt_exist.return_value = []
        # call tested method
        result = focv.validate_product()

        # assert results
        self.assertTrue(mock_write_history_and_publish.called)
        self.assertFalse(mock_write_errors.called)
        self.assertTrue(result)
        # assert action field changed to UPDATE
        self.assertEqual(1, focv.data[0]['product_action'])

    @patch('common.validators.cloud_db.cloud_validator.tax_rate_doesnt_exist')
    @patch.object(ProductQueryOnCloud, 'get_products_for_company')
    @patch.object(FileOnCloudValidator, 'write_history_and_publish')
    @patch.object(FileOnCloudValidator, 'write_errors')
    def test_validate_product__action_50_product_exists_not_alive(
        self,
        mock_write_errors,
        mock_write_history_and_publish,
        mock_get_products_for_company,
        mock_tax_rate_doesnt_exist
    ):
        # data from .csv file
        data = [
            {
                'capacity': '8', 'weight': '0.100',
                'packing': '1', 'product_id': 'B999',
                'minimum_route_pickup': '0', 'short_shelf_life': '120',
                'description': 'Belvita Dark Chocolate Bar',
                'product_name': 'Milka 100g x10',
                'default_barcode': 'ABC123', 'age_verification': '100',
                'product_category_id': '13', 'product_action': '50',
                'product_packing_id': 'P456', 'price': '1.3',
                'tax_rate': '5.5'
            }
        ]

        # prepare class
        body = {
            'elastic_hash': self.elastic_hash,
            'company_id': COMPANY_ID,
            'data': data,
            'type': 'products',
            'input_file': '',
            'token': self.token,
            'email': '',
            'language': ''
        }
        focv = FileOnCloudValidator(body)

        # mock what is in the cloud database
        mock_get_products_for_company.return_value = {
            'status': True,
            'results': [
                {
                    'ext_id': 'B999',
                    'alive': False,
                    'id': 1,
                    'name': 'BELVITA CHOCOLAT 50GR X30',
                    'barcode': '789',
                    'barcode1': '',
                    'barcode2': '',
                    'barcode3': '',
                    'barcode4': '',
                    'packing_id': ''
                }
            ],
            'message': ''
        }
        mock_tax_rate_doesnt_exist.return_value = []
        # call tested method
        result = focv.validate_product()

        # assert results
        self.assertTrue(mock_write_history_and_publish.called)
        self.assertFalse(mock_write_errors.called)
        self.assertTrue(result)
        # assert action field changed to CREATE
        self.assertEqual(0, focv.data[0]['product_action'])

    @patch('common.validators.cloud_db.cloud_validator.tax_rate_doesnt_exist')
    @patch.object(ProductQueryOnCloud, 'get_products_for_company')
    @patch.object(FileOnCloudValidator, 'write_history_and_publish')
    @patch.object(FileOnCloudValidator, 'write_errors')
    def test_validate_product__action_50_product_doesnt_exists(
        self,
        mock_write_errors,
        mock_write_history_and_publish,
        mock_get_products_for_company,
        mock_tax_rate_doesnt_exist
    ):
        # data from .csv file
        data = [
            {
                'capacity': '8', 'weight': '0.100',
                'packing': '1', 'product_id': 'B999',
                'minimum_route_pickup': '0', 'short_shelf_life': '120',
                'description': 'Belvita Dark Chocolate Bar',
                'product_name': 'Milka 100g x10',
                'default_barcode': 'ABC123', 'age_verification': '100',
                'product_category_id': '13', 'product_action': '50',
                'product_packing_id': 'P456', 'price': '1.3',
                'tax_rate': '5.5'
            }
        ]

        # prepare class
        body = {
            'elastic_hash': self.elastic_hash,
            'company_id': COMPANY_ID,
            'data': data,
            'type': 'products',
            'input_file': '',
            'token': self.token,
            'email': '',
            'language': ''
        }
        focv = FileOnCloudValidator(body)

        # mock what is in the cloud database
        mock_get_products_for_company.return_value = {
            'status': True,
            'results': [
                {
                    'ext_id': 'B111',
                    'alive': True,
                    'id': 1,
                    'name': 'BELVITA CHOCOLAT 50GR X30',
                    'barcode': '789',
                    'barcode1': '',
                    'barcode2': '',
                    'barcode3': '',
                    'barcode4': '',
                    'packing_id': ''
                }
            ],
            'message': ''
        }
        mock_tax_rate_doesnt_exist.return_value = []
        # call tested method
        result = focv.validate_product()

        # assert results
        self.assertTrue(mock_write_history_and_publish.called)
        self.assertFalse(mock_write_errors.called)
        self.assertTrue(result)
        # assert action field changed to CREATE
        self.assertEqual(2, focv.data[0]['product_action'])

    @patch('common.validators.cloud_db.cloud_validator.tax_rate_doesnt_exist')
    @patch.object(FileOnCloudValidator, 'write_errors')
    def test_validate_product__tax_rate_doesnt_exists(
        self,
        mock_write_errors,
        mock_tax_rate_doesnt_exist
    ):
        # data from .csv file
        data = [
            {
                'capacity': '8', 'weight': '0.100',
                'packing': '1', 'product_id': 'B999',
                'minimum_route_pickup': '0', 'short_shelf_life': '120',
                'description': 'Belvita Dark Chocolate Bar',
                'product_name': 'Milka 100g x10',
                'default_barcode': 'ABC123', 'age_verification': '100',
                'product_category_id': '13', 'product_action': '50',
                'product_packing_id': 'P456', 'price': '1.3',
                'tax_rate': '5.55'
            }
        ]

        # prepare class
        body = {
            'elastic_hash': self.elastic_hash,
            'company_id': COMPANY_ID,
            'data': data,
            'type': 'products',
            'input_file': '',
            'token': self.token,
            'email': '',
            'language': ''
        }
        focv = FileOnCloudValidator(body)
        mock_tax_rate_doesnt_exist.return_value = ['5.55']

        # call tested method
        result = focv.validate_product()

        # assert results
        self.assertTrue(mock_write_errors.called)
        self.assertFalse(result)
        self.assertEqual(
            'Tax rate 5.55 does not exist for this company. Please choose an existing tax rate.',
            focv.errors[0]['message']
        )
        self.assertEqual(
            '5.55',
            focv.errors[0]['record']
        )

    @patch('common.validators.cloud_db.cloud_validator.tax_rate_duplicates')
    @patch.object(FileOnCloudValidator, 'write_errors')
    def test_validate_product__tax_rate_duplicate_found(
        self,
        mock_write_errors,
        mock_tax_rate_duplicates
    ):
        # data from .csv file
        data = [
            {
                'capacity': '8', 'weight': '0.100',
                'packing': '1', 'product_id': 'B999',
                'minimum_route_pickup': '0', 'short_shelf_life': '120',
                'description': 'Belvita Dark Chocolate Bar',
                'product_name': 'Milka 100g x10',
                'default_barcode': 'ABC123', 'age_verification': '100',
                'product_category_id': '13', 'product_action': '50',
                'product_packing_id': 'P456', 'price': '1.3',
                'tax_rate': '5.55'
            }
        ]

        # prepare class
        body = {
            'elastic_hash': self.elastic_hash,
            'company_id': COMPANY_ID,
            'data': data,
            'type': 'products',
            'input_file': '',
            'token': self.token,
            'email': '',
            'language': ''
        }
        focv = FileOnCloudValidator(body)
        mock_tax_rate_duplicates.return_value = ['5.5000']

        # call tested method
        result = focv.validate_product()

        # assert results
        self.assertTrue(mock_write_errors.called)
        self.assertFalse(result)
        self.assertEqual(
            'Tax rates should be unique on company level. Currently you have multiple instances of the same tax rate, 5.5000.',
            focv.errors[0]['message']
        )
        self.assertEqual(
            '5.5000',
            focv.errors[0]['record']
        )

    @patch.object(ProductQueryOnCloud, 'get_tax_rates_for_company')
    @patch.object(FileOnCloudValidator, 'write_errors')
    def test_validate_product__tax_rate_zero_on_existing_tax_rate_zero(
        self,
        mock_write_errors,
        mock_get_tax_rates_for_company
    ):
        # data from .csv file
        data = [
            {
                'capacity': '8', 'weight': '0.100',
                'packing': '1', 'product_id': 'B999',
                'minimum_route_pickup': '0', 'short_shelf_life': '120',
                'description': 'Belvita Dark Chocolate Bar',
                'product_name': 'Milka 100g x10',
                'default_barcode': 'ABC123', 'age_verification': '100',
                'product_category_id': '13', 'product_action': '50',
                'product_packing_id': 'P456', 'price': '1.3',
                'tax_rate': '0.00'
            }
        ]

        # prepare class
        body = {
            'elastic_hash': self.elastic_hash,
            'company_id': COMPANY_ID,
            'data': data,
            'type': 'products',
            'input_file': '',
            'token': self.token,
            'email': '',
            'language': ''
        }
        focv = FileOnCloudValidator(body)
        mock_get_tax_rates_for_company.return_value = [
            {'value': Decimal('0.0000')}
        ]

        # call tested method
        result = focv.validate_product()

        # assert results
        self.assertFalse(mock_write_errors.called)
        self.assertTrue(result)

    @patch.object(ProductQueryOnCloud, 'get_tax_rates_for_company')
    @patch.object(FileOnCloudValidator, 'write_errors')
    def test_validate_product__missing_tax_rate_field(
        self,
        mock_write_errors,
        mock_get_tax_rates_for_company
    ):
        # data from .csv file
        data = [
            {
                'capacity': '8', 'weight': '0.100',
                'packing': '1', 'product_id': 'B999',
                'minimum_route_pickup': '0', 'short_shelf_life': '120',
                'description': 'Belvita Dark Chocolate Bar',
                'product_name': 'Milka 100g x10',
                'default_barcode': 'ABC123', 'age_verification': '100',
                'product_category_id': '13', 'product_action': '50',
                'product_packing_id': 'P456', 'price': '1.3'
            }
        ]

        # prepare class
        body = {
            'elastic_hash': self.elastic_hash,
            'company_id': COMPANY_ID,
            'data': data,
            'type': 'products',
            'input_file': '',
            'token': self.token,
            'email': '',
            'language': ''
        }
        focv = FileOnCloudValidator(body)
        mock_get_tax_rates_for_company.return_value = [
            {'value': Decimal('0.0000')}
        ]

        # call tested method
        result = focv.validate_product()

        # assert results
        self.assertFalse(mock_write_errors.called)
        self.assertTrue(result)

    @patch.object(ProductQueryOnCloud, 'get_products_for_company')
    @patch.object(FileOnCloudValidator, 'write_errors')
    def test_validate_product__barcode_uniqueness_create_fail(
        self,
        mock_write_errors,
        mock_products_for_company
    ):
        """
        Case: create product fails, because another product in db
        has the same barcode.
        """
        # data from .csv file
        data = [
            {
                'capacity': '8', 'weight': '0.100',
                'packing': '1', 'product_id': 'B999',
                'minimum_route_pickup': '0', 'short_shelf_life': '120',
                'description': 'Belvita Dark Chocolate Bar',
                'product_name': 'Milka 100g x10',
                'default_barcode': '789', 'age_verification': '100',
                'product_category_id': '13', 'product_action': '0',
                'product_packing_id': 'P456', 'price': '1.3'
            }
        ]

        # prepare class
        body = {
            'elastic_hash': self.elastic_hash,
            'company_id': COMPANY_ID,
            'data': data,
            'type': 'products',
            'input_file': '',
            'token': self.token,
            'email': '',
            'language': ''
        }
        focv = FileOnCloudValidator(body)
        mock_products_for_company.return_value = {
            'status': True,
            'results': [
                {
                    'ext_id': 'B111',
                    'alive': True,
                    'id': 1,
                    'name': 'BELVITA CHOCOLAT 50GR X30',
                    'barcode': '789',
                    'barcode1': '',
                    'barcode2': '',
                    'barcode3': '',
                    'barcode4': '',
                    'packing_id': ''
                }
            ],
            'message': ''
        }

        # call tested method
        result = focv.validate_product()

        # assert results
        self.assertTrue(mock_write_errors.called)
        self.assertEqual(
            Const.FIELD_NOT_UNIQUE_PRODUCT_EXTERNAL_ID.value['en'].format('barcode', '789', 'B111'),
            focv.errors[0]['message']
        )
        self.assertFalse(result)

    @patch.object(ProductQueryOnCloud, 'get_products_for_company')
    @patch.object(FileOnCloudValidator, 'write_errors')
    def test_validate_product__values_uniqueness_fail(
        self,
        mock_write_errors,
        mock_products_for_company
    ):
        """
        Case: action create product fails, because in data from file
        there is a duplication of barcode and packing_id is not unique
        against the db.
        """
        # data from .csv file
        data = [
            {
                'product_id': 'B999',
                'product_name': 'Milka 100g x10',
                'default_barcode': '789',
                'product_action': '0',
                'price': '1.3',
                'product_packing_id': 'CODE123'
            },
            {
                'product_id': 'A222',
                'product_name': 'Belvita 150g x6',
                'default_barcode': '789',
                'product_action': '0',
                'price': '1.3'
            }
        ]

        # prepare class
        body = {
            'elastic_hash': self.elastic_hash,
            'company_id': COMPANY_ID,
            'data': data,
            'type': 'products',
            'input_file': '',
            'token': self.token,
            'email': '',
            'language': ''
        }
        focv = FileOnCloudValidator(body)
        mock_products_for_company.return_value = {
            'status': True,
            'results': [
                {
                    'ext_id': 'B111',
                    'alive': True,
                    'id': 1,
                    'name': 'BELVITA CHOCOLAT 50GR X30',
                    'barcode': '444',
                    'barcode1': '',
                    'barcode2': '',
                    'barcode3': '',
                    'barcode4': '',
                    'packing_id': 'CODE123'
                }
            ],
            'message': ''
        }

        # call tested method
        result = focv.validate_product()
        assert_messages_1 = [
            {'message': Const.FIELD_UNIQUENESS_IN_FILE.value['en'].format('barcode', '789'), 'record': '789'},
            {'message': Const.FIELD_UNIQUENESS_IN_FILE.value['en'].format('barcode1', '789'), 'record': '789'},
            {'message': Const.FIELD_UNIQUENESS_IN_FILE.value['en'].format('barcode2', '789'), 'record': '789'},
            {'message': Const.FIELD_UNIQUENESS_IN_FILE.value['en'].format('barcode3', '789'), 'record': '789'},
            {'message': Const.FIELD_UNIQUENESS_IN_FILE.value['en'].format('barcode4', '789'), 'record': '789'}
        ]

        assert_message_2 = {'message': Const.FIELD_NOT_UNIQUE_PRODUCT_EXTERNAL_ID.value['en'].format('packing_id', 'CODE123', 'B111'), 'record': 'CODE123'}

        # assert results
        self.assertTrue(mock_write_errors.called)
        self.assertEqual(assert_messages_1, focv.errors)
        self.assertEqual(
            {'message': Const.FIELD_NOT_UNIQUE_PRODUCT_EXTERNAL_ID.value['en'].format('packing_id', 'CODE123', 'B111'), 'record': 'CODE123'},
            assert_message_2
        )
        self.assertFalse(result)

    @patch.object(ProductQueryOnCloud, 'get_products_for_company')
    @patch.object(FileOnCloudValidator, 'write_errors')
    def test_validate_product__barcode_uniqueness_update_success(
        self,
        mock_write_errors,
        mock_products_for_company
    ):
        """Case: update product, barcode uniqueness is fine. """
        # data from .csv file
        data = [
            {
                'product_id': 'B999',
                'product_name': 'Milka 100g x10',
                'default_barcode': '789',
                'product_action': '1',
                'price': '1.3'
            },
        ]

        # prepare class
        body = {
            'elastic_hash': self.elastic_hash,
            'company_id': COMPANY_ID,
            'data': data,
            'type': 'products',
            'input_file': '',
            'token': self.token,
            'email': '',
            'language': ''
        }
        focv = FileOnCloudValidator(body)
        mock_products_for_company.return_value = {
            'status': True,
            'results': [
                {
                    'ext_id': 'B999',
                    'alive': True,
                    'id': 1,
                    'name': 'BELVITA CHOCOLAT 50GR X30',
                    'barcode': '789',
                    'barcode1': '',
                    'barcode2': '',
                    'barcode3': '',
                    'barcode4': '',
                    'packing_id': ''
                },
            ],
            'message': ''
        }

        # call tested method
        result = focv.validate_product()

        # assert results
        self.assertFalse(mock_write_errors.called)
        self.assertTrue(result)

    @patch.object(ProductRotationGroupQueryOnCloud, 'get_products_for_specific_product_rotation_group')
    @patch.object(ProductRotationGroupQueryOnCloud, 'get_product_rotation_groups_for_company')
    @patch.object(ProductQueryOnCloud, 'get_products_for_company')
    @patch.object(FileOnCloudValidator, 'write_warnings')
    def test_validate_product_assigned_on_product_rotation_group(
            self,
            mock_write_warnings,
            mock_products_for_company,
            moc_company_product_rotation_group,
            mock_assigned_product_on_rotation_group
    ):
        """
        Case: delete product assigned on product rotation group. Discard import row, and raise warning message,
        because product assigned on product rotation group can't be deleted (business logic)...
        """

        # data from .csv file
        data = [
            {
                'product_id': 'B999',
                'product_name': 'Milka 100g x10',
                'default_barcode': '789',
                'product_action': '2',
                'price': '1.3'
            },
            {
                'product_id': 'A111',
                'product_name': 'Smoki 100g x10',
                'default_barcode': '123',
                'product_action': '1',
                'price': '1.3'
            }
        ]

        # prepare class
        body = {
            'elastic_hash': self.elastic_hash,
            'company_id': COMPANY_ID,
            'data': data,
            'type': 'products',
            'input_file': '',
            'token': self.token,
            'email': '',
            'language': ''
        }

        focv = FileOnCloudValidator(body)

        mock_products_for_company.return_value = {
            'status': True,
            'results': [
                {
                    'ext_id': 'B999',
                    'alive': True,
                    'id': 1,
                    'name': 'Milka 100g x10',
                    'barcode': '789',
                    'barcode1': '',
                    'barcode2': '',
                    'barcode3': '',
                    'barcode4': '',
                    'packing_id': ''
                },
                {
                    'ext_id': 'A111',
                    'alive': True,
                    'id': 1,
                    'name': 'Smoki 100GR X10',
                    'barcode': '345',
                    'barcode1': '',
                    'barcode2': '',
                    'barcode3': '',
                    'barcode4': '',
                    'packing_id': ''
                },
                {
                    'ext_id': 'C333',
                    'alive': True,
                    'id': 1,
                    'name': 'Bobi 100GR X10',
                    'barcode': '1234',
                    'barcode1': '',
                    'barcode2': '',
                    'barcode3': '',
                    'barcode4': '',
                    'packing_id': ''
                }
            ],
            'message': ''
        }

        moc_company_product_rotation_group.return_value = {
            'status': True,
            'results': [{
                'id': 1,
                'ext_id': 'asd45asd74',
                'name': 'product_rotation_group_1',
                'alive': True,
                'enabled': True,
            }]
        }

        mock_assigned_product_on_rotation_group.return_value = {
            'status': True,
            'results': [{
                'prg_name': 'product_rotation_group_1',
                'prg_ext_id': 'asd45asd74',
                'product_name': 'Milka 100g x10',
                'product_ext_id': 'B999',
                'product_code': '',
            }]
        }

        result = focv.validate_product()
        except_messages_warning_message = {'message': Const.PRODUCT_ROTATION_GROUP_STOP_DELETE.value['en'].format(
            'Milka 100g x10', 'B999', 'product_rotation_group_1', 'asd45asd74', 2),
            'record': 'B999'
        }
        # assert results
        self.assertTrue(mock_write_warnings.called)
        self.assertEqual(except_messages_warning_message['message'], focv.warning[0]['message'])
        self.assertTrue(result)

    @patch.object(ProductQueryOnCloud, 'get_products_for_company')
    @patch.object(FileOnCloudValidator, 'write_errors')
    def test_validate_product__barcode_uniqueness_update_fail(
        self,
        mock_write_errors,
        mock_products_for_company
    ):
        """
        Case: product to update. Fails because another product in db
        has the same barcode.
        """
        # data from .csv file
        data = [
            {
                'product_id': 'B999',
                'product_name': 'Milka 100g x10',
                'default_barcode': '789',
                'product_action': '1',
                'price': '1.3'
            },
            {
                'product_id': 'A111',
                'product_name': 'Smoki 100g x10',
                'default_barcode': '123',
                'product_action': '1',
                'price': '1.3'
            }
        ]

        # prepare class
        body = {
            'elastic_hash': self.elastic_hash,
            'company_id': COMPANY_ID,
            'data': data,
            'type': 'products',
            'input_file': '',
            'token': self.token,
            'email': '',
            'language': ''
        }
        focv = FileOnCloudValidator(body)
        mock_products_for_company.return_value = {
            'status': True,
            'results': [
                {
                    'ext_id': 'B999',
                    'alive': True,
                    'id': 1,
                    'name': 'BELVITA CHOCOLAT 50GR X30',
                    'barcode': '789',
                    'barcode1': '',
                    'barcode2': '',
                    'barcode3': '',
                    'barcode4': '',
                    'packing_id': ''
                },
                {
                    'ext_id': 'A111',
                    'alive': True,
                    'id': 1,
                    'name': 'Smoki 100GR X10',
                    'barcode': '345',
                    'barcode1': '',
                    'barcode2': '',
                    'barcode3': '',
                    'barcode4': '',
                    'packing_id': ''
                },
                {
                    'ext_id': 'C333',
                    'alive': True,
                    'id': 1,
                    'name': 'Bobi 100GR X10',
                    'barcode': '123',
                    'barcode1': '',
                    'barcode2': '',
                    'barcode3': '',
                    'barcode4': '',
                    'packing_id': ''
                }
            ],
            'message': ''
        }

        # call tested method
        result = focv.validate_product()

        # assert results
        self.assertTrue(mock_write_errors.called)
        self.assertEqual(
            Const.FIELD_NOT_UNIQUE_PRODUCT_EXTERNAL_ID.value['en'].format('barcode', '123', 'C333'),
            focv.errors[0]['message']
        )
        self.assertFalse(result)

    @patch.object(ProductQueryOnCloud, 'products_machine_status')
    @patch.object(FileOnCloudValidator, 'write_errors')
    def test_validate_product__product_in_machine(
        self,
        mock_write_errors,
        mock_products_machine_status
    ):
        """
        Case: product to delete. Fails because it is attached to machine and
        planogram.
        """
        p_ext_id = 'A123'
        # data from .csv file
        data = [
            {
                'product_id': p_ext_id,
                'product_name': 'Milka 100g x10',
                'default_barcode': '789',
                'product_action': '2',
                'price': '1.3'
            },
        ]

        # prepare class
        body = {
            'elastic_hash': self.elastic_hash,
            'company_id': COMPANY_ID,
            'data': data,
            'type': 'products',
            'input_file': '',
            'token': self.token,
            'email': '',
            'language': ''
        }
        focv = FileOnCloudValidator(body)
        products_with_machines = {
            p_ext_id: ['DBB401035324', 'DBG321345654'],
            'B416': ['DBB401035324'],
            'C112': ['DBB401035324'],
            'C711': ['DBB401035324'],
            'Q277': ['DBB301212728', 'DBB301216964'],
            'B897': ['DBB401035324'],
            'C618': ['DBB401035324'],
            'B344': ['DBB401035324'],
            'A079': ['DBB401035324'],
            'Q651': ['DBB301212728', 'DBB301216964'],
            'C605': ['DBB401035324'],
            'C591': ['DBB401035324'],
            'Q246': ['DBB301212728', 'DBB301216964'],
            'Q354': ['DBB301212728', 'DBB301216964'],
            'Q245': ['DBB301212728', 'DBB301216964'],
            'A080': ['DBB401035324'],
            'C598': ['DBB401035324'],
            'Q852': ['DBB301212728', 'DBB301216964'],
            'C520': ['DBB401035324'],
            'B789': ['DBB401035324']
        }

        mock_products_machine_status.return_value = {
            'allowed': False, 'results': products_with_machines
        }

        # call tested method
        result = focv.validate_product()

        # assert results
        self.assertTrue(mock_write_errors.called)
        self.assertIn(
            {
                'message': Const.PRODUCT_DELETE_ERROR_MACHINES.value['en'].\
                    format(
                        p_ext_id,
                        len(products_with_machines[p_ext_id]),
                        ', '.join(products_with_machines[p_ext_id])
                    ),
                'record': p_ext_id,
            },
            focv.errors
        )
        self.assertFalse(result)

    @patch.object(ProductQueryOnCloud, 'products_planogram_status')
    @patch.object(FileOnCloudValidator, 'write_errors')
    def test_validate_product__product_in_planogram(
        self,
        mock_write_errors,
        mock_products_planogram_status
    ):
        """
        Case: product to delete. Fails because it is attached to planogram.
        """
        p_ext_id = 'A123'
        # data from .csv file
        data = [
            {
                'product_id': p_ext_id,
                'product_name': 'Milka 100g x10',
                'default_barcode': '789',
                'product_action': '2',
                'price': '1.3'
            },
            {
                'product_id': 'C444',
                'product_name': 'Bobi 40g x10',
                'default_barcode': '128',
                'product_action': '2',
                'price': '1.9'
            },
        ]

        # prepare class
        body = {
            'elastic_hash': self.elastic_hash,
            'company_id': COMPANY_ID,
            'data': data,
            'type': 'products',
            'input_file': '',
            'token': self.token,
            'email': '',
            'language': ''
        }
        focv = FileOnCloudValidator(body)
        products_with_planograms = {
            p_ext_id: ['DIV-0P-NX-SNACK', 'DIV-0P-NX-SNACK'],
            'A110': ['DIV-0P-NX-SNACK'],
            'C422': ['DIV-0P-NX-SNACK'],
            'B425': ['DIV-0P-NX-SNACK']
        }

        mock_products_planogram_status.return_value = {
            'allowed': False, 'results': products_with_planograms
        }

        # call tested method
        result = focv.validate_product()

        # assert results
        self.assertTrue(mock_write_errors.called)
        self.assertIn(
            {
                'message': Const.PRODUCT_DELETE_ERROR_PLANOGRAMS.value['en'].\
                    format(
                        p_ext_id,
                        ', '.join(products_with_planograms[p_ext_id])
                    ),
                'record': p_ext_id,
            },
            focv.errors
        )
        self.assertFalse(result)

    @patch.object(ProductQueryOnCloud, 'get_products_for_company')
    @patch.object(PlanogramQueryOnCloud, 'get_combo_recipe')
    def test_validate_planogram__different_product_cases(
        self, mock_get_combo_recipe, mock_get_products_for_company
    ):
        """Different cases for the product on Cloud."""
        product_ext_id = 'P001'
        data = [
            {
                'planogram_name': 'Planogram 1',
                'planogram_id': '5488777',
                'prg_id': '',
                'planogram_action': '0',
                'cashless_price': '0',
                'product_warning_percentage': '5',
                'component_warning_percentage': '9',
                'mail_notification': '1',
                'column_number': '1',
                'recipe_id': '369',
                'tags': 'test1',
                'capacity': '1',
                'warning': '1',
                'fill_rate': '1',
                'price_1': '5',
                'product_id': product_ext_id,
                'product_rotation_group_id': ''
            }
        ]
        self.body['data'] = data
        self.body['type'] = 'planograms'

        # mock cloud query for products
        mock_get_products_for_company.return_value = {
            'status': True,
            'results': [],
            'message': ''
        }

        # mock cloud query for combo products
        mock_get_combo_recipe.return_value = {
            'status': True,
            'results': [{'id': 5, 'name': 'Planogram 1', 'code': '369'}],
            'message': ''
        }

        cases = [
            [
                {
                    'ext_id': product_ext_id,
                    'alive': False,
                    'id': PRODUCT_ID,
                    'name': 'BELVITA CHOCOLAT 50GR X30',
                    'barcode': '789',
                    'packing_id': '',
                    'is_composite': False,
                    'is_combo': False
                },
                False,
                'case product not alive'
            ],
            [
                {
                    'ext_id': product_ext_id,
                    'alive': True,
                    'id': PRODUCT_ID,
                    'name': 'BELVITA CHOCOLAT 50GR X30',
                    'barcode': '789',
                    'packing_id': '',
                    'is_composite': False,
                    'is_combo': True
                },
                True,
                'case product is combo'
            ]
        ]

        for case in cases:
            focv = FileOnCloudValidator(self.body)
            del mock_get_products_for_company.return_value['results'][:]
            mock_get_products_for_company.return_value['results'].append(case[0])

            result = focv.validate_planogram()
            self.assertEqual(result, case[1], case[2])

    @patch.object(PlanogramQueryOnCloud, 'get_planogram_for_company')
    @patch.object(ProductQueryOnCloud, 'get_products_for_company')
    @patch.object(PlanogramQueryOnCloud, 'get_planogram_columns')
    @patch.object(PlanogramQueryOnCloud, 'get_layout_column_tags')
    @patch.object(PlanogramQueryOnCloud, 'get_layout_component')
    @patch.object(PlanogramQueryOnCloud, 'get_product_component')
    def test_validate_planogram__action_cases_pass(self,
                                                   mock_get_product_component,
                                                   mock_get_layout_component,
                                                   mock_get_layout_column_tags,
                                                   mock_get_planogram_columns,
                                                   mock_get_products_for_company,
                                                   mock_get_planogram_for_company):

        """Create, update and delete actions cases successful."""
        product_ext_id = 'P001'
        planogram_name = 'Planogram 1'
        planogram_id = 1
        planogram_ext_id = 'PL001'
        decimal_places_as_decimal = Decimal('0.01')
        data = [
            {
                'planogram_name': planogram_name,
                'planogram_id': planogram_ext_id,
                'planogram_action': '0',
                'cashless_price': '0',
                'product_warning_percentage': '5',
                'component_warning_percentage': '9',
                'mail_notification': '1',
                'column_number': '1',
                'recipe_id': '',
                'tags': 'tags2',
                'capacity': '1',
                'warning': '1',
                'fill_rate': '1',
                'price_1': '5',
                'product_id': product_ext_id,
                'product_rotation_group_id': ''
            }
        ]
        self.body['data'] = data
        self.body['type'] = 'planograms'

        # dynamically generate data for mock layout_column database
        price = Decimal('5'.replace(',', '.')).quantize(decimal_places_as_decimal)
        layout_column_structure = [{
            'caption': planogram_name,
            'alive': True,
            'column_id': 1,
            'planogram_id': 1,
            'external_id': planogram_ext_id,
            'index': 1,
            'price': price,
            'price_2': price,
            'price_3': price,
            'price_4': price,
            'price_5': price,
            'warning_quantity': 7,
            'max_quantity': 50,
            'notify_warning': False,
            'mail_notification': False,
            'multiple_pricelists': 3,
            'next_fill_quantity': 4,
            'component_warning_percentage': 0,
            'component_max_quantity': 10,
            'product_warning_percentage': 0
        }]

        mock_get_planogram_columns.return_value = layout_column_structure
        mock_get_layout_column_tags.return_value = [
            {
                'id': 1,
                'alive': True,
                'caption': 'tags1',
                'column_id': 1,
                'columns_tags_id': 1,
            }
        ]
        mock_get_layout_component.return_value = []
        mock_get_product_component.return_value = []
        # mock cloud query for products
        mock_get_products_for_company.return_value = {
            'status': True,
            'results': [
                {
                    'ext_id': product_ext_id,
                    'alive': True,
                    'id': PRODUCT_ID,
                    'name': 'BELVITA CHOCOLAT 50GR X30',
                    'barcode': '789',
                    'packing_id': '',
                    'is_composite': False,
                    'is_combo': False
                }
            ],
            'message': ''
        }

        mock_get_planogram_for_company.return_value = {
            'status': True,
            'results': [
                {
                    'id': planogram_id,
                    'ext_id': planogram_ext_id,
                    'name': planogram_name,
                    'alive': True,
                    'enabled': True,
                    'product_warning_percentage': 0,
                    'component_warning_percentage': 0,
                    'mail_notification': True,
                    'show_price_2': False
                }
            ],
            'message': ''
        }

        # run loop for testing create, update and delete actions
        action_cases = [
            ['0', True, 'case Create'],
            ['1', True, 'case Update'],
            ['2', True, 'case Delete']
        ]
        for case in action_cases:
            data[0]['planogram_action'] = case[0]
            focv = FileOnCloudValidator(self.body)
            result = focv.validate_planogram()
            self.assertEqual(result, case[1], case[2])

    @patch.object(PlanogramQueryOnCloud, 'get_planogram_columns')
    @patch.object(ProductQueryOnCloud, 'get_products_for_company')
    @patch.object(PlanogramQueryOnCloud, 'get_planogram_for_company')
    def test_validate_planogram__action_cases_fail_on_db_validation(
        self,
        mock_get_planogram_for_company,
        mock_get_products_for_company,
        mock_get_planogram_columns,
    ):
        """
        Create, update and delete actions cases fail because of the data found in database.
        """
        product_ext_id = 'P001'
        planogram_name = 'Planogram_1'
        planogram_id = 1
        planogram_ext_id = 'PL001'

        data = [
            {
                'multiple_pricelists': 1,
                'planogram_name': planogram_name,
                'planogram_id': planogram_ext_id,
                'planogram_action': 0,
                'product_warning_percentage': '5',
                'component_warning_percentage': '9',
                'mail_notification': '1',
                'column_number': '123', # called 'index' in column query
                'recipe_id': '',
                'tags': 'test1',
                'capacity': '1',
                'warning': '1',
                'fill_rate': '1',
                'price_1':  str(random.randint(1, 10)),
                'product_id': product_ext_id,
                'product_rotation_group_id': ''
            }
        ]
        self.body['data'] = data
        self.body['type'] = 'planograms'

        # mock cloud query for products
        mock_get_products_for_company.return_value = {
            'status': True,
            'results': [
                {
                    'ext_id': product_ext_id,
                    'alive': True,
                    'id': 1,
                    'name': 'BELVITA CHOCOLAT 50GR X30',
                    'barcode': '789',
                    'packing_id': '',
                    'is_composite': False,
                    'is_combo': False
                }
            ],
            'message': ''
        }

        mock_get_planogram_columns.return_value = []
        mock_get_planogram_for_company.return_value = {
            'status': True,
            'results': [],
            'message': ''
        }

        def mock_planogram_for_company(status):
            if status == 'create':
                data_e = [
                    {
                        'id': planogram_id,
                        'ext_id': planogram_ext_id,
                        'name': planogram_name,
                        'alive': True,
                        'enabled': True,
                        'product_warning_percentage': 0,
                        'component_warning_percentage': 0,
                        'mail_notification': True,
                        'show_price_2': False
                    }]
            else:
                data_e = []
            return data_e

        # each contains: action, planogram query result, mock planogram query,  expected result and name of the case
        decimal_places_as_decimal = Decimal('0.01')
        price = Decimal('5'.replace(',', '.')).quantize(decimal_places_as_decimal)
        action_cases = [
            ['0', [
                {
                    'caption': planogram_name,
                    'external_id': planogram_ext_id,
                    'planogram_id': planogram_id,
                    'product_warning_percentage':0,
                    'component_warning_percentage': 0,
                    'mail_notification': True,
                    'multiple_pricelists': 1,
                    'product_id': 1,
                    'alive': True,
                    'column_id': 1,
                    'company_id': 1,
                    'notify_warning': False,
                    'warning_quantity': 0,
                    'next_fill_quantity': 0,
                    'component_max_quantity': 0,
                    'max_quantity': 0,
                    'index': 123,
                    'recipe_id': '',
                    'price': price,
                    'price_2': price,
                    'price_3': price,
                    'price_4': price,
                    'price_5': price,
                    'combo_recipe_id': '',
                    'product_rotation_group_id': '',
                    'columns_tags_id': '',
                    'tags_action': 0,
                    'tags_caption': 'tags',
                    'tags_id': None,

                }
            ], mock_planogram_for_company('create'), False, 'case Create'],
            ['1', [], mock_planogram_for_company('update'), False, 'case Update'],
            ['2', [], mock_planogram_for_company('delete'), False, 'case Delete']
        ]
        # run loop for testing create, update and delete actions
        for case in action_cases:
            data[0]['planogram_action'] = case[0]
            mock_get_planogram_columns.return_value = case[1]
            mock_get_planogram_columns.return_value = case[1]
            mock_get_planogram_for_company.return_value['results'] = case[2]
            focv = FileOnCloudValidator(self.body)
            result = focv.validate_planogram()
            self.assertEqual(result, case[3], case[4])

    @patch.object(PlanogramQueryOnCloud, 'get_recipe')
    @patch.object(ProductQueryOnCloud, 'get_products_for_company')
    def test_validate_planogram__composite_product_cases(
        self,
        mock_get_products_for_company,
        mock_get_recipe
    ):
        """Cases for composite product."""
        product_id = 'P001'
        planogram_name = 'Planogram 1'
        data = [
            {
                'planogram_name': planogram_name,
                'planogram_id': '5488777',
                'prg_id': '',
                'planogram_action': '0',
                'cashless_price': '0',
                'product_warning_percentage': '5',
                'component_warning_percentage': '9',
                'mail_notification': '1',
                'column_number': '1',
                'recipe_id': '333',
                'tags': 'test1',
                'capacity': '1',
                'warning': '1',
                'fill_rate': '1',
                'price_1': '5',
                'product_id': product_id,
                'product_rotation_group_id': ''
            }
        ]
        self.body['data'] = data
        self.body['type'] = 'planograms'

        # mock cloud query for products
        mock_get_products_for_company.return_value = {
            'status': True,
            'results': [
                {
                    'ext_id': product_id,
                    'alive': True,
                    'id': PRODUCT_ID,
                    'name': 'BELVITA CHOCOLAT 50GR X30',
                    'barcode': '789',
                    'packing_id': '',
                    'is_composite': True,
                    'is_combo': False
                }
            ],
            'message': ''
        }

        # each item is a dict for recipe with expected result and case name
        recipe_cases = [
            [
                {
                    'status': True,
                    'results': [
                        {
                            'id': '1',
                            'name': 'Cake recipe',
                            'default': False,
                            'code': '333'
                        }
                    ],
                    'message': ''
                }, True, 'case everything OK'
            ],
            [
                {
                    'status': True,
                    'results': [
                        {
                            'id': '2',
                            'name': 'Cake recipe',
                            'default': False,
                            'code': '555'
                        }
                    ],
                    'message': ''
                }, False, 'case not exact recipe found on cloud'
            ],
            [
                {
                    'status': False,
                    'results': [],
                    'message': ''
                }, False, 'case query status is False'
            ],
            [
                {
                    'status': True,
                    'results': [],
                    'message': ''
                }, False, 'case results are empty'
            ],
        ]

        for recipe_case in recipe_cases:
            mock_get_recipe.return_value = recipe_case[0]
            focv = FileOnCloudValidator(self.body)
            result = focv.validate_planogram()
            self.assertEqual(result, recipe_case[1], recipe_case[2])

    @patch.object(PlanogramQueryOnCloud, 'get_planogram_for_company')
    @patch.object(ProductQueryOnCloud, 'get_products_for_company')
    def test_validate_planogram__query_cloud_error(
        self,
        mock_get_products_for_company,
        mock_get_planogram_for_company
    ):
        """
        Errors due to unsuccessfull querying on the Cloud database.
        """
        product_ext_id = 'P001'
        planogram_name = 'Planogram 1'
        planogram_id = 1
        planogram_ext_id = 'PL001'
        data = [
            {
                'planogram_name': planogram_name,
                'planogram_id': planogram_ext_id,
                'planogram_action': '0',
                'cashless_price': '0',
                'product_warning_percentage': '5',
                'component_warning_percentage': '9',
                'mail_notification': '1',
                'column_number': '123', # called 'index' in column query
                'recipe_id': '',
                'tags': 'test1',
                'capacity': '1',
                'warning': '1',
                'fill_rate': '1',
                'price_1': '5',
                'price_2': '',
                'product_rotation_group_id': '',
                'product_id': product_ext_id
            }
        ]
        self.body['data'] = data
        self.body['type'] = 'planograms'

        mock_get_planogram_for_company.return_value = {
            'status': True,
            'results': [],
            'message': ''
        }

        # mock cloud query for products
        mock_get_products_for_company.return_value = {
            'status': True,
            'results': [],
            'message': ''
        }

        cases = [
            [False, True, 'case planogram db query fails'],
            [True, False, 'case products db query fails']
        ]
        for case in cases:
            mock_get_planogram_for_company.return_value['status'] = case[0]
            mock_get_products_for_company.return_value['status'] = case[1]
            focv = FileOnCloudValidator(self.body)
            result = focv.validate_planogram()
            self.assertFalse(result)

    def test_validate_planogram__fail_due_to_import_data_errors(self):
        """Various cases where input data is not appropriate."""
        product_ext_id = 'P001'
        planogram_name = 'Planogram 1'
        planogram_ext_id = 'PL001'

        # different cases for input data
        cases = [
            [
                {
                    'planogram_name': planogram_name,
                    'planogram_id': planogram_ext_id,
                    'planogram_action': '0',
                    'cashless_price': '0',
                    'product_warning_percentage': '5',
                    'component_warning_percentage': '9',
                    'mail_notification': '1',
                    'column_number': '123', # called 'index' in column query
                    'recipe_id': '',
                    'tags': 'test1',
                    'capacity': '1',
                    'warning': '1',
                    'fill_rate': '4',
                    'price_1': '5',
                    'product_id': product_ext_id,
                    'product_rotation_group_id': ''
                },
                'case fill rate wrong'
            ],
            [
                {
                    'planogram_name': planogram_name,
                    'planogram_id': planogram_ext_id,
                    'planogram_action': '0',  # when you send string in action field, this import file never be sent to cloud validator (json schema raise validation error)
                    'cashless_price': '0',
                    'product_warning_percentage': '5',
                    'component_warning_percentage': '9',
                    'mail_notification': '1',
                    'column_number': '123', # called 'index' in column query
                    'recipe_id': '',
                    'tags': 'test1',
                    'capacity': '1',
                    'warning': '1',
                    'fill_rate': '1',
                    'price_1': '5',
                    'product_id': product_ext_id,
                    'product_rotation_group_id': ''
                },
                'case wrong action field'
            ],
            [
                {
                    'planogram_name': planogram_name,
                    'planogram_id': planogram_ext_id,
                    'planogram_action': '0',
                    'cashless_price': '1',
                    'product_warning_percentage': '5',
                    'component_warning_percentage': '9',
                    'mail_notification': '1',
                    'column_number': '123',
                    'recipe_id': '',
                    'tags': 'test1',
                    'capacity': '1',
                    'warning': '1',
                    'fill_rate': '1',
                    'price_1': '5',
                    'product_id': product_ext_id,
                    'product_rotation_group_id': ''
                },
                'case price check'
            ]
        ]

        self.body['data'] = []
        self.body['type'] = 'planograms'

        for case in cases:
            del self.body['data'][:]
            self.body['data'].append(case[0])
            focv = FileOnCloudValidator(self.body)
            result = focv.validate_planogram()
            self.assertFalse(result, case[1])

    def test_set_upsert_if_needed(self):
        # UPDATE and ENTITY DOES NOT EXISTS -> CREATE
        action = ImportAction.UPDATE
        entity_id = "MACH_01"
        alive_ids = ["mach", ]
        new_action, new_value = FileOnCloudValidator.get_upsert_if_needed(action, entity_id, alive_ids)
        self.assertTrue(new_action == ImportAction.CREATE)
        self.assertTrue(new_value == ImportAction.CREATE.value)

        # ACTION CREATE ENTITY EXISTS -> UPDATE
        action = ImportAction.CREATE
        entity_id = "MACH_01"
        alive_ids = ["MACH_01", ]
        new_action, new_value = FileOnCloudValidator.get_upsert_if_needed(action, entity_id, alive_ids)
        self.assertTrue(new_action == ImportAction.UPDATE)
        self.assertTrue(new_value == ImportAction.UPDATE.value)

        # ACTION DELETE
        action = ImportAction.DELETE
        entity_id = "MACH_01"
        alive_ids = ["MACH_01", ]
        new_action, new_value = FileOnCloudValidator.get_upsert_if_needed(action, entity_id,
            alive_ids)
        self.assertTrue(new_action == ImportAction.DELETE)
        self.assertTrue(new_value == ImportAction.DELETE.value)

        # ACTION DELETE ENTITY DOES NOT EXIST
        action = ImportAction.DELETE
        entity_id = "MACH_01"
        alive_ids = ["mach", ]
        new_action, new_value = FileOnCloudValidator.get_upsert_if_needed(action, entity_id, alive_ids)
        self.assertTrue(new_action == ImportAction.DELETE)
        self.assertTrue(new_value == ImportAction.DELETE.value)

        # UPDATE ENTITY ALREADY EXISTS
        action = ImportAction.UPDATE
        entity_id = "MACH_01"
        alive_ids = ["MACH_01", ]
        new_action, new_value = FileOnCloudValidator.get_upsert_if_needed(action, entity_id, alive_ids)
        self.assertTrue(new_action == ImportAction.UPDATE)
        self.assertTrue(new_value == ImportAction.UPDATE.value)

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

    def test_composite_and_combo_product_check(self):
        """
        This method check if specific product is composite and combo!
        :return:
        """
        product_data = [{
            'use_packing': False,
            'barcode': '',
            'id': '234',
            'ext_id': '369P',
            'is_composite': False,
            'name': 'Berger Spitzbub 74g',
            'alive': True,
            'is_combo': False,
            'is_combo_result':  False,
            'is_composite_result': False,
            'message': 'Case 1: non combo & composite product'
        }, {
            'use_packing': True,
            'barcode': '',
            'id': '69',
            'ext_id': '56738',
            'is_composite': True,
            'name': 'Caffe',
            'alive': True,
            'is_combo': False,
            'is_combo_result':  False,
            'is_composite_result': True,
            'message': 'Case 2: composite product'
        }, {
            'use_packing': False,
            'barcode': '',
            'id': '8563',
            'ext_id': '56256',
            'is_composite': False,
            'name': 'Balisto Yoberry-Mix 37 g',
            'alive': True,
            'is_combo': True,
            'is_combo_result':  True,
            'is_composite_result': False,
            'message': 'Case 3: combo product'
        }]

        all_company_planograms = []
        planogram_name_and_external_id = []

        all_alive_company_products = [p for p in product_data if p['alive'] is True]
        all_cloud_company_product_ext_id = [x['ext_id'] for x in all_alive_company_products]
        products_data_frame_object = pd.DataFrame(all_alive_company_products,
                                                  columns=['ext_id', 'is_composite', 'is_combo'])
        planogram_handler = PlanogramHandler(
            all_company_planograms=all_company_planograms,
            all_company_product=product_data,
            planogram_name_and_external_id=planogram_name_and_external_id,
            all_cloud_company_product_ext_id=all_cloud_company_product_ext_id,
            language='en',
            products_data_frame_object=products_data_frame_object)

        for x in product_data:
            prod_ext_id = x['ext_id']
            expected_errors_composite_product = x['is_composite']
            expected_errors_combo_product = x['is_combo_result']
            message = x['message']

            composite_product, combo_product = planogram_handler.composite_and_combo_product_check(prod_ext_id)
            self.assertEqual(composite_product, expected_errors_composite_product, message)
            self.assertEqual(combo_product, expected_errors_combo_product, message)

    @patch.object(PlanogramQueryOnCloud, 'get_planogram_columns')
    @patch.object(PlanogramQueryOnCloud, 'get_layout_component')
    @patch.object(PlanogramQueryOnCloud, 'get_product_component')
    @patch.object(PlanogramQueryOnCloud, 'get_layout_column_tags')
    def test_planogram_processor_create_action(self,
                                               mock_get_layout_column_tags,
                                               mock_get_product_component,
                                               mock_get_layout_component,
                                               mock_get_planogram_columns):
        """
        Test for planogram insert action.
        There are many different cases for planogram, but we will go through the main cases and planogram scenarios.
        Planogram_processor is core function for building planogram import entity, everything for planogram entity is
        based on this method. The scenario: create 3 planogram with n column with regular non composite product
        and tags on that columns.
        :return:
        """
        import_data = []

        mock_get_planogram_columns.return_value = []
        mock_get_layout_component.return_value = []
        mock_get_product_component.return_value = []
        mock_get_layout_column_tags.return_value = []

        # generate planogram import data
        product_external_id = 'asda244as'
        planogram_names = ['Planogram_a', 'Planogram_b', 'Planogram_c']
        planogram_action = 0
        column_number = 2
        for planogram_name in planogram_names:
            for i in range(1, column_number + 1):
                import_row = {
                    'product_rotation_group_id': '',
                    'minimum_route_pickup': 0,
                    'recipe_id': '',
                    'prg_id': '',
                    'is_composite': False,
                    'is_combo': False,
                    'product_id': product_external_id,
                    'company_product_id': PRODUCT_ID,
                    'planogram_id': planogram_name,
                    'planogram_name': planogram_name,
                    'planogram_action': planogram_action,
                    'multiple_pricelists': 3,
                    'fill_rate': i,
                    'capacity': i * 2 + 2,
                    'tags': 'tags_' + str(i),
                    'warning': i * 2,
                    'product_warning_percentage': random.randint(1, 98),
                    'component_warning_percentage': random.randint(1, 80),
                    'column_number': i,
                    'price_1': str(random.randint(1, 10)),
                    'price_2': str(random.randint(1, 20)),
                    'price_3': str(random.randint(1, 30)),
                    'mail_notification': '',
                }
                import_data.append(import_row)

        planogram_for_import, column_for_import, component_for_import, tags_create_list = planogram_processor(
            data=import_data,
            planograms_columns=[],
            product_components=[],
            layout_columns_tags=[],
            layout_components=[]
        )

        planogram_for_import_match_data = [
            {
                'caption': x['caption'],
                'planogram_id': x['planogram_id'],
                'external_id': x['external_id'],
                'import_action': x['import_action']
            } for x in planogram_for_import
        ]
        # build except result for product_templates
        except_planogram_for_import = [
            {'caption': 'Planogram_a', 'planogram_id': None, 'external_id': 'Planogram_a', 'import_action': 0},
            {'caption': 'Planogram_b', 'planogram_id': None, 'external_id': 'Planogram_b', 'import_action': 0},
            {'caption': 'Planogram_c', 'planogram_id': None, 'external_id': 'Planogram_c', 'import_action': 0}
        ]

        # build except result for layout_column
        except_column_for_import = []
        for x in import_data:
            decimal_places_as_decimal = Decimal('0.01')

            data_structure = {
                'tags_id': None,
                'column_id': None,
                'planogram_id': None,
                'columns_tags_id': None,
                'mail_notification': False,
                'notify_warning': True,
                'is_composite': False,
                'is_combo': False,
                'alarm_quantity': '',
                'combo_recipe_id': '',
                'recipe_id': '',
                'product_rotation_group_id': '',
                'tags_action': 0,
                'column_action': 0,
                'import_action': 0,
                'minimum_route_pickup': 0,
                'component_warning_percentage': 0,
                'next_fill_quantity': x['fill_rate'],
                'price': Decimal(x['price_1'].replace(',', '.')).quantize(decimal_places_as_decimal),
                'price_2': Decimal(x['price_2'].replace(',', '.')).quantize(decimal_places_as_decimal),
                'price_3': Decimal(x['price_3'].replace(',', '.')).quantize(decimal_places_as_decimal),
                'price_4': x['price_1'],
                'price_5': x['price_1'],
                'tags_caption': x['tags'],
                'warning_quantity': x['warning'],
                'product_warning_percentage': x['product_warning_percentage'],
                'pricelist_count': x['multiple_pricelists'],
                'index': x['column_number'],
                'max_quantity': x['capacity'],
                'caption': x['planogram_name'],
                'product_id': x['company_product_id'],
                'external_id': x['planogram_name']
            }

            except_column_for_import.append(data_structure)

        sorted_except_column = []
        for x in except_column_for_import:
            sorted_except_column.append(sorted(x.keys()))

        sorted_column_for_import = []
        for x in column_for_import:
            sorted_column_for_import.append(sorted(x.keys()))

        # build except result for tags
        except_tags = []

        for x in column_for_import:
            tags_data = {
                'column_action': x['column_action'],
                'import_action': x['import_action'],
                'index': x['index'],
                'external_id': x['external_id'],
                'columns_tags_id': x['columns_tags_id'],
                'tags_action': x['tags_action'],
                'planogram_id': x['planogram_id'],
                'tags_caption': x['tags_caption'],
                'tags_id': x['tags_id'],
                'column_id': x['column_id']
            }
            except_tags.append(tags_data)

        # test import data for product_templates
        self.assertListEqual(
            except_planogram_for_import,
            planogram_for_import_match_data,
            'Fail planogram create action for product_templates'
        )

        # test import data for layout_columns
        self.assertListEqual(
            sorted_except_column,
            sorted_column_for_import,
            'Fail planogram create action for layout_columns'
        )

        # test import data for layout_components
        self.assertListEqual(
            [],
            component_for_import,
            'Fail planogram create action for layout_components'
        )

        # test import data for tags
        self.assertListEqual(
            except_tags,
            tags_create_list,
            'Fail planogram create action for tags'
        )

    @patch.object(PlanogramQueryOnCloud, 'get_planogram_columns')
    @patch.object(PlanogramQueryOnCloud, 'get_layout_component')
    @patch.object(PlanogramQueryOnCloud, 'get_product_component')
    @patch.object(PlanogramQueryOnCloud, 'get_layout_column_tags')
    def test_planogram_processor_update_action(self,
                                               mock_get_layout_column_tags,
                                               mock_get_product_component,
                                               mock_get_layout_component,
                                               mock_get_planogram_columns):

        decimal_places_as_decimal = Decimal('0.01')

        # dynamically generate planogram import data
        import_data = []
        planogram_count = 10
        column_count = 30
        recipe_id = 1
        planogram_names = ['Planogram'+str(x) for x in range(1, planogram_count+1)]

        for planogram_name in planogram_names:
            for i in range(1, column_count + 1):
                import_row = {
                    'product_rotation_group_id': '',
                    'prg_id': '',
                    'recipe_id': recipe_id,
                    'minimum_route_pickup': 0,
                    'is_composite': True,
                    'is_combo': False,
                    'product_id': 'asda244as',
                    'company_product_id': PRODUCT_ID,
                    'planogram_id': planogram_name,
                    'planogram_name': planogram_name,
                    'planogram_action': 0,
                    'multiple_pricelists': 3,
                    'fill_rate': 5,
                    'capacity': 5,
                    'tags': 'tags_' + str(i),
                    'warning': 5,
                    'product_warning_percentage': 80,
                    'component_warning_percentage': 90,
                    'column_number': i+10,
                    'price_1': '1',
                    'price_2': '2',
                    'price_3': '3',
                    'mail_notification': '',
                }
                import_data.append(import_row)

        # dynamically generate data for mock layout_column database
        generate_layout_columns_data = []
        i = 1
        created_planogram = []
        planogram_id = 1
        for x in import_data:
            i += 1
            if x['planogram_name'] not in created_planogram:
                created_planogram.append(x['planogram_name'])
                planogram_id += 1
                i = 1
            layout_column_structure = {
                'caption': x['planogram_name'],
                'alive': True,
                'column_id': i,
                'planogram_id': planogram_id,
                'minimum_route_pickup': 0,
                'external_id': x['planogram_name'],
                'index': x['column_number'],
                'price': Decimal(x['price_1'].replace(',', '.')).quantize(decimal_places_as_decimal),
                'price_2': Decimal(x['price_2'].replace(',', '.')).quantize(decimal_places_as_decimal),
                'price_3': Decimal(x['price_3'].replace(',', '.')).quantize(decimal_places_as_decimal),
                'price_4': Decimal(x['price_1'].replace(',', '.')).quantize(decimal_places_as_decimal),
                'price_5': Decimal(x['price_1'].replace(',', '.')).quantize(decimal_places_as_decimal),
                'warning_quantity': 7,
                'max_quantity': 50,
                'notify_warning': True,
                'mail_notification': True,
                'multiple_pricelists': 3,
                'next_fill_quantity': 4,
                'component_warning_percentage': 90,
                'component_max_quantity': 10,
                'product_warning_percentage': 0,
            }
            generate_layout_columns_data.append(layout_column_structure)

        # dynamically generate except result for layout_columns
        except_column_for_import = []
        created_planogram = []
        price1 = Decimal('1'.replace(',', '.')).quantize(decimal_places_as_decimal)
        price2 = Decimal('2'.replace(',', '.')).quantize(decimal_places_as_decimal)
        price3 = Decimal('3'.replace(',', '.')).quantize(decimal_places_as_decimal)
        planogram_id = 1
        i = 1
        for x in import_data:
            i += 1
            if x['planogram_name'] not in created_planogram:
                created_planogram.append(x['planogram_name'])
                planogram_id += 1
                i = 1
            data_structure = {
                'tags_id': '',
                'column_id': i,
                'planogram_id': planogram_id,
                'minimum_route_pickup': 0,
                'columns_tags_id': '',
                'mail_notification': False,
                'notify_warning': True,
                'is_composite': True,
                'is_combo': x['is_combo'],
                'tags_action': 1,
                'column_action': 1,
                'import_action': 0,
                'recipe_id': recipe_id,
                'alarm_quantity': '',
                'combo_recipe_id': '',
                'product_rotation_group_id': '',
                'component_warning_percentage': x['component_warning_percentage'],
                'next_fill_quantity': x['fill_rate'],
                'price': price1,
                'price_2': price2,
                'price_3': price3,
                'price_4': price1,
                'price_5': price1,
                'tags_caption': x['tags'],
                'warning_quantity': x['warning'],
                'product_warning_percentage': 0,
                'pricelist_count': x['multiple_pricelists'],
                'index': x['column_number'],
                'max_quantity': x['capacity'],
                'caption': x['planogram_name'],
                'product_id': PRODUCT_ID,
                'external_id': x['planogram_name']
            }

            except_column_for_import.append(data_structure)
        mock_get_planogram_columns.return_value = generate_layout_columns_data

        product_component_data = []
        for x in generate_layout_columns_data:
            product_component = {
                'id': x,
                'product_component_alive': True,
                'product_component_caption': 'Sugar',
                'product_component_id': x['column_id'],
                'product_component_quantity': 1,
                'product_component_recipe_id': 1
            }
            product_component_data.append(product_component)

        mock_get_product_component.return_value = product_component_data

        layout_components_data = []
        except_component_data = []
        i = 0
        for x in except_column_for_import:
            i += 1
            layout_components_structure = {
                'alive': True,
                'component_max_quantity': 500,
                'component_next_fill_quantity': 500,
                'component_notify_warning': 500,
                'component_tags': [],
                'component_warning_quantity': 0,
                'id': x['column_id'],
                'layout_id': x['planogram_id'],
                'product_component_id': x['column_id'],
            }
            component_data = {
                'caption': x['caption'],
                'component_action': 1,
                'recipe_id': 1,
                'component_id': x['column_id'],
                'component_max_quantity': 5,
                'component_next_fill_quantity': 5,
                'component_notify_warning': True,
                'component_tags': '{}',
                'component_warning_quantity': 5,
                'external_id': x['external_id'],
                'planogram_id': x['planogram_id'],
                'product_component_id': x['column_id'],
                'product_id': PRODUCT_ID
            }
            layout_components_data.append(layout_components_structure)
            except_component_data.append(component_data)

        mock_get_layout_component.return_value = layout_components_data

        # build except result for product_templates
        except_planogram_for_import = []
        pl_name_test = []
        for x in except_column_for_import:
            if x['caption'] not in pl_name_test:
                pl_name_test.append(x['caption'])
                planogram_data = {
                    'caption': x['caption'],
                    'planogram_id': x['planogram_id'],
                    'external_id': x['external_id'],
                    'import_action': 1
                }
                except_planogram_for_import.append(planogram_data)

        mock_get_layout_column_tags.return_value = []

        # build except result for tags
        except_tags = []

        for x in except_column_for_import:
            tags_data = {
                'column_action': x['column_action'],
                'import_action': x['import_action'],
                'index': x['index'],
                'external_id': x['external_id'],
                'columns_tags_id': x['columns_tags_id'],
                'tags_action': x['tags_action'],
                'planogram_id': x['planogram_id'],
                'tags_caption': x['tags_caption'],
                'tags_id': x['tags_id'],
                'column_id': x['column_id']
            }
            except_tags.append(tags_data)

        planogram_for_import, column_for_import, component_for_import, tags_create_list = planogram_processor(
            data=import_data,
            planograms_columns=generate_layout_columns_data,
            product_components=product_component_data,
            layout_columns_tags=[],
            layout_components=layout_components_data
        )
        planogram_for_import_match_data = [
            {
                'caption': x['caption'],
                'planogram_id': x['planogram_id'],
                'external_id': x['external_id'],
                'import_action': 1
            } for x in planogram_for_import
        ]

        # test import data for product_templates
        self.assertListEqual(
            except_planogram_for_import,
            planogram_for_import_match_data,
            'Fail planogram update action for product_templates'
        )

        # test import data for layout_columns
        self.assertListEqual(
            except_column_for_import,
            column_for_import,
            'Fail planogram update action for layout_columns'
        )

        # test import data for tags
        self.assertListEqual(
            except_tags,
            tags_create_list,
            'Fail planogram update action for tags'
        )

        # test import data for layout_components
        self.assertListEqual(
            except_component_data,
            component_for_import,
            'Fail planogram update action for layout_components'
        )


if __name__ == '__main__':
    unittest.main()