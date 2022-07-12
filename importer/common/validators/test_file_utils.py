import os
import pandas
from unittest import TestCase

from common.validators.file_utils import clear_test_dirs
from common.mixin.validation_const import (
    machineParser, locationParser, productParser, regionsParser
)
from common.mixin.validator_import import STORE_DIR
from common.validators.file_utils import write_csv


class TestFileUtils(TestCase):
    def setUp(self):
        clear_test_dirs()

    def tearDown(self):
        clear_test_dirs()

    def test_write_csv__machines(self):
        filepath = os.path.join(STORE_DIR, 'file_test.csv')
        import_type = 'machines'
        delimiter = ';'
        validity = 'valid'

        result = write_csv(filepath, import_type, delimiter, validity)

        self.assertTrue(os.path.isfile(filepath))

        data_frame = pandas.read_csv(filepath, sep=';')

        # compare with the machineParser defined columns
        read_columns = data_frame.columns.tolist()
        self.assertEqual(read_columns, machineParser['required'])

    def test_write_csv__locations_missing_value(self):
        filepath = os.path.join(STORE_DIR, 'file_test.csv')
        import_type = 'locations'
        delimiter = ';'
        validity = 'missing_field'

        result = write_csv(filepath, import_type, delimiter, validity)

        self.assertTrue(os.path.isfile(filepath))

        data_frame = pandas.read_csv(filepath, sep=';')
        # replace nan values
        data_frame = data_frame.fillna('removed')

        rows = data_frame.values.tolist()
        first_row = rows[0]

        for value in first_row:
            if value == 'removed':
                del first_row[first_row.index(value)]

        # assert number of values in row after removing nan values
        self.assertEqual(len(first_row), 2)

        read_columns = data_frame.columns.tolist()
        self.assertEqual(read_columns, locationParser['required'])

    def test_write_csv__regions_missing_header(self):
        required_headers = regionsParser['required']
        filepath = os.path.join(STORE_DIR, 'file_test.csv')
        import_type = 'regions'
        delimiter = '|'
        validity = 'missing_header'

        result = write_csv(filepath, import_type, delimiter, validity)

        data_frame = pandas.read_csv(filepath, sep='|', keep_default_na=False)

        columns = data_frame.columns.tolist()

        # pandas added 'Unnamed' to empty string columns,
        # remove these columns, so that we know the number of
        # non-empty columns
        for column in columns:
            if 'Unnamed' in column:
                del columns[columns.index(column)]

        self.assertTrue(os.path.isfile(filepath))
        # assert number of columns after removing 'nan' values
        self.assertEqual(len(columns), 2)

    def test_write_csv__product_wrong_header(self):
        """Test that csv writer works with validity 'wrong_header'."""
        filepath = os.path.join(STORE_DIR, 'file_test.csv')
        import_type = 'products'
        delimiter = '|'
        validity = 'wrong_header'

        result = write_csv(filepath, import_type, delimiter, validity, 'all_fields')

        expected_headers = productParser['all_fields']
        # read file that we've written
        data_frame = pandas.read_csv(filepath, sep='|')
        # extract columns
        columns = data_frame.columns.tolist()
        self.assertNotEqual(expected_headers[6], columns[6])
        self.assertEqual(columns[6], 'wrong_header')
