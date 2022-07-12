import json
import os
from random import choice
import shutil
import time
from unittest import TestCase
from unittest.mock import patch

from common.logging.setup import logger
from common.mixin.enum_errors import EnumValidationMessage as enum_msg
from common.mixin.handle_file import CsvValidatorHandleFIle
from common.mixin.validation_const import return_import_type_based_on_parser, \
    machineParser
from common.mixin.validator_import import (
    WORKING_DIR, HISTORY_FAIL_DIR, STORE_DIR, ZIP_WORKING_DIR, \
    HISTORY_SUCCESS_DIR
)
from common.validators.csv.csv_validator import ProcessLogger
from common.validators.file_utils import create_file_path, create_file, \
    create_zip, clear_test_dirs
from common.validators.db_testing_utils import clear_db
from database.cloud_database.core.query import CustomUserQueryOnCloud


TEST_DATA = json.loads(os.environ['TEST'])
FTP_USERNAME = TEST_DATA['username']
FTP_PASSWORD = TEST_DATA['password']
COMPANY_ID = TEST_DATA['company_id']
USER_ID = TEST_DATA['user_id']


class TestHandleFile(TestCase):
    def setUp(self):
        clear_test_dirs()
        clear_db()

        self.filename = ''
        self.import_type = 'MACHINES'
        self.company_id = COMPANY_ID
        self.process_logger = ProcessLogger
        self.delimiter = ';'
        self.token = CustomUserQueryOnCloud. \
            get_auth_token_from_user_id(USER_ID)

        self.csv_validator_handle_file = CsvValidatorHandleFIle(
            self.filename,
            self.import_type,
            self.company_id,
            self.process_logger,
            self.delimiter,
            self.token
        )

    def tearDown(self):
        time.sleep(0.5)
        clear_test_dirs()
        clear_db()

    def test_get_import_type(self):
        result = self.csv_validator_handle_file.get_import_type()

    def test_check_file_extension(self):
        extension = '.xlsx'
        file_path = '/example/file_path/name_of_file' + extension
        self.csv_validator_handle_file.filename = file_path
        result = self.csv_validator_handle_file.check_file_extension()
        self.assertEqual(extension, result)

    @patch.object(ProcessLogger, 'update_system_log_flow')
    def test_check_file_extension__no_extension(
            self,
            mock_update_system_log_flow
    ):
        file_path = '/example/file_path/name_of_file'
        self.csv_validator_handle_file.filename = file_path

        with self.assertLogs('application', level='ERROR') as cm:
            result = self.csv_validator_handle_file. \
                check_file_extension()

            mock_update_system_log_flow.assert_called_once_with(
                self.csv_validator_handle_file.filename,
                key_enum=enum_msg.FILE_EXTENSION_ERROR.value
            )
            self.assertEqual('', result)

    @patch.object(ProcessLogger, 'update_system_log_flow')
    @patch.object(os.path, 'splitext')
    def test_check_file_extension__exception(
            self,
            mock_splitext,
            mock_update_system_log_flow
    ):
        mock_splitext.side_effect = Exception('Exception info')
        file_path = '/example/file_path/name_of_file.csv'
        self.csv_validator_handle_file.filename = file_path

        with self.assertRaises(Exception) as cm:
            self.csv_validator_handle_file.check_file_extension()

        mock_update_system_log_flow.assert_called_once_with(
            None,
            key_enum=enum_msg.FILE_WRONG_FORMAT.value
        )

    def test_return_variable_type__return_string(self):
        input_data = ['word', '12', '12.0', '12.23', 12, 12.0, 12.23]
        for data in input_data:
            result = self.csv_validator_handle_file.return_variable_type(data)
            self.assertEqual(type(result).__name__, 'str')

    def test_return_variable_type__return_none(self):
        input_data = ['', None]
        for data in input_data:
            result = self.csv_validator_handle_file.return_variable_type(data)
            self.assertEqual(type(result).__name__, 'NoneType')

    def test_serializer_data__missing_field(self):
        import_type = 'locations'
        parser = return_import_type_based_on_parser(import_type.upper())
        required_headers = parser['required']

        file_path = create_file(
            base_path=STORE_DIR,
            file_name='test_file',
            extension='.csv',
            import_type=import_type,
            delimiter=';',
            validity='missing_field'
        )[1]

        result = self.csv_validator_handle_file.serializer_data(
            file_path,
            parser,
            'csv_validator'
        )

        # assert that the first row, the headers, correspond to locationParser
        # defined required headers
        self.assertEqual(set(result[0]), set(required_headers))

    def test_serializer_data__regions_missing_header(self):
        import_type = 'regions'
        parser = return_import_type_based_on_parser(import_type.upper())
        required_headers_len = len(parser['required'])

        file_path = create_file(
            base_path=STORE_DIR,
            file_name='test_file',
            extension='.csv',
            import_type=import_type,
            delimiter=';',
            validity='missing_header'
        )[1]

        result = self.csv_validator_handle_file.serializer_data(
            file_path,
            parser,
            'csv_validator'
        )

        # assert that the length resulting headers correspond to required headers,
        # as defined by regionsParser, minus one
        self.assertEqual(len(result[0]), required_headers_len-1)

    def test_serializer_data__products_all_fields_valid(self):
        import_type = 'products'
        parser = return_import_type_based_on_parser(import_type.upper())
        required_headers = parser['required']

        file_path = create_file(
            base_path=STORE_DIR,
            file_name='test_file',
            extension='.csv',
            import_type=import_type,
            delimiter=';',
            validity='valid',
            fields='all_fields'
        )[1]

        result = self.csv_validator_handle_file.serializer_data(
            file_path,
            parser,
            'csv_validator'
        )

        # check that every header is present and that is has some value
        for header in required_headers:
            self.assertTrue(result[0][header])

    def test_serializer_data__machines_all_fields_valid(self):
        import_type = 'machines'
        parser = return_import_type_based_on_parser(import_type.upper())
        required_headers = parser['all_fields']

        file_path = create_file(
            base_path=STORE_DIR,
            file_name='test_file',
            extension='.csv',
            import_type=import_type,
            delimiter=';',
            validity='valid',
            fields='all_fields'
        )[1]

        result = self.csv_validator_handle_file.serializer_data(
            file_path,
            parser,
            'csv_validator'
        )

        # check that every header in result corresponds to header in parser
        self.assertEqual(set(required_headers), set(result[0].keys()))

    def test_serializer_data__locations_valid_data(self):
        import_type = 'locations'
        parser = return_import_type_based_on_parser(import_type.upper())
        required_headers = parser['required']

        file_path = create_file(
            base_path=STORE_DIR,
            file_name='test_file',
            extension='.csv',
            import_type=import_type,
            delimiter=';',
            validity='valid'
        )[1]

        result = self.csv_validator_handle_file.serializer_data(
            file_path,
            parser,
            'csv_validator'
        )

        # check that every header is present and that is has some value
        for header in required_headers:
            self.assertTrue(result[0][header])

    def test_handle_zip_file__zipped_files(self):
        """Test handle_zip_file method with a zip archive."""
        # create several files
        files_list = []
        extensions = ['.csv', '.txt']
        validities = ['valid', 'missing_header']
        for i in range(9):
            file_path = create_file(
                base_path=STORE_DIR,
                file_name='file_'+str(i),
                extension=choice(extensions),
                import_type='machines',
                delimiter=';',
                validity='valid'
            )
            files_list.append(file_path[1])

        # create zip archive with the files
        destination_path = create_file_path(
            base_path=STORE_DIR,
            file_name='archive',
            extension='.zip'
        )
        zipped_archive = create_zip(
            zip_path=destination_path,
            file_paths=files_list
        )

        # call tested method with the zip file as parameter
        result = self.csv_validator_handle_file. \
            handle_zip_file(zipped_archive)

        self.assertEqual(
            set(
                [os.path.join(WORKING_DIR, os.path.basename(file_name)) \
                    for file_name in files_list
                ]
            ),
            set(result)
        )

    def test_handle_zip_file__zipped_single_file(self):
        """Test handle_zip_file method with a zip archive."""
        file_path = create_file(
            base_path=STORE_DIR,
            file_name='file_test',
            extension='.csv',
            import_type='machines',
            delimiter=';',
            validity='valid'
        )[1]
        files_list = [file_path]

        # create zip archive with the files
        destination_path = create_file_path(
            base_path=STORE_DIR,
            file_name='archive',
            extension='.zip'
        )
        zipped_archive = create_zip(
            zip_path=destination_path,
            file_paths=files_list
        )

        # call tested method with the zip file as parameter
        result = self.csv_validator_handle_file. \
            handle_zip_file(zipped_archive)

        self.assertEqual(
            set(
                [os.path.join(WORKING_DIR, os.path.basename(file_name)) \
                    for file_name in files_list
                ]
            ),
            set(result)
        )

    def test_handle_zip_file__zipped_dir(self):
        """
        Test handle_file with zipped folder containing files.
        """
        files_list = []
        extensions = ['.csv', '.txt']
        validities = ['valid', 'missing_header', 'missing_field']
        for i in range(16):
            file_path = create_file(
                base_path=STORE_DIR,
                file_name='file_'+str(i),
                extension=choice(extensions),
                import_type='machines',
                delimiter=';',
                validity=choice(validities)
            )
            files_list.append(file_path[1])

        # zip the folder
        destination_path = create_file_path(
            base_path=STORE_DIR,
            file_name='archived_folder',
            extension='.zip'
        )
        zipped_archive = create_zip(
            zip_path=destination_path,
            file_paths=files_list,
            parent_folder='parent_folder'
        )

        # call the tested method
        result = self.csv_validator_handle_file. \
            handle_zip_file(zipped_archive)

        self.assertEqual(
            set(
                [os.path.join(WORKING_DIR, os.path.basename(file_name)) \
                    for file_name in files_list
                ]
            ),
            set(result)
        )

    def test_handle_zip_file__zipped_dir_single_file(self):
        """
        Test handle_file with zipped folder containing files.
        """
        file_path = create_file(
            base_path=STORE_DIR,
            file_name='file_test',
            extension='.csv',
            import_type='machines',
            delimiter=';',
            validity='valid'
        )[1]
        files_list = [file_path]

        # zip the folder
        destination_path = create_file_path(
            base_path=STORE_DIR,
            file_name='archived_folder',
            extension='.zip'
        )
        zipped_archive = create_zip(
            zip_path=destination_path,
            file_paths=files_list,
            parent_folder='parent_folder'
        )

        # call the tested method
        result = self.csv_validator_handle_file. \
            handle_zip_file(zipped_archive)

        self.assertEqual(
            set([os.path.join(WORKING_DIR, os.path.basename(file_path))]),
            set(result)
        )

    @patch.object(ProcessLogger, 'update_system_log_flow')
    def NOtest_handle_zip_file__folder_inside_folder(
        self,
        mock_update_system_log_flow
    ):
        files_list = []
        extensions = ['.csv', '.txt']
        validities = ['valid', 'missing_header', 'missing_field']
        for i in range(16):
            file_path = create_file(
                base_path=STORE_DIR,
                file_name='file_'+str(i),
                extension=choice(extensions),
                import_type='machines',
                delimiter=';',
                validity=choice(validities)
            )
            files_list.append(file_path[1])

        # zip the folder
        destination_path = create_file_path(
            base_path=STORE_DIR,
            file_name='archived_folder',
            extension='.zip'
        )
        zipped_archive = create_zip(
            zip_path=destination_path,
            file_paths=files_list,
            parent_folder='parent_folder/sub_folder'
        )

        #with self.assertRaises(Exception) as cm:
        # call the tested method
        result = self.csv_validator_handle_file. \
            handle_zip_file(zipped_archive)

            #mock_update_system_log_flow.assert_called_with(
            #    'Exception info',
            #    key_enum=enum_msg.RENAME_LOCAL_FILE_ERROR.value
            #)
