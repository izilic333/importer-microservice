from elasticsearch.exceptions import TransportError
import ftputil
from ftputil import FTPHost
import json
import os
import time
import shutil
import sys
from threading import Thread
from unittest import TestCase
from unittest.mock import Mock
from unittest.mock import patch
import urllib3
import xlsxwriter

from common.logging.setup import logger
from common.mixin.elastic_login import ElasticCloudLoginFunctions
import common.mixin.enum_errors as const
from common.mixin.enum_errors import EnumErrorType, \
    enum_message_on_specific_language
from common.mixin.enum_errors import EnumValidationMessage as enum_msg
from common.mixin.handle_file import CsvValidatorHandleFIle as handle_file
from common.mixin.validation_const import return_import_type_based_on_parser
from common.mixin.validator_import import (
    escape_list, WORKING_DIR, HISTORY_FAIL_DIR, STORE_DIR, ZIP_WORKING_DIR, \
    HISTORY_SUCCESS_DIR, HISTORY_FILES_DIR
)
from common.validators.file_utils import create_file_path, create_file, \
    create_zip, clear_test_dirs
from common.validators.csv.csv_validator import ParseFileBasedOnExtension, \
    ProcessLogger, CsvFileValidatorRemote, CsvFileValidatorLocal, \
    FieldValidation, generate_elastic_process
from common.validators.db_testing_utils import clear_db
from database.cloud_database.core.query import CustomUserQueryOnCloud
from database.company_database.core.query_history import CompanyFailHistory
#from test import log_test


TEST_DATA = json.loads(os.environ['TEST'])
FTP_USERNAME = TEST_DATA['username']
FTP_PASSWORD = TEST_DATA['password']
COMPANY_ID = TEST_DATA['company_id']
USER_ID = TEST_DATA['user_id']
FTP_HOME = TEST_DATA['ftp_home']


class TestParseFileBasedOnExtension(TestCase):
    def create_xlsx(self):
        """Create proper .xlsx file."""
        xlsx_filepath = self.xlsx_filepath = STORE_DIR + '/test_excel_file.xlsx'
        workbook = xlsxwriter.Workbook(xlsx_filepath)
        worksheet = workbook.add_worksheet()
        worksheet.set_column('A:A', 20)
        worksheet.write('A1', 'Title A')
        worksheet.write('A2', 'š đ č ć ž')
        worksheet.write('A3', '13.935')
        worksheet.set_column('B:B', 24)
        worksheet.write('B1', 'Title B')
        worksheet.write('B2', 'ш ђ ч ћ ж')
        worksheet.write('B3', '7.689')
        worksheet.set_column('C:C', 17)
        worksheet.write('C1', 'Title C')
        worksheet.write('C2', 'Ä äÜ üÖ ö ß')
        worksheet.write('C3', '189.001')
        workbook.close()

    def create_xls(self):
        """Create proper .xls file."""
        xls_filepath = self.xls_filepath = STORE_DIR + '/test_excel_file.xls'
        workbook = xlsxwriter.Workbook(xls_filepath)
        worksheet = workbook.add_worksheet()
        worksheet.set_column('A:A', 20)
        worksheet.write('A1', 'Title A')
        worksheet.write('A2', 'š đ č ć ž')
        worksheet.write('A3', '13.935')
        worksheet.set_column('B:B', 24)
        worksheet.write('B1', 'Title B')
        worksheet.write('B2', 'ш ђ ч ћ ж')
        worksheet.write('B3', '7.689')
        worksheet.set_column('C:C', 17)
        worksheet.write('C1', 'Title C')
        worksheet.write('C2', 'Ä äÜ üÖ ö ß')
        worksheet.write('C3', '189.001')
        workbook.close()

    def create_empty_file(self):
        """
        Create non-excel (no excel defining headers) file with .xlsx
        extension.
        """
        empty_file = self.empty_file = STORE_DIR + '/empty_file.xlsx'
        with open(empty_file, 'w') as file_open:
            file_open.write('Some text here\n')
        return empty_file

    def create_non_xls_file(self):
        """Create non-excel file with .xls extension."""
        non_xls_file = self.non_xls_file = STORE_DIR + '/non_xls_file.xls'
        file_open = open(non_xls_file, 'w')
        file_open.write('Some text here\n')
        file_open.close()
        return non_xls_file

    def setUp(self):
        clear_test_dirs()
        clear_db()

        self.create_xlsx()
        self.create_xls()

        self.company_id = COMPANY_ID
        self.import_type = 'MACHINES'
        self.elastic_hash = ElasticCloudLoginFunctions.create_process(
            company_id=self.company_id,
            import_type=self.import_type,
            process_request_type='FILE'
        )
        self.user_token = CustomUserQueryOnCloud.get_auth_token_from_user_id(
            USER_ID
        )

        self.process_logger = ProcessLogger(
            company_id=self.company_id,
            import_type=self.import_type,
            elastic_hash=self.elastic_hash,
            token=self.user_token['token']
        )

    def tearDown(self):
        time.sleep(0.5)
        clear_db()
        clear_test_dirs()

    def test_return_file_extension(self):
        file_parse = '/example/path/test_file.xlsx'
        delimiter = ';'
        parse_file_based_on_ext = ParseFileBasedOnExtension(
            file_parse, delimiter, self.process_logger
        )
        return_file_ext = parse_file_based_on_ext.return_file_extension()
        self.assertEqual(('xlsx', '/example/path/test_file'), return_file_ext)

    def test_return_file_extension__not_found(self):
        file_parse = '/example/path/test_file.noname'
        delimiter = ';'
        parse_file_based_on_ext = ParseFileBasedOnExtension(
            file_parse, delimiter, self.process_logger
        )
        return_file_ext = parse_file_based_on_ext.return_file_extension()
        self.assertEqual((False, None), return_file_ext)

    @patch.object(logger, 'info')
    def test_open_xlsx_file_and_convert_to_csv__xlsx(
            self,
            mocked_logger_info
        ):
        delimiters = [',', ';', '|']
        for delimiter in delimiters:
            file_parse = self.xlsx_filepath
            parse_file_based_on_ext = ParseFileBasedOnExtension(
                file_parse, delimiter, self.process_logger
            )
            new_file_path = os.path.splitext(file_parse)[0]

            # call the tested method
            result = parse_file_based_on_ext. \
                open_xlsx_file_and_convert_to_csv(new_file_path)

            expected = {
                'success': True,
                'file_path': file_parse.replace('.xlsx', '.csv')
            }
            self.assertEqual(expected, result)
            mocked_logger_info.assert_called_with(
                self.process_logger.update_system_log_flow(
                    file_parse.replace('.xlsx', '.csv'),
                    key_enum=enum_msg. \
                        VALIDATION_SYSTEM_LOG_INFO_CONVERTED_FILE.value
                )
            )

    @patch.object(logger, 'info')
    def test_open_xlsx_file_and_convert_to_csv__xls(
            self,
            mocked_logger_info
        ):
        delimiters = [',', ';', '|']
        for delimiter in delimiters:
            file_parse = self.xls_filepath
            parse_file_based_on_ext = ParseFileBasedOnExtension(
                file_parse, delimiter, self.process_logger
            )
            new_file_path = os.path.splitext(file_parse)[0]

            # call the tested method
            result = parse_file_based_on_ext. \
                open_xlsx_file_and_convert_to_csv(new_file_path)

            expected = {
                'success': True,
                'file_path': file_parse.replace('.xls', '.csv')
            }
            self.assertEqual(expected, result)
            mocked_logger_info.assert_called_with(
                self.process_logger.update_system_log_flow(
                    file_parse.replace('.xls', '.csv'),
                    key_enum=enum_msg. \
                        VALIDATION_SYSTEM_LOG_INFO_CONVERTED_FILE.value
                )
            )

    @patch.object(ParseFileBasedOnExtension, 'convert_xlsx_with_csv_reader')
    @patch.object(logger, 'error')
    def test_open_xlsx_file_and_convert_to_csv__fail_pandas(
            self,
            mocked_logger_error,
            mocked_convert_xlsx_with_csv_reader
    ):
        """
        Test method open_xslsx_file_and_convert_to_csv for fail,
        by giving it non-excel file.
        """
        file_parse = self.create_empty_file()
        delimiter = ','
        parse_file_based_on_ext = ParseFileBasedOnExtension(
            file_parse,
            delimiter,
            self.process_logger
        )
        new_file_path = os.path.splitext(file_parse)[0]

        # call the tested method
        parse_file_based_on_ext. \
            open_xlsx_file_and_convert_to_csv(new_file_path)

        self.assertTrue(mocked_logger_error.called)
        self.assertTrue(mocked_convert_xlsx_with_csv_reader.called)

    def test_convert_xlsx_with_csv_reader(self):
        """
        Test by giving it a .csv formatted file with changed extension to .xls.
        Perhaps an unneccessary feature.
        """
        delimiter = ';'
        file_parse = create_file(
           base_path=STORE_DIR,
           extension='.csv',
           import_type='machines',
           delimiter=delimiter
        )[1]
        # change extension to .xls
        new_file_path = file_parse.replace('.csv', '.xls')
        os.rename(file_parse, new_file_path)

        parse_file_based_on_ext = ParseFileBasedOnExtension(
            new_file_path,
            delimiter,
            self.process_logger
        )
        result = parse_file_based_on_ext.convert_xlsx_with_csv_reader()
        expected = {'success': True, 'file_path': file_parse.replace('.xls', '.csv')}
        self.assertEqual(expected, result)

    def test_convert_xlsx_with_csv_reader__fail(self):
        """
        Test convert_xlsx_second_attempt fails with non-excel file.
        """
        file_parse = self.xlsx_filepath
        delimiter = ';'
        parse_file_based_on_ext = ParseFileBasedOnExtension(
            file_parse,
            delimiter,
            self.process_logger
        )
        with self.assertLogs('application', level='INFO') as cm:
            result = parse_file_based_on_ext.convert_xlsx_with_csv_reader()
            expected = {'success': False, 'file_path': file_parse}
            self.assertEqual(expected, result)

    @patch.object(ParseFileBasedOnExtension, 'open_xlsx_file_and_convert_to_csv')
    def test_start_checking_file__xlsx_success(
            self,
            mocked_open_xlsx_file_and_convert_to_csv
    ):
        """
        Test start_checking_file with normal excel file.
        """
        file_parse = self.xlsx_filepath
        delimiter = ';'
        parse_file_based_on_ext = ParseFileBasedOnExtension(
            file_parse,
            delimiter,
            self.process_logger
        )

        mocked_open_xlsx_file_and_convert_to_csv.return_value = {
            'success': True,
            'file_path': os.path.splitext(file_parse)[0]
        }

        result = parse_file_based_on_ext.start_checking_file()
        expected = {
            'success': True,
            'file_path': os.path.splitext(file_parse)[0]
        }

        self.assertTrue(mocked_open_xlsx_file_and_convert_to_csv.called)
        self.assertEqual(expected, result)

    @patch.object(ParseFileBasedOnExtension, 'open_xlsx_file_and_convert_to_csv')
    def test_start_checking_file__xlsx_fail(
            self,
            mocked_open_xlsx_file_and_convert_to_csv
    ):
        """
        Test start_checking_file with non-excel file.
        """
        file_parse = self.create_empty_file()
        delimiter = ';'
        parse_file_based_on_ext = ParseFileBasedOnExtension(
            file_parse,
            delimiter,
            self.process_logger
        )

        mocked_open_xlsx_file_and_convert_to_csv.return_value = {
            'success': False,
            'file_path': os.path.splitext(file_parse)[0]
        }

        # call start_checking_file
        result = parse_file_based_on_ext.start_checking_file()
        expected = {
            'success': False,
            'file_path': os.path.splitext(file_parse)[0]
        }

        self.assertTrue(mocked_open_xlsx_file_and_convert_to_csv.called)
        self.assertEqual(expected, result)

    def test_start_checking_file__csv(self):
        """
        Test start_checking_file with CSV file.
        """
        file_parse = '/example/path/file.csv'
        delimiter = ';'
        parse_file_based_on_ext = ParseFileBasedOnExtension(
            file_parse,
            delimiter,
            self.process_logger
        )
        result = parse_file_based_on_ext.start_checking_file()
        expected = {'success': True, 'file_path': file_parse}
        self.assertEqual(expected, result)

    def test_start_checking_file__wrong_extension(self):
        """
        Test start_checking_file with filepath with wrong extension.
        """
        file_parse = '/example/path/file.noname'
        delimiter = ';'
        parse_file_based_on_ext = ParseFileBasedOnExtension(
            file_parse,
            delimiter,
            self.process_logger
        )
        result = parse_file_based_on_ext.start_checking_file()
        expected = None
        self.assertEqual(expected, result)


class TestProcessLogger(TestCase):
    def create_empty_file(self):
        """
        Create non-excel (no excel defining headers) file with .xlsx
        extension.
        """
        clear_test_dirs()
        clear_db()

        empty_file = self.empty_file = os.path.join(
            STORE_DIR + '/empty_file.xlsx'
        )
        file_open = open(empty_file, 'w')
        file_open.write('Some text here\n')
        file_open.close()
        return empty_file

    def setUp(self):
        """
        Setup ProcessLogger with initial fields.
        """
        clear_test_dirs()
        clear_db()

        self.company_id = COMPANY_ID
        self.import_type = 'machines'
        self.elastic_hash = generate_elastic_process(
            COMPANY_ID,
            'machines',
            None
        )
        self.token = CustomUserQueryOnCloud. \
            get_auth_token_from_user_id(USER_ID)
        self.data_hash = ''

        self.process_logger = ProcessLogger(
            company_id=self.company_id,
            import_type=self.import_type,
            elastic_hash=self.elastic_hash,
            token=self.token
        )

        self.error = Mock()
        self.error.name = 'error name'
        self.error.value = 400
        self.file_path = '/file/path'
        self.email = 'test@example.com'

    def tearDown(self):
        time.sleep(0.5)
        clear_db()
        clear_test_dirs()


    def test_init(self):
        """Test that class initialization is as expected."""
        self.assertEqual(self.process_logger.company_id, self.company_id)
        self.assertEqual(self.process_logger.import_type, self.import_type)
        self.assertEqual(self.process_logger.elastic_hash, self.elastic_hash)
        self.assertEqual(self.process_logger.token, self.token)
        self.assertEqual(self.process_logger.data_hash, self.data_hash)

    def test_set_data_hash(self):
        """Test set_data_hash."""
        data_hash = 'example-data-hash'
        self.process_logger.set_data_hash(data_hash)
        self.assertEqual(self.process_logger.data_hash, data_hash)

    @patch.object(ElasticCloudLoginFunctions, 'update_main_process')
    @patch.object(ProcessLogger, 'insert_into_fail_history')
    @patch.object(ProcessLogger, 'update_process_and_cloud_flow')
    def test_create_process_and_cloud_flow_and_main(
            self,
            mocked_update_process_and_cloud_flow,
            mocked_insert_into_fail_history,
            mocked_update_main_process,
    ):
        """
        Test create_process_and_cloud_flow_and_main for calling specific
        methods with specific parameters.
        """
        key_enum = enum_msg.USER_ERROR.value
        language = 'en'

        # call the tested method
        self.process_logger.create_process_and_cloud_flow_and_main(
            error=self.error,
            file_path=self.file_path,
            email=self.email,
            language=language,
            key_enum=key_enum
        )

        mocked_update_main_process.assert_called_once_with(
            hash=self.elastic_hash,
            error=self.error.name
        )
        mocked_insert_into_fail_history.assert_called_once_with(
            self.error.value,
            self.process_logger.data_hash,
            self.file_path,
            self.email
        )
        mocked_update_process_and_cloud_flow.assert_called_once_with(
            error=self.error.name,
            language=language,
            key_enum=key_enum
        )

    @patch.object(ElasticCloudLoginFunctions, 'create_cloud_process_flow')
    @patch.object(ProcessLogger, 'update_process_flow')
    def test_update_process_and_cloud_flow(
            self,
            mocked_update_process_flow,
            mocked_create_cloud_process_flow
    ):
        """
        Test update_process_and_cloud_flow for calling specific methods.
        """
        language = 'en'
        key_enum = enum_msg.USER_ERROR.value

        # call update_process_and_cloud_flow
        self.process_logger.update_process_and_cloud_flow(
            error=self.error,
            language=language,
            key_enum=key_enum
        )

        message = enum_message_on_specific_language(key_enum, language)
        mocked_create_cloud_process_flow.assert_called_once_with(
            hash=self.elastic_hash,
            error=self.error,
            message=message
        )
        mocked_update_process_flow.assert_called_once_with(
            error=self.error,
            message=message,
            key_enum=key_enum
        )

    @patch.object(ElasticCloudLoginFunctions, 'create_process_flow')
    @patch.object(logger, 'error')
    def test_update_process_flow(
        self,
        mocked_logger_error,
        mocked_create_process_flow
    ):
        """Test update_process_flow method."""
        error_type = 'TEST_ERROR_TYPE'
        error_message = 'This is test error message.'

        # call update_process_flow
        self.process_logger.update_process_flow(
            error=error_type,
            message=error_message,
        )
        mocked_create_process_flow.assert_called_once_with(
            hash=self.elastic_hash,
            error=error_type,
            message=error_message
        )
        mocked_logger_error.assert_called_once_with(
            '{} - {}'.format(error_type, error_message)
        )
        '''
        test_log(
            error_type,
            '.'.join((
                __name__,
                self.__class__.__name__,
                sys._getframe().f_code.co_name
            ))
        )
        '''

    @patch.object(CompanyFailHistory, 'insert_fail_history')
    @patch.object(Thread, 'start')
    @patch('common.mixin.mixin.generate_uid')
    def test_insert_fail_history(
        self,
        mocked_generate_uid,
        mocked_thread_start,
        mocked_insert_fail_history
    ):
        """Test insert_into_fail_history method."""
        self.file_path = self.create_empty_file()
        mocked_generate_uid.return_value = '1234567890'

        # call insert_into_fail_history
        self.process_logger.insert_into_fail_history(
            error=self.error,
            data_hash=self.data_hash,
            file_path=self.file_path,
            email=self.email
        )

        mocked_insert_fail_history.called_once_with(
            self.company_id,
            self.import_type,
            self.elastic_hash,
            self.data_hash,
            os.path.join(HISTORY_FAIL_DIR, '{}_{}${}'.format(
                self.company_id,
                mocked_generate_uid,
                os.path.split(self.file_path)[1]
            )),
            self.error,
            self.token
        )

        self.assertTrue(mocked_thread_start.called)

    @patch.object(CompanyFailHistory, 'insert_fail_history')
    @patch.object(Thread, 'start')
    @patch('common.mixin.mixin.generate_uid')
    def test_insert_fail_history__existing_data_hash(
        self,
        mocked_generate_uid,
        mocked_thread_start,
        mocked_insert_fail_history
    ):
        """Test insert_into_fail_history method when data_hash exists."""
        self.file_path = self.create_empty_file()
        self.data_hash = 'some-hash'
        mocked_generate_uid.return_value = '1234567890'

        # call insert_into_fail_history
        self.process_logger.insert_into_fail_history(
            error=self.error,
            data_hash=self.data_hash,
            file_path=self.file_path,
            email=self.email
        )

        mocked_insert_fail_history.called_once_with(
            self.company_id,
            self.import_type,
            self.elastic_hash,
            self.data_hash,
            os.path.join(HISTORY_FAIL_DIR, '{}_{}${}'.format(
                self.company_id,
                mocked_generate_uid,
                os.path.split(self.file_path)[1]
            )),
            self.error,
            self.token
        )

        self.assertTrue(mocked_thread_start.called)

    @patch.object(Thread, 'start')
    @patch.object(CompanyFailHistory, 'insert_fail_history')
    @patch.object(logger, 'error')
    @patch.object(shutil, 'copy2')
    @patch('common.mixin.mixin.generate_uid')
    def test_insert_fail_history__exception(
        self,
        mocked_generate_uid,
        mocked_copy2,
        mocked_logger_error,
        mocked_insert_fail_history,
        mocked_thread_start
    ):
        """
        Test insert_into_fail_history method when exception is triggered.
        """
        self.file_path = self.create_empty_file()
        mocked_generate_uid.return_value = '1234567890'
        # make copy2 method trigger an exception
        mocked_copy2.side_effect = Exception

        # call insert_into_fail_history
        self.process_logger.insert_into_fail_history(
            error=self.error,
            data_hash=self.data_hash,
            file_path=self.file_path,
            email=self.email
        )

        mocked_insert_fail_history.called_once_with(
            self.company_id,
            self.import_type,
            self.elastic_hash,
            self.data_hash,
            os.path.join(HISTORY_FAIL_DIR, '{}_{}${}'.format(
                self.company_id,
                mocked_generate_uid,
                os.path.split(self.file_path)[1]
            )),
            self.error,
            self.token
        )

        self.assertTrue(mocked_logger_error.called)
        #mocked_logger_error.assert_called_once_with(
        #    'Error moving file to HISTORY_FAIL_DIR {}'. \
        #        format(context.print_exc())
        #)
        self.assertTrue(mocked_thread_start.called)


class TestCsvFileValidatorRemote(TestCase):
    def setUp(self):
        """
        Initial data required for tests to work and class initialization.
        """
        clear_test_dirs()
        clear_db()

        username = self.username = FTP_USERNAME
        self.input_params = {
            'company': COMPANY_ID,
            'url': 'localhost',
            'username': username,
            'password': FTP_PASSWORD,
            'port': 21,
            'ftp_path': FTP_HOME,
            'file_delimiters': ';',
            'email': ['test1@example.com', 'test2@example.com'],
            'category_import': 3,
            'user_id': USER_ID
        }
        self.csv_file_validator_remote = CsvFileValidatorRemote(self.input_params)


    def tearDown(self):
        time.sleep(0.5)
        clear_db()
        clear_test_dirs()

    def test_init(self):
        self.assertEqual(
            self.csv_file_validator_remote.company_id,
            self.input_params['company']
        )

    @patch.object(ElasticCloudLoginFunctions, 'create_process')
    def test_generate_elastic_process(
            self,
            mocked_create_process
    ):
        result = generate_elastic_process(
            self.csv_file_validator_remote.company_id,
            self.csv_file_validator_remote.import_type_execute,
            'FILE'
        )
        mocked_create_process.assert_called_once_with(
            self.csv_file_validator_remote.company_id,
            self.csv_file_validator_remote.import_type_execute,
            'FILE'
        )

    def test_get_remote_file__success(self):
        """
        Test get_remote_file with file created on ftp path.
        """
        filepath = create_file(
            base_path=FTP_HOME,
            extension='.csv',
            import_type='machines',
            validity='valid'
        )[1]

        result = self.csv_file_validator_remote.get_remote_file()
        self.assertEqual(os.path.basename(filepath), result[0]['name'])
        self.assertTrue(result[0]['time'])

    def test_get_remote_file__success_port_22(self):
        filepath = create_file(
            base_path=FTP_HOME,
            extension='.csv',
            import_type='machines',
            validity='valid'
        )[1]

        self.csv_file_validator_remote.port = 22
        result = self.csv_file_validator_remote.get_remote_file()
        self.assertEqual(
            os.path.join(STORE_DIR, os.path.basename(filepath)),
            result[0]['name']
        )
        self.assertTrue(result[0]['time'])

    @patch.object(ProcessLogger, 'create_process_and_cloud_flow_and_main')
    def test_get_remote_file__no_user_token(
            self,
            mocked_create_process_and_cloud_flow_and_main
    ):
        self.csv_file_validator_remote.user_token['status'] = False
        result = self.csv_file_validator_remote.get_remote_file()
        mocked_create_process_and_cloud_flow_and_main.assert_called_with(
            error=EnumErrorType.FAIL,
            file_path='',
            email=self.csv_file_validator_remote.emails,
            language='en',
            key_enum=enum_msg.USER_ERROR.value
        )
        self.assertEqual({'success': False}, result)

    @patch.object(ProcessLogger, 'create_process_and_cloud_flow_and_main')
    @patch.object(ftputil, 'FTPHost')
    def test_get_remote_file__ftp_connect_fail(
            self,
            mocked_ftp_host,
            mocked_create_process_and_cloud_flow_and_main,
    ):
        """
        Test get_remote_file method for ftp connection failure. Mocking
        ProcessLogger in order to check that it is called with the right
        arguments.
        """
        mocked_ftp_host.side_effect = Exception('Exc info')

        result = self.csv_file_validator_remote.get_remote_file()

        mocked_create_process_and_cloud_flow_and_main.assert_called_with(
            error=EnumErrorType.ERROR,
            file_path='',
            email=self.csv_file_validator_remote.emails,
            language='en',
            key_enum=enum_msg.FTP_CONNECTION_ERROR.value
        )
        self.assertEqual({'success': False}, result)

    @patch.object(os, 'chdir')
    @patch.object(ProcessLogger, 'create_process_and_cloud_flow_and_main')
    def test_get_remote_file__change_dir_fail(
            self,
            mocked_create_process_and_cloud_flow_and_main,
            mocked_chdir
    ):
        """
        Test get_remote_file for exception when changing directory.
        """
        mocked_chdir.side_effect = OSError('Exc info')

        result = self.csv_file_validator_remote.get_remote_file()

        mocked_create_process_and_cloud_flow_and_main.assert_called_with(
            None,
            error=EnumErrorType.ERROR,
            file_path='',
            email=self.csv_file_validator_remote.emails,
            language='en',
            key_enum=enum_msg.FTP_PATH_ERROR.value,
        )
        self.assertEqual({'success': False}, result)

    @patch.object(ftputil.FTPHost, 'download')
    @patch.object(ProcessLogger, 'update_process_and_cloud_flow')
    def test_get_remote_file__download_file_fail(
            self,
            mock_update_process_and_cloud_flow,
            mock_download
    ):
        """
        Test get_remote_file for exception when downloading file.
        """
        ext = '.txt'
        file_path = create_file(
            base_path=FTP_HOME,
            extension=ext
        )[1]
        mock_download.side_effect = Exception('Exc info')

        with self.assertLogs('application', level='ERROR') as cm:
            result = self.csv_file_validator_remote.get_remote_file()

            # this is the second call to update_process_and_cloud_flow
            mock_update_process_and_cloud_flow.assert_called_with(
                'localhost',
                error=EnumErrorType.FAIL.name,
                language='en',
                key_enum=enum_msg.VALIDATION_MAIN_PROCESS_FTP.value
            )
            self.assertEqual(None, result)

    @patch.object(logger, 'error')
    def test_get_remote_file__disallowed_extension(
            self,
            mock_logger_error
    ):
        """
        Test get_remote method with file with wrong extension. Check that
        file is removed from STORE_DIR and that logger is called for error
        message.

        TODO: Test content that is sent with the logger.
        """
        ext = '.noname'
        file_name = create_file(
            base_path=FTP_HOME,
            extension=ext
        )[0]

        # call tested method
        self.csv_file_validator_remote.get_remote_file()

        # assert that file is removed from STORE_DIR,
        # due to disallowed extension
        file_path = os.path.join(STORE_DIR, file_name+ext)
        self.assertFalse(os.path.isfile(file_path))

        self.assertTrue(mock_logger_error.called)

    @patch.object(ftputil.FTPHost, 'remove')
    @patch.object(logger, 'error')
    @patch.object(ProcessLogger, 'update_system_log_flow')
    @patch.object(ProcessLogger, 'update_process_and_cloud_flow')
    def test_get_remote_file__ftp_remove_fail(
            self,
            mock_update_process_and_cloud_flow,
            mock_update_system_log_flow,
            mock_logger_error,
            mock_ftp_remove
    ):
        file_name = create_file(base_path=FTP_HOME)[0]
        # fake exception when removing with ftp
        mock_ftp_remove.side_effect = Exception('Exc info')

        result = self.csv_file_validator_remote.get_remote_file()

        self.assertTrue(mock_logger_error.called)
        self.assertTrue(mock_update_system_log_flow.called)
        self.assertTrue(mock_update_process_and_cloud_flow.called)
        #new_file_path = os.path.join(STORE_DIR, file_name)
        #self.assertEqual(new_file_path, result[0]['name'])
        #self.assertTrue(result[0]['time'])

    @patch('common.validators.csv.csv_validator.generate_elastic_process')
    @patch('common.validators.csv.csv_validator.ProcessLogger')
    @patch('common.validators.csv.csv_validator.CsvFileValidatorLocal')
    def test_process_ftp_files__assert_calls(
        self,
        mock_generate_elastic_process,
        mock_process_logger,
        mock_csv_file_validator_local
    ):
        mock_update_process_flow = mock_process_logger.update_process_flow \
            = Mock()
        filepath = create_file(
            base_path=FTP_HOME,
            extension='.csv',
            import_type='machines',
            validity='valid'
        )
        self.csv_file_validator_remote.get_remote_file()
        self.csv_file_validator_remote.import_type_execute = 3
        try:
            self.csv_file_validator_remote.process_ftp_files()
        except TransportError:
            pass
        self.assertTrue(mock_generate_elastic_process.called)
        self.assertTrue(mock_process_logger.called)
        self.assertTrue(mock_update_process_flow.called)
        self.assertTrue(mock_csv_file_validator_local.called)

    def test_process_ftp_files__machines(self):
        """
        Test process_ftp_files method with 'machines' file from
        file_examples directory.
        """
        filepath = create_file(
            base_path=FTP_HOME,
            extension='.csv',
            import_type='machines',
            validity='valid'
        )

        # get remote files over ftp
        self.csv_file_validator_remote.get_remote_file()

        # set import type which corresponds to the file
        self.csv_file_validator_remote.import_type_execute = 3

        try:
            # call the tested method
            with self.assertLogs('application', level='ERROR') as cm:
                self.csv_file_validator_remote.process_ftp_files()
        except TransportError:
            # allow this exception to pass
            pass

    def test_process_ftp_files__machine_types(self):
        """
        Test process_ftp_files method with 'machine types' file from
        file_examples directory.
        """
        filepath = create_file(
            base_path=FTP_HOME,
            extension='.csv',
            import_type='machine_types',
            validity='valid'
        )

        # get remote files over ftp
        self.csv_file_validator_remote.get_remote_file()

        # set import type which corresponds to the file
        self.csv_file_validator_remote.import_type_execute = 13

        try:
            # call the tested method
            with self.assertLogs('application', level='ERROR') as cm:
                self.csv_file_validator_remote.process_ftp_files()
        except TransportError:
            # allow this exception to pass
            pass

    def test_process_ftp_files__locations(self):
        """
        Test process_ftp_files method with 'locations' file from
        file_examples directory.
        """
        filepath = create_file(
            base_path=FTP_HOME,
            extension='.csv',
            import_type='locations',
            validity='valid'
        )

        # get remote files over ftp
        self.csv_file_validator_remote.get_remote_file()

        # set import type which corresponds to the file
        self.csv_file_validator_remote.import_type_execute = 4

        try:
            # call the tested method
            with self.assertLogs('application', level='ERROR') as cm:
                self.csv_file_validator_remote.process_ftp_files()
        except TransportError:
            # allow this exception to pass
            pass

    def test_process_ftp_files__regions(self):
        """
        Test process_ftp_files method with 'regions' file from
        file_examples directory.
        """
        filepath = create_file(
            base_path=FTP_HOME,
            extension='.csv',
            import_type='regions',
            validity='valid'
        )

        # get remote files over ftp
        self.csv_file_validator_remote.get_remote_file()

        # set import type which corresponds to the file
        self.csv_file_validator_remote.import_type_execute = 5

        try:
            # call the tested method
            with self.assertLogs('application', level='ERROR') as cm:
                self.csv_file_validator_remote.process_ftp_files()
        except TransportError:
            # allow this exception to pass
            pass

    @patch.object(CsvFileValidatorLocal, 'validation_for_specific_file')
    @patch.object(ProcessLogger, 'update_process_flow')
    def test_process_ftp_files__assert_calls(
        self,
        mock_update_process_flow,
        mock_validation_for_specific_file
    ):
        """
        Test process_ftp_files method making the correct calls.
        """
        filepath = create_file(
            base_path=FTP_HOME,
            extension='.csv',
            import_type='regions',
            validity='valid'
        )

        # prepare and call the tested method
        self.csv_file_validator_remote.get_remote_file()
        self.csv_file_validator_remote.process_ftp_files()

        self.assertTrue(mock_update_process_flow.called)
        self.assertTrue(mock_validation_for_specific_file.called)


class TestCsvFileValidatorLocal(TestCase):
    def setUp(self):
        clear_test_dirs()
        clear_db()

        # initialize dependency class
        self.cfv_remote_input_params = {
            'company': COMPANY_ID,
            'url': 'localhost',
            'username': FTP_USERNAME,
            'password': FTP_PASSWORD,
            'port': 21,
            'ftp_path': FTP_HOME,
            'file_delimiters': ';',
            'email': ['test1@example.com', 'test2@example.com'],
            'category_import': 3,
            'user_id': USER_ID
        }

        cfv_remote = self.cfv_remote = CsvFileValidatorRemote(self.cfv_remote_input_params)

        # set up the actual class for testing
        self.input_params = {
            'company': COMPANY_ID,
            'delimiters': ';',
            'email': 'test@example.com',
            'import_type': 3,
            'language': 'en',
        }
        self.elastic_hash = generate_elastic_process(
            self.input_params['company'],
            self.input_params['import_type'],
            'FILE'
        )
        user_token = CustomUserQueryOnCloud.get_auth_token_from_user_id(
            int(self.cfv_remote_input_params['user_id'])
        )
        self.token = user_token['token']
        file_name = ''
        self.process_logger = ProcessLogger(
            self.input_params['company'],
            self.input_params['import_type'],
            self.elastic_hash,
            self.token
        )

        self.csv_file_validator_local = CsvFileValidatorLocal(
            self.input_params,
            file_name,
            self.elastic_hash,
            self.token
        )

    def tearDown(self):
        time.sleep(0.5)
        clear_db()
        clear_test_dirs()

    def test_init(self):
        self.assertEqual(self.csv_file_validator_local.elastic_hash, self.elastic_hash)
        self.assertEqual(self.csv_file_validator_local.token, self.token)
        self.assertEqual(type(self.csv_file_validator_local.process_logger), ProcessLogger)

    @patch.object(ProcessLogger, 'update_process_flow')
    def test_validation_for_specific_file__assert_calls(
        self,
        mock_update_process_flow
    ):
        file_path = create_file(
            base_path=STORE_DIR,
            file_name='test_file',
            extension='.csv',
            import_type='machines'
        )[1]

        self.csv_file_validator_local.filename = file_path
        result = self.csv_file_validator_local.validation_for_specific_file()
        self.assertTrue(mock_update_process_flow.called)

    @patch.object(FieldValidation, 'get_validation_fields')
    def test_validation_for_specific_file(
        self,
        mock_get_validation_fields
    ):
        file_path = create_file(
            base_path=STORE_DIR,
            file_name='test_file',
            extension='.csv',
            import_type='machines'
        )[1]
        self.csv_file_validator_local.filename = file_path
        helper_methods_main = handle_file(
            file_path,
            self.input_params['import_type'],
            self.input_params['company'],
            self.process_logger,
            self.input_params['delimiters'],
            self.token
        )
        self.csv_file_validator_local.helper_methods_main = helper_methods_main

        try:
            # call tested method
            with self.assertLogs('application', level='ERROR') as cm:
                result = self.csv_file_validator_local.validation_for_specific_file()
                self.assertTrue(mock_get_validation_fields.called)
        except TransportError:
            # allow this exception to pass
            pass

    def test_validation_for_specific_file__products(self):
        import_type = 'products'
        file_path = create_file(
            base_path=STORE_DIR,
            file_name='test_file',
            extension='.csv',
            import_type=import_type,
            fields='all_fields'
        )[1]
        self.csv_file_validator_local.filename = file_path
        helper_methods_main = handle_file(
            file_path,
            import_type,
            self.input_params['company'],
            self.process_logger,
            self.input_params['delimiters'],
            self.token
        )
        self.csv_file_validator_local.helper_methods_main = helper_methods_main

        try:
            # call tested method
            with self.assertLogs('application', level='ERROR') as cm:
                result = self.csv_file_validator_local.validation_for_specific_file()
                self.assertEqual(None, result)
        except TransportError:
            # allow this exception to pass
            pass

    def test_validation_for_specific_file__invalid_headers(self):
        file_path = create_file(
            base_path=STORE_DIR,
            file_name='test_file',
            extension='.csv',
            import_type='machines',
            validity='missing_header'
        )[1]
        self.csv_file_validator_local.filename = file_path

        try:
            # call tested method and capture error log
            with self.assertLogs('application', level='ERROR') as cm:
                result = self.csv_file_validator_local. \
                    validation_for_specific_file()
        except TransportError:
            # allow this exception to pass
            pass

    @patch.object(ProcessLogger, 'update_process_flow')
    def test_validation_for_specific_file__csv(
        self,
        mock_update_process_flow
    ):
        file_path = create_file(
            base_path=STORE_DIR,
            file_name='test_file',
            extension='.csv',
            import_type='regions',
            delimiter='|',
            validity='valid'
        )[1]
        self.csv_file_validator_local.filename = file_path
        self.csv_file_validator_local.delimiter = '|'
        self.csv_file_validator_local.import_type = 5

        # call tested method
        result = self.csv_file_validator_local. \
            validation_for_specific_file()
        self.assertTrue(mock_update_process_flow.called)

    @patch.object(ProcessLogger, 'update_process_and_cloud_flow')
    @patch('common.validators.csv.csv_validator.FieldValidation')
    def test_validation_for_specific_file__zip(
            self,
            mock_field_validation,
            mock_update_process_and_cloud_flow
    ):
        """
        Test with zip archive with valid csv files.

        TODO:
        check parameters in called methods
        """
        files_list = []
        for i in range(4):
            file_name = create_file(
                base_path=FTP_HOME,
                file_name='file_'+str(i),
                extension='.csv',
                import_type='machines',
                delimiter=';',
                validity='valid'
            )
            files_list.append(file_name[1])

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

        # test with a new instance of CsvFileValidatorLocal class
        cfvl = CsvFileValidatorLocal(
            self.input_params,
            zipped_archive,
            self.elastic_hash,
            self.token
        )

        with self.assertLogs(logger='application', level='ERROR') as cm:
            result = cfvl.validation_for_specific_file()
            self.assertTrue(mock_update_process_and_cloud_flow.called)
            self.assertTrue(mock_field_validation.called)

    @patch.object(ProcessLogger, 'create_process_and_cloud_flow_and_main')
    def test_validation_for_specific_file__wrong_extension(
        self,
        mock_create_process_and_cloud_flow_and_main
    ):
        file_path = create_file(
            base_path=STORE_DIR,
            file_name='test_file',
            extension='.noname',
            import_type='machines',
            delimiter=';',
            validity='valid'
        )[1]
        self.csv_file_validator_local.filename = file_path

        with self.assertLogs(logger='application', level='ERROR') as cm:
            result = self.csv_file_validator_local.validation_for_specific_file()

            self.assertEqual(
                {'success': False, 'file': file_path},
                result
            )
            self.assertTrue(mock_create_process_and_cloud_flow_and_main.called)

    @patch.object(logger, 'error')
    def test_validation_for_specific_file__zipped_wrong_ext(
        self,
        mock_logger_error
    ):
        files_list = []
        for i in range(2):
            file_name = create_file(
                base_path=FTP_HOME,
                file_name='file_'+str(i),
                extension='.noname',
                import_type='machines',
                delimiter=';',
                validity='valid'
            )
            files_list.append(file_name[1])

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

        self.csv_file_validator_local.filename = zipped_archive
        self.csv_file_validator_local.delimiter = ';'
        self.csv_file_validator_local.import_type = 3

        result = self.csv_file_validator_local. \
            validation_for_specific_file()
        self.assertTrue(mock_logger_error.called)

    @patch.object(FieldValidation, 'get_validation_fields')
    def test_validation_for_specific_file__machines_empty_row(
        self,
        mock_get_validation_fields
    ):
        file_path = create_file(
            base_path='/home/ivan/Downloads',
            file_name='test_file', #'machines_empty_row'
            extension='.csv',
            import_type='machines',
            delimiter=';',
            fields='required',
            validity='valid'
        )[1]
        self.assertTrue(os.path.isfile(file_path))

        # add blank row in between lines which contain values
        with open(file_path, 'r') as fr:
            contents = fr.readlines()
        contents.insert(2, '\n')
        with open(file_path, 'w+') as fw:
            contents = ''.join(contents)
            fw.write(contents)

        self.csv_file_validator_local.filename = file_path

        helper_methods_main = handle_file(
            file_path,
            self.input_params['import_type'],
            self.input_params['company'],
            self.process_logger,
            self.input_params['delimiters'],
            self.token
        )
        self.csv_file_validator_local.helper_methods_main = helper_methods_main

        with self.assertLogs('application', level='ERROR') as cm:
            result = self.csv_file_validator_local.validation_for_specific_file()
            self.assertTrue(mock_get_validation_fields.called)


class TestFieldValidation(TestCase):
    def setUp(self):
        clear_test_dirs()
        clear_db()

        self.language = 'en'
        self.email = ['test1@example.com', 'test2@example.com']
        self.company_id = COMPANY_ID
        self.import_type = 'MACHINES'
        user_token = CustomUserQueryOnCloud.get_auth_token_from_user_id(
            int(USER_ID))
        self.token = user_token['token']
        self.working_file = ''
        self.process_request_type = None
        self.delimiter = ';'
        self.zip_elastic_hash = generate_elastic_process(
            self.company_id,
            self.import_type,
            self.process_request_type)
        self.zip_process_logger = ProcessLogger(
            self.company_id,
            self.import_type,
            self.zip_elastic_hash,
            self.token)

        self.field_validation = FieldValidation(
            self.language, self.zip_process_logger, self.email,
            self.company_id, self.import_type, self.token,
            self.working_file, self.process_request_type, self.delimiter,
            self.zip_elastic_hash)

    def tearDown(self):
        time.sleep(0.5)
        clear_db()
        clear_test_dirs()

    def test_init(self):
        self.assertEqual(
            self.field_validation.process_logger,
            self.zip_process_logger)
        self.assertEqual(self.field_validation.email, self.email)
        self.assertEqual(self.field_validation.company_id, self.company_id)
        self.assertEqual(self.field_validation.import_type, self.import_type)
        self.assertEqual(self.field_validation.token, self.token)
        self.assertEqual(self.field_validation.filename_path, self.working_file)
        self.assertEqual(
            self.field_validation.process_request_type,
            self.process_request_type)
        self.assertEqual(self.field_validation.delimiter, self.delimiter)
        self.assertEqual(
            self.field_validation.elastic_hash,
            self.zip_elastic_hash)
        self.assertEqual(self.field_validation.language, self.language)

    def test_parse_dictionary_for_cloud(self):
        file_path = create_file(
            base_path=WORKING_DIR,
            file_name='test_file',
            extension='.csv',
            import_type='machines',
            delimiter=';',
            validity='valid'
        )[1]
        parser = return_import_type_based_on_parser(self.import_type)
        result = self.field_validation.parse_dictionary_for_cloud(
            file_path, parser)

    def test_get_ouput_response(self):
        file_path = create_file(
            base_path=WORKING_DIR,
            file_name='test_file',
            extension='.csv',
            import_type='machines',
            delimiter=';',
            validity='valid'
        )[1]
        parser = return_import_type_based_on_parser(self.import_type)
        result = self.field_validation.get_output_response(
            file_path, parser)

    def test_get_validation_fields__different_delimiters(self):
        delimiters = [',', '|', ';']
        for delimiter in delimiters:
            file_path = create_file(
                base_path=WORKING_DIR,
                file_name='test_file',
                extension='.csv',
                import_type='machines',
                delimiter=delimiter,
                validity='valid'
            )[1]
            self.field_validation.filename_path = file_path
            self.field_validation.delimiter = delimiter

            # call tested method
            with self.assertLogs('application', level='ERROR') as cm:
                result = self.field_validation.get_validation_fields()
                self.assertEqual(
                    {
                    'file': os.path.basename(file_path),
                    'success': True
                    },
                    result
                )

    def test_get_validation_fields__invalid_fields(self):
        validities = ['missing_header', 'missing_field']
        for validity in validities:
            file_path = create_file(
                base_path=WORKING_DIR,
                file_name='test_file',
                extension='.csv',
                import_type='machines',
                delimiter=';',
                validity=validity
            )[1]
            self.field_validation.filename_path = file_path

            with self.assertLogs('application', level='ERROR') as cm:
                # call tested method
                result = self.field_validation.get_validation_fields()
                expected = {
                    'file': os.path.basename(file_path),
                    'success': False
                }
                self.assertEqual(
                    expected,
                    result
                )

    def test_get_validation_fields__machines_all_fields(self):
        file_path = create_file(
            base_path=WORKING_DIR,
            file_name='test_file',
            extension='.csv',
            import_type='machines',
            delimiter=';',
            validity='valid',
            fields='all_fields'
        )[1]
        self.field_validation.import_type = 'MACHINES'
        self.field_validation.filename_path = file_path

        with self.assertLogs('application', level='ERROR') as cm:
            result = self.field_validation.get_validation_fields()
            expected = {
                'file': os.path.basename(file_path),
                'success': True
            }
            self.assertEqual(expected, result)

    def test_get_validation_fields__wrong_header(self):
        file_path = create_file(
            base_path=WORKING_DIR,
            file_name='test_file',
            extension='.csv',
            import_type='machines',
            delimiter=';',
            fields='all_fields',
            validity='wrong_header'
        )[1]
        self.field_validation.filename_path = file_path

        with self.assertLogs('application', level='ERROR') as cm:
            # call tested method
            result = self.field_validation.get_validation_fields()
            self.assertEqual(result, None)

    @patch.object(ProcessLogger, 'create_process_and_cloud_flow_and_main')
    def test_get_validation_fields__empty_required_field(
        self,
        mock_create_process_and_cloud_flow_and_main,
    ):
        file_path = create_file(
            base_path=WORKING_DIR,
            file_name='test_file_empty_required_field',
            extension='.csv',
            import_type='products',
            delimiter=';',
            fields='all_fields',
            validity='empty_required_field'
        )[1]
        self.field_validation.import_type = 'PRODUCTS'
        self.field_validation.filename_path = file_path

        with self.assertLogs('application', level='ERROR') as cm:
            # call tested method
            result = self.field_validation.get_validation_fields()
            self.assertFalse(result['success'])
            self.assertTrue(mock_create_process_and_cloud_flow_and_main.called)

    def test_get_validation_fields__products(self):
        """TODO: made just for quick testing. Needs proper assertion."""
        validities = ['valid', 'missing_header', 'missing_field', 'wrong_header']
        for validity in validities:
            file_path = create_file(
                base_path=WORKING_DIR,
                file_name='test_file',
                extension='.csv',
                import_type='products',
                delimiter=';',
                validity=validity,
                fields='all_fields'
            )[1]
            self.field_validation.import_type = 'PRODUCTS'
            self.field_validation.filename_path = file_path

            try:
                with self.assertLogs('application', level='ERROR') as cm:
                    result = self.field_validation.get_validation_fields()
                    expected = {
                        'file': os.path.basename(file_path),
                        'success': True
                    }
                    self.assertEqual(expected, result)
            except TransportError:
                pass

    def test_get_validation_fields__products_valid_success(self):
        file_path = create_file(
            base_path=WORKING_DIR,
            file_name='test_file',
            extension='.csv',
            import_type='products',
            delimiter=';',
            validity='valid',
            fields='all_fields'
        )[1]
        self.field_validation.import_type = 'PRODUCTS'
        self.field_validation.filename_path = file_path

        try:
            with self.assertLogs('application', level='ERROR') as cm:
                result = self.field_validation.get_validation_fields()
                expected = {
                    'file': os.path.basename(file_path),
                    'success': True
                }
                self.assertEqual(expected, result)
        except TransportError:
            pass

    @patch.object(ElasticCloudLoginFunctions, 'create_cloud_process_flow')
    @patch.object(ProcessLogger, 'create_process_and_cloud_flow_and_main')
    def test_get_validation_fields__invalid_price_field_format(
        self,
        mock_create_process_and_cloud_flow_and_main,
        mock_create_cloud_process_flow
    ):
        content = [('BELVITA CHOCOLAT 50GR X30', 'B448', 1, '1.AAA')]
        file_path = create_file(
            base_path=WORKING_DIR,
            file_name='test_file',
            extension='.csv',
            import_type='products',
            delimiter=';',
            fields='required',
            content=content
        )[1]
        self.field_validation.import_type = 'PRODUCTS'
        self.field_validation.filename_path = file_path

        with self.assertLogs('application', level='ERROR') as cm:
            # call tested method
            result = self.field_validation.get_validation_fields()
            self.assertTrue(mock_create_cloud_process_flow.called)
            self.assertTrue(mock_create_process_and_cloud_flow_and_main.called)
            self.assertEqual({'file': 'test_file.csv', 'success': False}, result)
