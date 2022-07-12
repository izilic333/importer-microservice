from unittest import TestCase
from unittest.mock import patch, Mock

from importer.common.email.send_email import connection, send_email_error_on_file_parse
from importer.common.mixin.validator_import import WORKING_DIR
from importer.common.validators.file_utils import create_file, clear_test_dirs


class TestSendEmail(TestCase):
    def setUp(self):
        clear_test_dirs()
    
    def tearDown(self):
        clear_test_dirs()
    
    @patch('common.email.send_email.connection')
    def NOtest_send_email_error_on_file_parse(self, mock_connection):
        #mock_connection.return_value = Mock()
        mock_connection.sendmail = Mock()
        file_path = create_file(
            base_path=WORKING_DIR,
            file_name='test_file'
        )[1]
        email = 'test@example.com'
        import_type = 6
        date_exp = ''

        #import pdb; pdb.set_trace()

        send_email_error_on_file_parse(
            file_path,
            email,
            import_type,
            date_exp
        )

        self.assertTrue(mock_connection.sendmail.called)
