from common.logging.setup import logger
from unittest.mock import patch
from unittest import TestCase

from common.mixin import enum_errors
from common.mixin.enum_errors import EnumValidationMessage as enum_msg


class TestEnumErrors(TestCase):
    def setUp(self):
        pass

    def test_enum_message_on_specific_language(self):
        key_enum = enum_msg.VALIDATION_SYSTEM_LOG_INFO_INDEX_MESSAGE.value
        language = 'en'

        result = enum_errors.enum_message_on_specific_language(
            key_enum,
            language
        )
        self.assertEqual(result, 'File index {}, message: {}')

    def test_enum_message_on_specific_language__formatted_message(self):
        key_enum = enum_msg.VALIDATION_SYSTEM_LOG_INFO_INDEX_MESSAGE.value
        language = ''
        msg1 = 'test1'
        msg2 = 'test2'

        result = enum_errors.enum_message_on_specific_language(
            key_enum,
            language,
            msg1,
            msg2
        )
        self.assertEqual(result, 'File index test1, message: test2')

    def test_enum_message_on_specific_language__args_none(self):
        key_enum = enum_msg.VALIDATION_SYSTEM_LOG_INFO_INDEX_MESSAGE.value
        language = ''
        arg = None

        result = enum_errors.enum_message_on_specific_language(
            key_enum,
            language,
            arg
        )
        self.assertEqual(result, 'File index {}, message: {}')
