import json
from collections import OrderedDict
from copy import deepcopy
from enum import Enum

# File supported for API
import pytz
from jsonschema import Draft4Validator

ALLOWED_EXTENSIONS = ['.csv', '.xls', '.xlsx', '.zip', '.xml', '.dex', '.rsp']


class MainImportType(Enum):
    cpi_vends = 1
    master_data = 2


class Enum_validation_error(Enum):
    """
    This is message for CLOUD on specific language
    """
    DEVICE_TYPE_ERROR = {
        "en": "Device type must contain 'TE' for device, 'SI' for Sip, 'UN' for Unicum, 'NA' for Nayax, 'CP' for Cpi, 'VC' for Vcore or 'NT' for no audit.",
        "de": "",
        "it": "",
        "fr": ""
    }
    EMAIL_ERROR = {
        "en": "Email must be in the format mail@domain.ext e.g.",
        "de": "",
        "it": "",
        "fr": ""
    }
    ACTION_ERROR = {
        "en": "Action can be only: '0' for create, '1' for update, '2'  for delete or '50' for no status.",
        "de": "",
        "it": "",
        "fr": ""
    }
    PLANOGRAM_ACTION_ERROR = {
        "en": "Action can be only: '0' for create, '1' for update, '2' for delete.",
        "de": "",
        "it": "",
        "fr": ""
    }
    PLANOGRAM_MULTIPLE_PRICE_ERROR = {
        "en": "multiple_pricelists  must be integer and it can have values from 1 to 5",
        "de": "",
        "it": "",
        "fr": ""
    }
    DATE_ERROR = {
        "en": "The installation date must be in the form YYYY-MM-DD e.g. 2001-09-11.",
        "de": "",
        "it": "",
        "fr": ""
    }
    WORK_DAY_ERROR = {
        "en": "Workdays must contain '0' for not open or '1' for open in the sequence as are the days in a week e.g. 1111100 for open every day except during weekend.",
        "de": "",
        "it": "",
        "fr": ""
    }
    WORK_HOURS_ERROR = {
        "en": "Working hours must be in the format 'hh:mm' for single working period or 'hh:mm#hh:mm' for multiple working periods in a day.",
        "de": "",
        "it": "",
        "fr": ""
    }
    BOLL_ERROR = {
        "en": "Alarm fields must be '1' for on or  '0' for off.",
        "de": "",
        "it": "",
        "fr": ""
    }
    DEVICE_STATUS_ERROR = {
        "en": "Device status must be only 'true' or 'false'.",
        "de": "",
        "it": "",
        "fr": ""
    }
    CASHLESS_ERROR = {
        "en": "Alarms should be set up in the format hh:mm. Example: 12:00.",
        "de": "",
        "it": "",
        "fr": ""
    }
    LONGITUDE_LATITUDE_LOCATION_ADDRESS_ERROR = {
        "en": "'location_address' or 'longitude' and 'latitude' are mandatory but missing in the import file.",
        "de": "",
        "it": "",
        "fr": ""
    }

    RECOMMENDED_VISIT = {
        "en": "Recommended visit must be an integer!",
        "de": "",
        "it": "",
        "fr": ""
    }

    URGENT_VISIT = {
        "en": "Urgent visit must be an integer!",
        "de": "",
        "it": "",
        "fr": ""
    }

    METER_READINGS_LIST = {
        "en": "Meter readings list must be in this format A-CASH:123456789.123456#A-TOTB:987654321.654321!",
        "de": "",
        "it": "",
        "fr": ""
    }

    METER_READING_TRACKING = {
        "en": "Meter reading tracking must be 0 or 1 which means False or True",
        "de": "",
        "it": "",
        "fr": ""
    }

    PRICE_ERROR = {
        "en": "'price' field should be decimal value with maximum 4 decimal places.",
        "de": "",
        "it": "",
        "fr": ""
    }
    PLANOGRAM_PRICE_1_ERROR = {
        "en": "price_1 field should be decimal value with maximum 2 decimal places, (max 99999999)",
        "de": "",
        "it": "",
        "fr": ""
    }
    PLANOGRAM_PRICE_2_ERROR = {
        "en": "price_2 field should be decimal value with maximum 2 decimal places, (max 99999999)",
        "de": "",
        "it": "",
        "fr": ""
    }
    PLANOGRAM_PRICE_3_ERROR = {
        "en": "price_3 field should be decimal value with maximum 2 decimal places, (max 99999999)",
        "de": "",
        "it": "",
        "fr": ""
    }
    PLANOGRAM_PRICE_4_ERROR = {
        "en": "price_4 field should be decimal value with maximum 2 decimal places, (max 99999999)",
        "de": "",
        "it": "",
        "fr": ""
    }
    PLANOGRAM_PRICE_5_ERROR = {
        "en": "price_5 field should be decimal value with maximum 2 decimal places, (max 99999999)",
        "de": "",
        "it": "",
        "fr": ""
    }
    TAX_RATE_ERROR = {
        "en": "'tax_rate' field should be decimal value with maximum 4 decimal places.",
        "de": "",
        "it": "",
        "fr": ""
    }

    BLACKLISTED_ERROR = {
        "en": "'blacklisted' field should be integer value of 1 or 0",
        "de": "",
        "it": "",
        "fr": ""
    }

    MAIL_PLANOGRAM_ERROR = {
        "en": "'mail_notification' field should be integer value of 1 or 0",
        "de": "",
        "it": "",
        "fr": ""
    }
    COMPONENT_WARNING_PERCENTAGE = {
        "en": "'component_warning_percentage' field should be positive integer from 0 to 100.",
        "de": "",
        "it": "",
        "fr": ""
    }

    PRODUCT_WARNING_PERCENTAGE = {
        "en": "'product_warning_percentage' field should be positive integer from 0 to 100.",
        "de": "",
        "it": "",
        "fr": ""
    }

    PLANOGRAM_NAME_ERROR_MESSAGE = {
        "en": "'planogram_name' field should be less than or equal to 100",
        "de": "",
        "it": "",
        "fr": ""
    }

    PLANOGRAM_ID_ERROR_MESSAGE = {
        "en": "'planogram_id' field should be less than or equal to 32",
        "de": "",
        "it": "",
        "fr": ""
    }

    PLANOGRAM_PRODUCT_ID_ERROR_MESSAGE = {
        "en": "'product_id' field should be less than or equal to 32",
        "de": "",
        "it": "",
        "fr": ""
    }

    PLANOGRAM_PRODUCT_ROTATION_GROUP_ID_ERROR_MESSAGE = {
        "en": "'product_rotation_group_id' field should be less than or equal to 32",
        "de": "",
        "it": "",
        "fr": ""
    }

    PLANOGRAM_PRODUCT_ROTATION_GROUP_MISSING_ERROR_MESSAGE = {
        "en": "'product_id' or 'product_rotation_group_id' fields are mandatory and should be less than or equal to 32",
        "de": "",
        "it": "",
        "fr": ""
    }

    PLANOGRAM_RECIPE_ID_ERROR_MESSAGE = {
        "en": "'recipe_id' field should be less than or equal to 10",
        "de": "",
        "it": "",
        "fr": ""
    }

    PLANOGRAM_TAGS_ERROR_MESSAGE = {
        "en": "'tags' field should be less than or equal to 255, and separated by comma.",
        "de": "",
        "it": "",
        "fr": ""
    }
    PLANOGRAM_FILL_RATE_ERROR_MESSAGE = {
        "en": "'fill_rate' field must be number 0 - 999999999 and lower than or equal to capacity.",
        "de": "",
        "it": "",
        "fr": ""
    }

    PLANOGRAM_CAPACITY_ERROR_MESSAGE = {
        "en": "'capacity' field must be number 0 - 999999999.",
        "de": "",
        "it": "",
        "fr": ""
    }
    PLANOGRAM_WARNING_FIELD_ERROR_MESSAGE = {
        "en": "'warning' Column warning must be integer from 0 to capacity value",
        "de": "",
        "it": "",
        "fr": ""
    }
    PLANOGRAM_COLUMN_FIELD_ERROR_MESSAGE = {
        "en": "'column_number' field must be number 0 - 32767.",
        "de": "",
        "it": "",
        "fr": ""
    }


def return_language_error(enum, language):
    """

    :param enum: enum message for translate
    :param language: language
    :return: message on specific language
    """
    return enum[language]


def return_all_fields_and_mandatory(input_params):
    """

    :param input_params: json schema validation type
    :return: all fields and mandatory fields from specific import type
    """
    mandatory_fields = input_params['main_fields']
    all_fields = [fl for fl in input_params['all_fields']]
    return all_fields, mandatory_fields


def return_import_type_based_on_parser(import_type, data=None, api_request=True):
    """

    :param import_type: location, machine, machine_type, regions
    :param data: import content, only for planogram import
    :return: json schema validation type
    """
    for i_type in ImportType:
        if import_type == i_type.value.get('id') or import_type == i_type.name:
            # This is new business logic request only for planogram import!
            if import_type == ImportType.PLANOGRAMS.name:
                actual_import_fields = deepcopy(i_type.value['def']['custom_valid_fields'])
                overwrite_import_fields = dict(
                    [(x, y) for x, y in actual_import_fields.items() if not x.startswith('price_')]
                )
                actual_all_fields = deepcopy(i_type.value['def']['all_fields'])
                overwrite_actual_fields = [x for x in actual_all_fields if not x.startswith('price_')]

                multi_price_list = []
                try:
                    for x in data:
                        multi_price = x.get('multiple_pricelists')
                        if not multi_price:
                            multi_price_list.append(1)
                        else:
                            import_multi_price = int(float(multi_price))
                            multi_price_list.append(import_multi_price)
                except Exception as e:
                    return False
                if len(multi_price_list):
                    for x in range(1, max(multi_price_list)+1):
                        price = 'price_'+str(x)
                        overwrite_actual_fields.append(price)
                        overwrite_import_fields.update({price: float})
                i_type.value['def']['custom_valid_fields'] = overwrite_import_fields
                i_type.value['def']['all_fields'] = overwrite_actual_fields

                return i_type.value['def']

            if import_type == ImportType.MACHINES.name and api_request:
                parser_def = deepcopy(i_type.value['def'])
                parser_def['custom_valid_fields'].pop('meter_readings_list')
                parser_def['custom_valid_fields'].pop('meter_reading_tracking')
                return parser_def

            return i_type.value['def']


def find_key_and_custom_message(key_in, language, parser):
    """

    :param key_in: specific field of import type
    :param language: desired language for message
    :param parser: json schema validation type
    :return: message on selected language
    """
    key_find = any('%s' % key_in in x for x in parser['custom_message'])
    if key_find:
        msg = parser['custom_message'][0][key_in].get('message', None)
        return {'success': True, 'message': msg[language]}
    else:

        return {'success': False, 'message': ''}


"""

    REGIONS

"""

regionsParser = {
    "type": "object",
    "properties": {
        "region_name": {
            "type": "string",
            "maxLength": 100,
            "minLength": 1
        },
        "region_id": {
            "type": "string",
            "maxLength": 100,
            "minLength": 1,
        },
        "region_action": {
            "type": "string",
            "pattern": "(^(1)$|^(2)$|^(0)$|^(50)$)",
            "minLength": 1
        },
        "parent_region_id": {
            "type": ["string", "null"],
            "maxLength": 32
        }
    },
    "required": ["region_name", "region_id", "region_action"],
    "main_fields": ["region_name", "region_id", "region_action"],
    "all_fields": ["region_name", "region_id", "region_action", "parent_region_id"],
    "custom_message": [{
        "region_action": {
            "message": Enum_validation_error.ACTION_ERROR.value
        }
    }],
    "custom_valid_fields":{
        "region_name":str,
        "region_id":str,
        "region_action":str,
        "parent_region_id":str
    }
}

"""

    LOCATION

"""

locationParser = {
    "type": "object",
    "required": ["location_name", "location_id", "location_action"],
    "properties": {
        "location_name": {
            "type": "string",
            "maxLength": 100,
            "minLength": 2
        },
        "location_id": {
            "type": "string",
            "maxLength": 100,
            "minLength": 1
        },
        "location_action": {
            "type": "string",
            "pattern": "(^(1)$|^(2)$|^(0)$|^(50)$)"
        },
        "region_id": {
            "type": ["string", "null"],
            "maxLength": 32
        },
        "phone": {
            "type": ["string", "null"]
        },
        "email": {
            "type": ["string", "null"],
            "pattern": r"^([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)?$",
            "maxLength": 100
        },
        "description": {
            "type": ["string", "null"],
            "maxLength": 255,
        },
        "working_days": {
            "type": ["string", "null"],
            "maxLength": 7,
            "pattern": r"^([0|1]{7})?$",
        },
        "working_hours": {
            "type": ["string", "null"],
            "maxLength": 98,
            "pattern": r"(^(([01]\d|2[0-3]):([0-5]\d)|24:00)-(([01]\d|2[0-3]):([0-5]\d)|24:00){1}$)|(^(([01]\d|2[0-3]):([0-5]\d)|24:00)-(([01]\d|2[0-3]):([0-5]\d)|24:00){1})(\#(([01]\d|2[0-3]):([0-5]\d)|24:00)-(([01]\d|2[0-3]):([0-5]\d)|24:00))*?$",

        },
    },

    "main_fields": ["location_name", "location_id", "location_action"],
    "all_fields": [
        "location_name", "location_id", "location_action",
        "location_address", "region_id", "longitude", "latitude", "phone", "email", "description",
        "working_days", "working_hours"
    ],
    'custom_message': [{
        "working_days": {
            "message": Enum_validation_error.WORK_DAY_ERROR.value
        },
        "working_hours": {
            "message": Enum_validation_error.WORK_HOURS_ERROR.value
        },
        "email": {
            "message": Enum_validation_error.EMAIL_ERROR.value
        },
        "location_action": {
            "message": Enum_validation_error.ACTION_ERROR.value
        },
        "latitude": {
            "message": Enum_validation_error.LONGITUDE_LATITUDE_LOCATION_ADDRESS_ERROR.value
        },
        "longitude": {
            "message": Enum_validation_error.LONGITUDE_LATITUDE_LOCATION_ADDRESS_ERROR.value
        },
        "location_address": {
            "message": Enum_validation_error.LONGITUDE_LATITUDE_LOCATION_ADDRESS_ERROR.value
        },
        "anyOf":  {
            "message": Enum_validation_error.LONGITUDE_LATITUDE_LOCATION_ADDRESS_ERROR.value
        }
    }
    ],

    "custom_valid_fields":{
        "location_name":str,
        "location_id":str,
        "location_action":str,
        "region_id":str,
        "location_address":str,
        "longitude":float,
        "latitude":float,
        "phone":str,
        "email":str,
        "description":str,
        "working_days":str,
        "working_hours":str
    }
}

"""

    MACHINE TYPE

"""
machineTypeParser = {
    "type": "object",
    "properties": {
        "machine_type_name": {
            "maxLength": 100,
            "type": "string",
            "minLength": 1
        },
        "machine_type_id": {
            "maxLength": 32,
            "type": "string",
            "minLength": 1
        },
        "machine_type_action": {
            "type": "string",
            "pattern": "(^(1)$|^(2)$|^(0)$|^(50)$)"
        }
    },
    "required": ["machine_type_name", "machine_type_id", "machine_type_action"],
    "main_fields": ["machine_type_name", "machine_type_id", "machine_type_action"],
    "all_fields": ["machine_type_name", "machine_type_id", "machine_type_action"],
    "custom_message": [{}],
    "custom_valid_fields":{
        "machine_type_name":str,
        "machine_type_id":str,
        "machine_type_action":str
    }
}

"""

    CLIENTS

"""

clientParser = {
    "type": "object",
    "properties": {
        "client_name": {
            "maxLength": 100,
            "type": "string"
        },
        "client_id": {
            "maxLength": 32,
            "type": "string"
        },
        "parent_client_id": {
            "maxLength": 32,
            "type": ["string", "null"]
        },
        "client_type_id": {
            "maxLength": 32,
            "type": ["string", "null"]
        },
        "client_action": {
            "type": "string",
            "pattern": "(^(1)$|^(2)$|^(0)$|^(50)$)",
            "minLength": 1
        }
    },
    "custom_valid_fields": {
        "client_name": str,
        "client_id": str,
        "parent_client_id": str,
        "client_type_id": str,
        "client_action": str,
    },
    "required": ["client_name", "client_id", "client_action"],
    "main_fields": ["client_name", "client_id", "client_action"],
    "all_fields": ["client_name", "client_id", "parent_client_id", "client_type_id",  "client_action"],
    "custom_message": [{
        "client_action": {
            "message": Enum_validation_error.ACTION_ERROR.value
        }
    }]
}




"""

    MACHINES

"""

machineParser = {
    "type": "object",
    "properties": {
        "machine_name": {
            "maxLength": 100,
            "type": "string",
            "minLength": 1
        },
        "machine_id": {
            "maxLength": 32,
            "type": "string",
            "minLength": 1
        },
        "machine_action": {
            "type": "string",
            "pattern": r"(^(1)$|^(2)$|^(0)$|^(50)$)"
        },
        "machine_location_id": {
            "maxLength": 32,
            "type": "string",
            "minLength": 1
        },
        "machine_type_id": {
            "maxLength": 32,
            "type": ["string", "null"],
            "minLength": 1
        },
        "client_id": {
            "maxLength": 100,
            "type": ["string", "null"],
        },
        "cluster_id": {
            "maxLength": 32,
            "type": ["string", "null"],
        },
        "installation_date": {
            "type": ["string","null"],
            "maxLength": 10,
            "pattern": r"^\d{4}\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01])",
        },
        "model_number": {
            "maxLength": 20,
            "type": ["string", "null"],
        },
        "tags": {
            "maxLength": 30,
            "type": ["string", "null"],
        },
        "description": {
            "maxLength": 255,
            "type": ["string", "null"],
        },
        "recommended_visit": {
            "maxLength": 5,
            "pattern": r"(^[0-9]+$)|\W+",
            "type": ["string", "null"],
        },
        "urgent_visit": {
            "maxLength": 5,
            "pattern": r"(^[0-9]+$)",
            "type": ["string", "null"],
        },
        "raise_event_on_undefined_column": {
            "type": ["string", "null"],
            "pattern": r"(^(1)$|^(0)$)"
        },
        "routing": {
            "type": ["string", "null"],
            "pattern": r"(^(1)$|^(0)$)"
        },
        "stock_tracking": {
            "type": ["string","null"],
            "pattern": r"(^(1)$|^(0)$)"
        },
        "events_tracking": {
            "type": ["string", "null"],
            "pattern": r"(^(1)$|^(0)$)"
        },
        "no_cash_alarm": {
            "maxLength": 8,
            "pattern": r"^([0-9]+:[0-5][0-9])?$",
            "type": ["string", "null"],

        },
        "no_cashless_1_alarm": {
            "maxLength": 8,
            "pattern": r"^([0-9]+:[0-5][0-9])?$",
            "type": ["string", "null"]
        },
        "no_cashless_2_alarm": {
            "maxLength": 8,
            "pattern": r"^([0-9]+:[0-5][0-9])?$",
            "type": ["string", "null"],

        },
        "no_cashless_3_alarm": {
            "maxLength": 8,
            "pattern": r"^([0-9]+:[0-5][0-9])?$",
            "type": ["string", "null"],

        },
        "no_sales_alarm": {
            "maxLength": 8,
            "pattern": r"^([0-9]+:[0-5][0-9])?$",
            "type": ["string", "null"],
        },
        "meter_readings_list": {
            "maxLength": 1024,
            # A-CASH:3422.001#A-CASH:1000.000
            "pattern": r"^([\w-]{1,6}:\d{1,9}\.\d{0,6}#{1}){0,}([\w-]{1,6}:\d{1,9}\.\d{0,6})$",
            "type": ["string", "null"]
        },
        "meter_reading_tracking": {
            "maxLength": 1,
            # 0/1
            "pattern": r"^[01]{1}$",
            "type": ["string", "null"]
        },
        "location_warehouse_id": {
            "maxLength": 32,
            "type": ["string", "null"],
            "minLength": 1
        },
        "machine_category_id": {
            "maxLength": 32,
            "type": ["string", "null"],
            "minLength": 1
        }
    },
    "custom_valid_fields":{
        "machine_name":str,
        "machine_id":str,
        "machine_action":str,
        "machine_location_id":str,
        "machine_type_id":str,
        "client_id":str,
        "cluster_id":str,
        "installation_date":str,
        "model_number":str,
        "tags":str,
        "description":str,
        "recommended_visit":int,
        "urgent_visit":int,
        "raise_event_on_undefined_column": str,
        "routing":int,
        "stock_tracking":str,
        "events_tracking":str,
        "no_cash_alarm":str,
        "no_cashless_1_alarm":str,
        "no_cashless_2_alarm":str,
        "no_cashless_3_alarm":str,
        "no_sales_alarm":str,
        "meter_readings_list":str,
        "meter_reading_tracking":str,
        "location_warehouse_id":str,
        "machine_category_id":str
    },
    "required": ["machine_name", "machine_id", "machine_action", "machine_location_id"],
    "main_fields": [
        "machine_name", "machine_id", "machine_action", "machine_location_id",
        "machine_type_id"
    ],
    "all_fields": [
        "machine_name", "machine_id", "machine_action", "machine_location_id",
        "machine_type_id", "client_id", "cluster_id", "installation_date",
        "model_number", "tags", "description", "recommended_visit",
        "urgent_visit", "raise_event_on_undefined_column", "routing", "stock_tracking", "events_tracking",
        "no_cash_alarm", "no_cashless_1_alarm", "no_cashless_2_alarm", "no_cashless_3_alarm",
        "no_sales_alarm", "meter_readings_list", "meter_reading_tracking", "location_warehouse_id",
        "machine_category_id"
    ],
    "custom_message": [{
        "no_sales_alarm": {
            "message": Enum_validation_error.CASHLESS_ERROR.value
        },
        "no_cashless_3_alarm": {
            "message": Enum_validation_error.CASHLESS_ERROR.value
        },
        "no_cashless_2_alarm": {
            "message": Enum_validation_error.CASHLESS_ERROR.value
        },
        "no_cashless_1_alarm": {
            "message": Enum_validation_error.CASHLESS_ERROR.value
        },
        "no_cash_alarm": {
            "message": Enum_validation_error.CASHLESS_ERROR.value
        },
        "raise_event_on_undefined_column": {
            "message": Enum_validation_error.BOLL_ERROR.value
        },
        "events_tracking": {
            "message": Enum_validation_error.BOLL_ERROR.value
        },
        "stock_tracking": {
            "message": Enum_validation_error.BOLL_ERROR.value
        },
        "routing": {
            "message": Enum_validation_error.BOLL_ERROR.value,
        },
        "installation_date": {
            "message": Enum_validation_error.DATE_ERROR.value
        },
        "machine_action": {
            "message": Enum_validation_error.ACTION_ERROR.value
        },
        "recommended_visit": {
            "message": Enum_validation_error.RECOMMENDED_VISIT.value
        },
        "urgent_visit": {
            "message": Enum_validation_error.URGENT_VISIT.value
        },
        "meter_readings_list": {
            "message": Enum_validation_error.METER_READINGS_LIST.value
        },
        "meter_reading_tracking": {
            "message": Enum_validation_error.METER_READING_TRACKING.value
        },

    }]

}


"""

    PACKINGS

"""

packingParser = {
    "type": "object",
    "properties": {
        "packing_id": {
            "maxLength": 32,
            "type": "string"
        },
        "packing_name_id": {
            "maxLength": 32,
            "type": "string"
        },
        "product_id": {
            "maxLength": 32,
            "type": "string"
        },
        "barcode": {
            "maxLength": 50,
            "type": ["string", "null"]
        },
        "default": {
            "type": "string",
            "pattern": r"(^(1)$|^(0)$)"
        },
        "quantity": {
            'type': 'string',
            "maxLength": 10,
            "pattern": r"(^[0-9]+$)|\W+",
        },
        "packing_action": {
            "type": "string",
            "pattern": "(^(1)$|^(2)$|^(0)$|^(50)$)",
            "minLength": 1
        }
    },
    "custom_valid_fields": {
        "packing_id": str,
        "packing_name_id": str,
        "product_id": str,
        "barcode": str,
        "quantity": int,
        "default": int,
        "packing_action": str,
    },
    "required": ["packing_id", "packing_name_id", "product_id", "default", "quantity", "packing_action"],
    "main_fields": ["packing_id", "packing_name_id", "product_id", "default", "quantity", "packing_action"],
    "all_fields": ["packing_id", "packing_name_id", "product_id", "default", "quantity", "packing_action", "barcode"],
    "custom_message": [{
        "packing_action": {
            "message": Enum_validation_error.ACTION_ERROR.value
        }
    }]
}

"""
PRODUCTS
"""
productParser = {
    'type': 'object',
    'properties': {
        "product_name": {
            'type': 'string',
            'maxLength': 100,
            "minLength": 1
        },
        "product_id": {
            'type': 'string',
            'maxLength': 32,
            "minLength": 1
        },
        "product_action": {
            'type': 'string',
            'pattern': r'(^(1)$|^(2)$|^(0)$|^(50)$)'
        },
        "price": {
            'type': 'string',
            'maxLength': 12,
            'pattern': r'(^\d+(?:\.\d{1,4})?$)'
        },
        "tax_rate": {
            'type': ['string', 'null'],
            'maxLength': 12,
            'pattern': r'(^\d+(?:\.\d{1,4})?$)'
        },
        "product_category_id": {
            'type': ['string', 'null'],
            'maxLength': 30
        },
        "default_barcode": {
            'type': ['string', 'null'],
            'maxLength': 50
        },
        "barcode1": {
            'type': ['string', 'null'],
            'maxLength': 50
        },
        "barcode2": {
            'type': ['string', 'null'],
            'maxLength': 50
        },
        "barcode3": {
            'type': ['string', 'null'],
            'maxLength': 50
        },
        "barcode4": {
            'type': ['string', 'null'],
            'maxLength': 50
        },
        "weight": {
            'type': ['string', 'null'],
            'maxLength': 17
        },
        "use_packing": {
            "type": ["string", "null"],
            "pattern": r"(^(1)$|^(0)$)"
        },
        "description": {
            'type': ['string', 'null'],
            'maxLength': 255
        },
        "short_shelf_life": {
            'type': ['string', 'null'],
            'maxLength': 10
        },
        "age_verification": {
            'type': ['string', 'null'],
            'maxLength': 10
        },
        "capacity": {
            'type': ['string', 'null']
        },
        "minimum_route_pickup": {
            'type': ['string', 'null']
        },
        'blacklisted': {
            'type': ['string', 'null'],
            'pattern': r'(^(1)$|^(0)$)'
        }
    },
    'custom_valid_fields': {
        'product_name': str,
        'product_id': str,
        'product_action': int,
        'price': float,
        'tax_rate': float,
        'product_category_id': int,
        'default_barcode': str,
        'barcode1': str,
        'barcode2': str,
        'barcode3': str,
        'barcode4': str,
        'weight': float,
        'use_packing': int,
        'description': str,
        'short_shelf_life': int,
        'age_verification': int,
        'capacity': int,
        'minimum_route_pickup': int,
        'blacklisted': int
    },
    'required': [
        'product_name', 'product_id',
        'product_action', 'price'
    ],
    'main_fields': [
        'product_name', 'product_id',
        'product_action', 'price'
    ],
    'all_fields': [
        'product_name', 'product_id', 'product_action', 'price',
        'tax_rate', 'product_category_id', 'default_barcode',
        'weight', 'use_packing',
        'description', 'short_shelf_life', 'age_verification',
        'capacity', 'minimum_route_pickup', 'blacklisted',
        'barcode1', 'barcode2', 'barcode3', 'barcode4'
    ],
    'custom_message': [{
        'product_action': {
            'message': Enum_validation_error.ACTION_ERROR.value
        },
        'price': {
            'message': Enum_validation_error.PRICE_ERROR.value
        },
        'tax_rate': {
            'message': Enum_validation_error.TAX_RATE_ERROR.value
        },
        'blacklisted': {
            'message': Enum_validation_error.BLACKLISTED_ERROR.value
        }
    }]
}
"""
PLANOGRAMS
"""

PlanogramParser = {
    'type': 'object',
    'properties': {
        "planogram_name": {
            'type': 'string',
            'maxLength': 100,
            "minLength": 1
        },
        "planogram_id": {
            'type': 'string',
            'maxLength': 32,
            "minLength": 1
        },
        "planogram_action": {
            'type': 'string',
            'pattern': r'(^(1)$|^(2)$|^(0)$)'
        },
        "product_warning_percentage": {
            'type': ['string', 'null'],
            'maxLength': 12,
            'pattern': r'(^(100(?:\.0)?|0(?:\.\d\d)?|\d?\d(?:\.\d)?)$)'
        },
        "component_warning_percentage": {
            'type': ['string', 'null'],
            'maxLength': 12,
            'pattern': r'(^(100(?:\.0)?|0(?:\.\d\d)?|\d?\d(?:\.\d)?)$)'
        },
        "mail_notification": {
            'type': ['string', 'null'],
            'pattern': r'(^(1)$|^(0)$)'
        },
        "column_number": {
            'type': 'string',
            'maxLength': 9,
            'pattern': r'(^(?:\d\d{0,4})|0)$'
        },
        "recipe_id": {
            'type': ['string', 'null'],
            'maxLength': 30
        },
        "tags": {
            'type': ['string', 'null'],
            'maxLength': 255
        },
        "capacity": {
            'type': 'string',
            'maxLength': 9,
            'pattern': r'(^(?:\d\d{0,8})|0)$'
        },
        "warning": {
            'type': ['string', 'null'],
            'maxLength': 9,
            'pattern': r'(^(?:\d\d{0,8})|0)$'
        },
        'fill_rate': {
            'type': 'string',
            'maxLength': 9,
            'pattern': r'(^(?:\d\d{0,8})|0)$'
        },
        'price_1': {
            'type': 'string',
            'maxLength': 9,
            'pattern': r'(^\d{0,8}(?:\.\d{1,2})?$)'
        },
        'price_2': {
            'type': ['string', 'null'],
            'maxLength': 9,
            'pattern': r'(^\d{0,8}(?:\.\d{1,2})?$)'
        },
        'price_3': {
            'type': ['string', 'null'],
            'maxLength': 9,
            'pattern': r'(^\d{0,8}(?:\.\d{1,2})?$)'
        },
        'price_4': {
            'type': ['string', 'null'],
            'maxLength': 9,
            'pattern': r'(^\d{0,8}(?:\.\d{1,2})?$)'
        },
        'price_5': {
            'type': ['string', 'null'],
            'maxLength': 9,
            'pattern': r'(^\d{0,8}(?:\.\d{1,2})?$)'
        },
        'multiple_pricelists': {
            'type': ['string', 'null'],
            'pattern': r'(^(1)$|^(2)$|^(3)$|^(4)$|^(5)$)'
        },
        "minimum_route_pickup": {
            'type': ['string', 'null']
        },

    },
    'custom_valid_fields': {
        'planogram_name': str,
        'planogram_id': str,
        'product_id': str,
        'product_rotation_group_id': str,
        'planogram_action': int,
        'multiple_pricelists': int,
        'product_warning_percentage': int,
        'component_warning_percentage': int,
        'mail_notification': str,
        'column_number': int,
        'recipe_id': str,
        'tags': str,
        'capacity': int,
        'warning': int,
        'fill_rate': int,
        'price_1': float,
        'minimum_route_pickup': int,

    },
    'required': [
        'planogram_name', 'planogram_action', 'column_number', 'capacity', 'fill_rate', 'price_1', 'planogram_id'
    ],
    'main_fields': [
        'planogram_name', 'planogram_action', 'column_number', 'capacity', 'fill_rate', 'price_1', 'planogram_id'
    ],
    'all_fields': [
        'planogram_name', 'planogram_id', 'planogram_action', 'product_warning_percentage',
        'component_warning_percentage', 'mail_notification', 'column_number', 'product_id',
        'product_rotation_group_id','recipe_id', 'tags',
        'capacity', 'warning', 'fill_rate', 'price_1', 'multiple_pricelists', 'minimum_route_pickup'
    ],
    'custom_message': [{
        'planogram_action': {
            'message': Enum_validation_error.PLANOGRAM_ACTION_ERROR.value
        },
        'multiple_pricelists': {
            'message': Enum_validation_error.PLANOGRAM_MULTIPLE_PRICE_ERROR.value
        },
        'price_1': {
            'message': Enum_validation_error.PLANOGRAM_PRICE_1_ERROR.value
        },
        'price_2': {
            'message': Enum_validation_error.PLANOGRAM_PRICE_2_ERROR.value
        },
        'price_3': {
            'message': Enum_validation_error.PLANOGRAM_PRICE_3_ERROR.value
        },
        'price_4': {
            'message': Enum_validation_error.PLANOGRAM_PRICE_4_ERROR.value
        },
        'price_5': {
            'message': Enum_validation_error.PLANOGRAM_PRICE_5_ERROR.value
        },
        'mail_notification': {
            'message': Enum_validation_error.MAIL_PLANOGRAM_ERROR.value
        },
        'component_warning_percentage': {
            'message': Enum_validation_error.COMPONENT_WARNING_PERCENTAGE.value
        },
        'product_warning_percentage': {
            'message': Enum_validation_error.PRODUCT_WARNING_PERCENTAGE.value
        },
        'planogram_name': {
            'message': Enum_validation_error.PLANOGRAM_NAME_ERROR_MESSAGE.value
        },
        'planogram_id': {
            'message': Enum_validation_error.PLANOGRAM_ID_ERROR_MESSAGE.value
        },
        'recipe_id': {
            'message': Enum_validation_error.PLANOGRAM_RECIPE_ID_ERROR_MESSAGE.value
        },
        'tags': {
            'message': Enum_validation_error.PLANOGRAM_TAGS_ERROR_MESSAGE.value
        },
        'fill_rate': {
            'message': Enum_validation_error.PLANOGRAM_FILL_RATE_ERROR_MESSAGE.value
        },
        'capacity': {
            'message': Enum_validation_error.PLANOGRAM_CAPACITY_ERROR_MESSAGE.value
        },
        'warning': {
            'message': Enum_validation_error.PLANOGRAM_WARNING_FIELD_ERROR_MESSAGE.value
        },
        'column_number': {
            'message': Enum_validation_error.PLANOGRAM_COLUMN_FIELD_ERROR_MESSAGE.value
        },
        'product_id': {
            'message': Enum_validation_error.PLANOGRAM_PRODUCT_ID_ERROR_MESSAGE.value
        },
        'product_rotation_group_id': {
            'message': Enum_validation_error.PLANOGRAM_PRODUCT_ROTATION_GROUP_ID_ERROR_MESSAGE.value
        },
        'anyOf': {
             'message': Enum_validation_error.PLANOGRAM_PRODUCT_ROTATION_GROUP_MISSING_ERROR_MESSAGE.value
        },
    }],
    "anyOf":
        [{
            "type": "object",
            "required": ["product_id"],
            "properties":
            {
                "product_id":
                {
                  "type": "string",
                  "maxLength": 32,
                }
            }
        },
        {
            "type": "object",
            "required": ["product_rotation_group_id"],
            "properties":
            {
                "product_rotation_group_id":
                {
                  "type": "string",
                  "maxLength": 32
                }
            }

        }]
}


"""
VENDS : CPI
"""

cpiParser = {
    'type': 'object',
    'properties': {
        "column": {
            'type': 'string',
            'maxLength': 10,
            "minLength": 1
        },
        "column_index": {
            'type': 'string',
            'maxLength': 32,
            "minLength": 1
        },
        "value": {
            'type': 'string',
        },
        "product": {
            'type': 'string',
            'maxLength': 12,
        },
        "timestamp": {
            'type': ['string', 'null'],
            'maxLength': 12,
            'pattern': r'()'
        },
        "device_pid": {
            'type': 'string',
            'maxLength': 30
        }
    },
    'custom_valid_fields': {
        'column': str,
        'column_index': str,
        'value': int,
        'product': float,
        'timestamp': float,
        'device_pid': int,
    },
    'required': [
        'timestamp', 'column',
        'value', 'device_pid'
    ],
    'main_fields': [
        'timestamp', 'column',
        'value', 'device_pid'
    ],
    'all_fields': [
        'column', 'column_index', 'value', 'product',
        'timestamp', 'device_pid'
    ],
    'custom_message': [{
        'timestamp': {
            'message': Enum_validation_error
        },
        'column': {
            'message': Enum_validation_error
        },
        'value': {
            'message': Enum_validation_error
        },
        'device_pid': {
            'message': Enum_validation_error
        }
    }]
}

auditParser = {
    "type": "object",
    "properties": {
        "action": {
            ("required", True),
            ("type", "number"),
            ("pattern", r"/^(0|1|2|50)$/"),
        },
        "device_pid": {
            ("required", True),
            ("maxLength", 255),
            ("type", "string")
        },
        "device_status": {
            ("required", True),
            ("pattern", r"^(true|false)$"),
            ("type", "string"),
        },
        "device_type": {
            ("required", True),
            ("type", "string"),
            ("pattern", r"/^(TE|SI|UN|NA|CP|VC|NT)$/"),
        },
        "machine_id": {
            ("required", True),
            ("maxLength", 32),
            ("type", "string")
        }
    },
    "required": ["action", "device_pid", "device_status", "device_type", "machine_id"]
}

"""
USERS
"""

UserParser = {
    'type': 'object',
    'properties': {
        "user_action": {
            "type": 'string',
            "enum": ['0', '1', '2', '50'],
        },
        "user_id": {
            'type': 'string',
            'maxLength': 32,
            "minLength": 1
        },
        "first_name": {
            'type': 'string',
            'maxLength': 30,
            "minLength": 1
        },
        "last_name": {
            'type': 'string',
            'maxLength': 30,
            "minLength": 1
        },
        "email": {
            "type": "string",
            "pattern": r"^([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)?$",
            "maxLength": 100
        },
        "user_role": {
            'type': 'string',
            'enum': ['Filler', 'Guest', 'CustomerSupport', 'Service',
                     'ServiceManager', 'CategoryManager', 'Supervisor',
                     'CompanyAdministrator', 'CompanyManager']
        },
        "timezone": {
            'type': ['string', 'null'],
            'enum': list(pytz.common_timezones) + [None, ],
        },
        "phone": {
            'type': ['string', 'null'],
            'maxLength': 100
        },
        "language": {
            'type': ['string', 'null'],
            'enum': ['en', 'de', 'es', 'it', 'hr', 'sr', 'fr', 'ru', 'hu',
                     'el', 'pl', 'pt', None]
        },
        "service_email_notification": {
            'type': ['string', 'null'],
            'enum': ['true', 'false', None]
        },
        "service_sms_notification": {
            'type': ['string', 'null'],
            'enum': ['true', 'false', None]
        },
        "service_staff_mobile_app": {
            'type': ['string', 'null'],
            'enum': ['true', 'false', None]
        },
        "service_staff_mobile_technical_view": {
            'type': ['string', 'null'],
            'enum': ['true', 'false', None]
        },
        "assign_filling_route": {
            'type': ['string', 'null'],
            'enum': ['true', 'false', None]
        },
        "assign_event": {
            'type': ['string', 'null'],
            'enum': ['true', 'false', None]
        }
    },
    'custom_valid_fields': {
        'user_action': int,
        'user_id': str,
        'first_name': str,
        'last_name': str,
        'email': str,
        'user_role': str,
        'timezone': str,
        'phone': str,
        'language': str,
        'service_email_notification': str,
        'service_sms_notification': str,
        'service_staff_mobile_app': str,
        'service_staff_mobile_technical_view': str,
        'assign_filling_route': str,
        'assign_event': str
    },
    'required': [
        'user_action', 'user_id', 'first_name', 'last_name', 'email', 'user_role'
    ],
    'main_fields': [
        'user_action', 'user_id', 'first_name', 'last_name', 'email', 'user_role'
    ],
    'all_fields': [
        'user_action', 'user_id', 'first_name', 'last_name', 'email', 'user_role',
        'timezone', 'phone', 'language', 'service_email_notification',
        'service_sms_notification', 'service_staff_mobile_app',
        'service_staff_mobile_technical_view', 'assign_filling_route',
        'assign_event'
    ],
    'custom_message': [{
        "email": {
            "message": Enum_validation_error.EMAIL_ERROR.value,
        }}
]
}


class ImportType(Enum):
    VENDS = {
        'id': 1,
        'def': '',
        'active': False,
        'show_in_history': False,
        'order': 0,
        'example_file': '',
        'capitalised_name': 'Vend',
        'vend': False
    }
    EVENTS = {
        'id': 2,
        'def': None,
        'active': False,
        'show_in_history': False,
        'order': 0,
        'example_file': '',
        'capitalised_name': 'Event',
        'vend': False
    }
    MACHINES = {
        'id': 3,
        'def': machineParser,
        'active': True,
        'show_in_history': True,
        'order': 5,
        'example_file': 'machine_import_file.csv',
        'capitalised_name': 'Machine',
        'vend': False
    }
    LOCATIONS = {
        'id': 4,
        'def': locationParser,
        'active': True,
        'show_in_history': True,
        'order': 2,
        'example_file': 'location_import_file.csv',
        'capitalised_name': 'Location',
        'vend': False
    }
    REGIONS = {
        'id': 5,
        'def': regionsParser,
        'active': True,
        'show_in_history': True,
        'order': 1,
        'example_file': 'region_import_file.csv',
        'capitalised_name': 'Region',
        'vend': False
    }
    PRODUCTS = {
        'id': 6,
        'def': productParser,
        'active': True,
        'show_in_history': True,
        'order': 7,
        'example_file': 'product_import_file.csv',
        'capitalised_name': 'Product',
        'vend': False
    }
    COMPONENTS = {
        'id': 7,
        'def': None,
        'active': False,
        'show_in_history': False,
        'order': 0,
        'example_file': '',
        'capitalised_name': 'Component',
        'vend': False
    }
    RECEPTURE = {
        'id': 8,
        'def': None,
        'active': False,
        'show_in_history': False,
        'order': 0,
        'example_file': '',
        'capitalised_name': 'Recepture',
        'vend': False
    }
    PLANOGRAMS = {
        'id': 9,
        'def': PlanogramParser,
        'active': True,
        'show_in_history': True,
        'order': 0,
        'example_file': 'planogram_import_file.csv',
        'capitalised_name': 'Planogram',
        'vend': False
    }
    CLIENTS = {
        'id': 10,
        'def': clientParser,
        'active': True,
        'order': 4,
        'example_file': 'client_import_file.csv',
        'capitalised_name': 'Client',
        'show_in_history': False,
        'vend': False
    }
    USERS = {
        'id': 11,
        'def': UserParser,
        'active': True,
        'show_in_history': True,
        'order': 0,
        'example_file': 'user_import_file.csv',
        'capitalised_name': 'User',
        'vend': False
    }
    CLUSTERS = {
        'id': 12,
        'def': None,
        'active': False,
        'show_in_history': False,
        'order': 0,
        'example_file': '',
        'capitalised_name': 'Cluster',
        'vend': False
    }
    MACHINE_TYPES = {
        'id': 13,
        'def': machineTypeParser,
        'active': True,
        'show_in_history': True,
        'order': 3,
        'example_file': 'machine_type_import_file.csv',
        'capitalised_name': 'Machine_Type',
        'vend': False

    }
    AUDIT_TYPE = {
        'id': 14,
        'def': auditParser,
        'active': False,
        'show_in_history': False,
        'order': 6,
        'example_file': '',
        'capitalised_name': 'Audit_Type',
        'vend': False
    }

    CPI_VENDS = {
        'id': 15,
        'def': cpiParser,
        'active': False,
        'show_in_history': True,
        'order': 0,
        'example_file': '',
        'capitalised_name': 'CPI_Vend',
        'vend': True
    }

    VENDON_VENDS = {
        'id': 18,
        'active': False,
        'show_in_history': True,
        'order': 0,
        'example_file': '',
        'capitalised_name': 'Vendon_Vend',
        'vend': True
    }

    DEX_VENDS = {
        'id': 19,
        'active': False,
        'show_in_history': True,
        'order': 0,
        'example_file': '',
        'capitalised_name': 'DEX_Vend',
        'vend': True
    }

    PACKINGS = {
        'id': 20,
        'def': packingParser,
        'active': True,
        'show_in_history': True,
        'order': 0,
        'example_file': 'packing_import_file.csv',
        'capitalised_name': 'Packing',
        'vend': False
    }


def return_file_example(type_name):
    """

    :param type_name: validation name
    :return: correct validation example file
    """
    for i_type in ImportType:
        if i_type.name == type_name.upper():
            return i_type.value['example_file']
    return None


def get_import_type_by_name(type_name):
    """

    :param type_name: validation name
    :return: name of validation example
    """
    for i_type in ImportType:
        if i_type.name == type_name.upper():
            return i_type
    return None

def get_import_type_by_id(id):
    """

    :param id: import id
    :return: import type
    """
    for i_type in ImportType:
        if i_type.value['id'] == id:
            return i_type
    return None


def get_parser_def_field(import_type, field_name):
    for x in ImportType:
        if x.name == import_type or x.value['id'] == import_type:
            result = x.value.get('def')
            return result['properties'].get(field_name) if result else None


class ImportAction(Enum):
    """
    This is basic type of operation on CLOUD
    """
    CREATE = 0
    UPDATE = 1
    DELETE = 2
    UNKNOWN = 50


def return_action_type(act_type):
    """

    :param act_type: selected import action type
    :return: name of selected import type action (CREATE, UPDATE etc..)
    """
    for i_type in ImportAction:
        if i_type.name == act_type.upper():
            return i_type.name
    return None


def return_active_vend_import_type(import_type):
    for x in ImportType:
        if type(x) is not int and import_type.upper() == x.name:
            enum_info = {
                'vend_status': x.value['vend'],
                'active': x.value['active'],
                'import_name': x.name,
                'id': x.value['id']
            }
            return enum_info
        elif type(x) is int and int(import_type) == int(x.value):
            enum_info = {
                'vend_status': x.value['vend'],
                'active': x.value['active'],
                'import_name': x.name,
                'id': x.value['id']
            }
            return enum_info


def return_import_type_status(import_type):
    """

    :param import_type: validation import type
    :return: status of that import type (active, etc ..)
    """
    for x in ImportType:
        if type(x) is not int and import_type.upper() == x.name:
            return x.value['active']
        elif type(x) is int and int(import_type) == int(x.value):
            return x.value['active']

    return False


def return_import_type_status_and_import(import_type):
    """

    :param import_type: location, machine, machine_type, regions
    :return: status and valid import type name
    """

    for x in ImportType:
        if type(import_type) is not int and import_type.upper() == x.name:
            if x.value['active']:
                return {'success': True, 'response': x.name}
            else:
                return {'success': False, 'response': 'Import type not activated.'}
        elif type(import_type) is int and int(import_type) == int(x.value['id']):
            if x.value['active']:
                return {'success': True, 'response': x.name}
            else:
                return {'success': False, 'response': 'Import type not activated.'}

    return {'success': False, 'response': 'Import type not found: %s' % import_type}


def return_import_type_id(import_type):
    """

    :param import_type: location, machine, machine_type, regions
    :return: id of specific import type
    """
    for i_type in ImportType:
        if type(import_type) is not int and import_type.upper() == i_type.name:
            return i_type.value['id']
        elif type(import_type) is int and import_type == i_type.value['id']:
            return import_type


def return_import_type_name(import_type):
    """

    :param import_type: location, machine, machine_type, regions
    :return: valid name of specific import type
    """
    for x in ImportType:
        if type(import_type) is int and int(import_type) == int(x.value['id']):
            return x.name
        elif type(import_type) is str and import_type == x.name:
            return x.name


def return_active_type(include_history_only=False):
    """

    :return: all active import type
    """
    active_type = []

    for x in ImportType:
        if x.value['active'] or (include_history_only and x.value['show_in_history']):
            active_type.append(
                {
                    'id': x.value['id'],
                    'name': x.name
                }
            )
    return active_type


def return_import_type_id_custom_validation(import_type):
    """

    :param import_type: location, machine, machine_type, regions, vends
    :return: id of import type
    """
    for x in ImportType:
        if type(import_type) is not int and import_type.upper() == x.name:
            return x.value['id']
        elif type(import_type) is int and import_type == x.value['id']:
            return x.value['id']

def return_import_object_type(imp_type):
    """

    :param imp_type: location, machine, machine_type, regions
    :return: object of specific import type ( 'id': 3, 'def': machineParser, 'active': True, etc ..)
    """
    for x in ImportType:
        if type(imp_type) is int and int(imp_type) == int(x.value['id']):
            return x
        elif type(imp_type) is str and imp_type == x.name:
            return x

