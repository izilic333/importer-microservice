import codecs
import csv
import json
import uuid
import os
import traceback
from jsonschema import Draft4Validator
from werkzeug.wrappers import Response
from elasticsearch_component.core.logger import CompanyProcessLogger
from elasticsearch_component.core.query_company import GetCompanyProcessLog
from common.logging.setup import logger
from common.mixin.validation_const import ALLOWED_EXTENSIONS, find_key_and_custom_message

logger_api = logger


def server_response(data, code, message, status):
    json_setup = {
        "status": status,
        "message": message,
        "data": data,
        "code": code
    }
    return Response(response=json.dumps(json_setup), status=code, content_type="application/json")


def server_socket(data, code, message, status):
    json_setup = {
        "status": status,
        "message": message,
        "data": data,
        "code": code
    }

    return json.dumps(json_setup)


def generate_uid():
    return uuid.uuid4().hex


def validate_file_extensions(input_file):
    return '.' in input_file and '.'+input_file.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def make_response(status, results, message):
    return {
        'status': status,
        'results': results,
        'message': message
    }



"""

    Date format check

"""

def validate_date_format_flask(date_format):
    import datetime
    try:
        datetime.datetime.strptime(date_format, '%Y-%m-%d')
        return {
            "success": True,
            "message": "Date format ok."
        }
    except ValueError:
        return {
            "success": False,
            "message":"Date is wrong formatted: %s. Please use format: YYYY-MM-DD" % date_format
        }


"""

    Elastic functions

"""


def function_check_elastic_status(company_id, import_type, api_name):
    elastic_check = (
        GetCompanyProcessLog.get_process_by_type(company_id, import_type, api_name)
    )
    return elastic_check


def generate_elastic_process(company_id, import_type, api_name):
    elastic_process = (
        CompanyProcessLogger.create_new_process(company_id, import_type, api_name)
    )
    return elastic_process


def return_something_from_list(inc, list_in):
    return list_in[inc]


def return_errors_from_json_schema(object_input, e, parser, language, line):
    count = line
    errors_append = []
    v = Draft4Validator(parser)
    errors = sorted(v.iter_errors(object_input), key=lambda e: e.schema_path)
    if len(e.context):
        for idx, item in enumerate(e.context):
            vt = return_something_from_list(idx, e.context)
            if len(vt.path) > 0:
                msg3 = find_key_and_custom_message(vt.path[0], language, parser)
                if msg3['success']:
                    insert_m = {"record": vt.path[0],
                                "message": "Line: {}: {}".format(count, msg3['message'])}
                    errors_append.append(insert_m)
                else:
                    insert_m = {"record": vt.path[0],
                                "message": "Line: {}: {}".format(count, vt.message)}
                    errors_append.append(insert_m)
            else:
                msg_extract = return_something_from_list(idx, e.context)
                error_path = msg_extract.message.split("'")[1]
                msg3 = find_key_and_custom_message(error_path, language, parser)
                if msg3['success']:
                    insert_m = {'record': error_path,
                                'message': 'Line: {}: {}'.format(count, msg3['message'])}
                    errors_append.append(insert_m)
                else:
                    insert_m = {'record': error_path,
                                'message': 'Line: {}: {}'.format(count, msg_extract.message)}
                    errors_append.append(insert_m)
        path = None
        for error in errors:
            if len(error.path) > 0:
                path = error.path[0]
            elif error.validator:
                path = error.validator
            if path:
                msg3 = find_key_and_custom_message(path, language, parser)
                if msg3['success']:
                    insert_m = {"record": path,
                                "message": "Line: {}: {}".format(count, msg3['message'])}
                    errors_append.append(insert_m)
                else:
                    insert_m = {"record": path, "message": "Line: {}: {}".format(count, e.message)}
                    errors_append.append(insert_m)
    else:
        path = None
        for error in errors:
            if len(error.path) > 0:
                path = error.path[0]
            elif error.validator:
                path = error.validator
            if path:
                msg3 = find_key_and_custom_message(path, language, parser)
                if msg3['success']:
                    insert_m = {"record": path,
                                "message": "Line: {}: {}".format(count, msg3['message'])}
                    errors_append.append(insert_m)
                else:
                    insert_m = {"record": path, "message": "Line: {}: {}".format(count, e.message)}
                    errors_append.append(insert_m)

    return errors_append

def generate_json(json_obj):
    return json.dumps(json_obj, sort_keys=True)


def generate_hash_for_json(json_obj):
    import hashlib
    unicode_object = generate_json(json_obj)
    hsh = hashlib.sha256(str(unicode_object).encode('utf-8')).hexdigest()
    return hsh


def check_two_objects_for_same_result(json_one, json_two):
    import hashlib
    obj1 = json.dumps(json_one, sort_keys=True)
    obj2 = json.dumps(json_two, sort_keys=True)

    hsh1 = hashlib.sha256(str(obj1).encode('utf-8')).hexdigest()
    hsh2 = hashlib.sha256(str(obj2).encode('utf-8')).hexdigest()

    if hsh1 != hsh2:
        return False
    else:
        return True


def convert_string_to_json(string):
    try:
        return json.loads(string)
    except json.decoder.JSONDecodeError as ex1:
        return string


def custom_hash(input_data):
    import hashlib
    hsh = hashlib.sha256(str(input_data).encode('utf-8')).hexdigest()
    return hsh


def custom_hash_vends(input_data):
    return "vends-"+custom_hash(input_data)


def delete_processed_file(file_path, process_logger):
    """

    :param file_path: full path to the specific file
    :return: status of deleted action
    """
    try:
        os.remove(file_path)
        return True
    except Exception:
        logger_api.error(process_logger.update_system_log_flow(
            traceback.print_exc(), key_enum=enum_msg.RENAME_LOCAL_FILE_ERROR.value))

    return False

class HandleValidationOfAPI(object):

    @classmethod
    def return_field_type(cls, input_data):
        """
            :return: determine/check file content type
        """

        if input_data is not None:
            value_type = input_data
            var_type = type(value_type)
            variable_changed = value_type
            if var_type.__name__ == 'int':
                return '{}'.format(int(variable_changed))
            elif var_type.__name__ == 'float':
                spl = str(variable_changed).split('.')
                if int(spl[1]) == 0:
                    return '{}'.format(round(float(variable_changed)))
                else:
                    return '{}'.format(float(variable_changed))
            elif var_type.__name__ == 'str':
                if variable_changed.lstrip('-').replace('.', '', 1).isdigit():
                    try:
                        z = float(variable_changed)
                        c = int(variable_changed.replace('.', '', 1))
                        if z != c:
                            spl = str(variable_changed).split('.')
                            if int(spl[1]) == 0:
                                return '{}'.format(round(float(variable_changed)))
                            else:
                                return '{}'.format(float(variable_changed))
                        else:
                            return '{}'.format(str(variable_changed))
                    except:
                        return '{}'.format(str(variable_changed).rstrip())
                else:
                    if len(variable_changed) > 0:
                        return '{}'.format(str(variable_changed).rstrip())
                    else:
                        return None
            elif variable_changed.lower() == 'true':
                return True
            elif variable_changed.lower() == 'false':
                return False
            else:
                return None
        else:
            return None

    @classmethod
    def handle_before_parsing(cls, parser, validator_type, json_data):
        parser_json = parser['custom_valid_fields']
        try:
            # Parse data
            serializer_out = []
            for row in json_data:
                out_row = {}
                for key, val in row.items():
                    # Find key
                    check_key = parser_json.get(key, None)
                    if (val):
                        current_value = cls.return_field_type(val.rstrip().lstrip())
                        if check_key:
                            if current_value and check_key:
                                if check_key.__name__ == 'str':
                                    out_row[key] = current_value
                                elif check_key.__name__ == 'int':
                                    out_row[key] = current_value
                                elif check_key.__name__ == 'float':
                                    out_row[key] = current_value
                            elif not current_value and check_key:
                                if check_key.__name__ == 'str':
                                    if validator_type == 'cloud_validator':
                                        out_row[key] = ''
                                    elif validator_type == 'csv_validator':
                                        out_row[key] = None
                                elif check_key.__name__ == 'int':
                                    out_row[key] = None
                                elif check_key.__name__ == 'float':
                                    out_row[key] = None
                        else:
                            out_row[key] = None
                    else:
                        if check_key.__name__ == 'str':
                            if validator_type == 'cloud_validator':
                                out_row[key] = ''
                            elif validator_type == 'csv_validator':
                                out_row[key] = None
                        elif check_key.__name__ == 'int':
                            out_row[key] = None
                        elif check_key.__name__ == 'float':
                            out_row[key] = None

                if out_row:
                    serializer_out.append(out_row)
                else:
                    continue
            return serializer_out

        except Exception as e:
            logger_api.error(
                'Problems with converting fields on API parser. Data: {} Error: {}'
                    .format(json_data, e)
            )

    @classmethod
    def handle_api_data_with_cloud_fields(cls, parser, validator_type, json_data):
        """

        :param parser: parser type (location, machine, machine_type, regions)
        :param validator_type: validator_type: data type for cloud or importer (json schema validation)
        :param json_data: input json
        :return: array of json objects
        """
        parser_json = parser['custom_valid_fields']
        try:
            # Parse data
            serializer_out = []
            for row in json_data:
                out_row = {}
                for key, val in row.items():
                    # Find key
                    if len(key):
                        if val:
                            current_value = cls.return_field_type(val.rstrip().lstrip())
                            check_key = parser_json.get(key, None)
                            if check_key:
                                if current_value and check_key:
                                    if check_key.__name__ == 'str':
                                        out_row[key] = current_value
                                    elif check_key.__name__ == 'int':
                                        out_row[key] = current_value
                                    elif check_key.__name__ == 'float':
                                        out_row[key] = current_value
                                elif not current_value and check_key:
                                    if check_key.__name__ == 'str':
                                        if validator_type == 'cloud_validator':
                                            out_row[key] = ''
                                        elif validator_type == 'csv_validator':
                                            out_row[key] = None
                                    elif check_key.__name__ == 'int':
                                        out_row[key] = None
                                    elif check_key.__name__ == 'float':
                                        out_row[key] = None
                            if not current_value:
                                if check_key.__name__ == 'str':
                                    if validator_type == 'cloud_validator':
                                        out_row[key] = ''
                                    elif validator_type == 'csv_validator':
                                        out_row[key] = None
                                elif check_key.__name__ == 'int':
                                    out_row[key] = None
                                elif check_key.__name__ == 'float':
                                    out_row[key] = None
                        else:
                            if check_key.__name__ == 'str':
                                if validator_type == 'cloud_validator':
                                    out_row[key] = ''
                                elif validator_type == 'csv_validator':
                                    out_row[key] = None
                            elif check_key.__name__ == 'int':
                                out_row[key] = None
                            elif check_key.__name__ == 'float':
                                out_row[key] = None

                if out_row:
                    serializer_out.append(out_row)
                else:
                    continue

            # Pop null values
            """
            for res in serializer_out:
                for key, value in list(res.items()):
                    if value is None:
                        res.pop(key)
                    elif not value:
                        res.pop(key)
            """

            logger_api.info('Parsed input before <null>: {}'.format(serializer_out))

            all_fields = parser['all_fields']

            # Prepare for cloud
            for x in serializer_out:
                p = [u for u in x.keys()]
                rel = list(set(all_fields) - set(p))
                for z in rel:
                    x[z] = '<null>'

            logger_api.info('Parsed input after <null>: {}'.format(serializer_out))

            return serializer_out

        except Exception as e:
            logger_api.error(
                'Problems with converting fields on API parser. Data: {} Error: {}'
                 .format(json_data, e)
            )


def import_file_content_to_json(working_file, delimiter):
    csv_data = []
    with codecs.open(working_file, 'r', encoding='utf-8', errors='ignore') as csv_file:
        reader = csv.DictReader(csv_file, delimiter=delimiter)
        for row in reader:
            csv_data.append(row)

    return csv_data


def mandatory_geo_location(address, latitude, longitude):
    """
    This function make basic validation for geo location import:
        a) location_address, latitude & longitude are defined -> mandatory latitude & longitude
        b) location_address not defined, latitude & longitude are defined -> mandatory latitude & longitude
        c) location_address are defined, latitude & longitude not defined -> mandatory location_address
        d) location_address, latitude & longitude not defined -> not valid import file

    :param address: geo location address str()
    :param latitude: geo location latitude str()
    :param longitude: geo location longitude str()
    :return: mandatory_status dict
    """

    mandatory_status = {
        'mandatory_fields': ['location_address', 'latitude', 'longitude'],
        'valid': False,
        'message': 'location_address or latitude and longitude are mandatory!'
    }

    if address and latitude and longitude:
        mandatory_status = {
            'mandatory_fields': ['latitude', 'longitude'],
            'valid': True,
            'message': "location_address, latitude and longitude are defined, latitude and longitude become mandatory!"
        }

    elif latitude and longitude:
        mandatory_status = {
            'mandatory_fields': ['latitude', 'longitude'],
            'valid': True,
            'message': "latitude and longitude are defined, so this fields become mandatory fields!"
        }

    elif address:
        mandatory_status = {
            'mandatory_fields': ['location_address'],
            'valid': True,
            'message': 'location_address are defined, so this field becomes mandatory field!'
        }

    return mandatory_status
