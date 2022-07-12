from enum import Enum
from common.logging.setup import logger

logger_api = logger

roles_support = [
    'SuperAdministrator',
    'SuperadminSupport',
    'Administrator',
    'MulticompanyManager',
    'CompanyManager',
    'CompanyAdministrator'
]


class LimitRequest(Enum):
    LIMIT_GET = 25


class EnumAPIType(Enum):
    GET = 0
    POST = 1


class EnumProcessType(Enum):
    FILE = 1
    API = 2
    API_FILE = 3


class ProcessEnum(Enum):
    PROCESS_FILE_VALIDATION = 1
    PROCESS_CLOUD_VALIDATION = 2
    PROCESS_CLOUD_INSERT = 3


class UserEnum(Enum):
    ADMIN = 1
    USER = 2


class EnumErrorType(Enum):
    STARTED = 1
    ERROR = 2
    SUCCESS = 3
    FAIL = 4
    IN_PROGRESS = 5
    WARNING = 6


class PlanogramEnum(Enum):
    MAX_COLUMNS = 32767
    EMPTY_HEADER_VALUE = [u'<null>', None, ""]


def return_enum_error():
    error_type = []
    for x in EnumErrorType:
        if x.value in [EnumErrorType.ERROR.value, EnumErrorType.SUCCESS.value,
                       EnumErrorType.FAIL.value, EnumErrorType.WARNING.value]:
            error_type.append({'name': x.name})
    return error_type


class EnumMessageDescription(Enum):
    CLOUD = 'cloud'
    ADMIN = 'admin'
    CLOUD_ADMIN = 'both'


class EnumValidationMessage(Enum):

    FTP_TRY_CONNECT = {
        "en": "Try connect to FTP server: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    FTP_CONNECTION_ERROR = {
        "en": "There was an error with connection to FTP server.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD_ADMIN.name
    }
    FTP_CONNECTION_ERROR_SYSTEM_LOG = {
        "en": "There was an error with connection to FTP server, error: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD_ADMIN.name
    }
    FTP_PATH_ERROR_SYSTEM_LOG = {
        "en": "FTP path doesn't exists, for FTP server: {} , error: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD_ADMIN.name
    }
    FTP_FILE_ALREADY_PROCESSED = {
        "en": "FTP file already processed, for FTP server: {}, company: {}, import type: {}, file: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD_ADMIN.name
    }
    FTP_LIST_REMOTE_DIR_SYSTEM_LOG = {
        "en": "Can't list remote FTP dir, for FTP server: {}, company: {}, import type: {}, error: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD_ADMIN.name
    }
    FTP_CONNECTING = {
        "en": "Connected to FTP server: {}",
    }
    FTP_CONNECTED = {
        "en": "Connected to FTP server: {}@{}:{}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    FTP_FILE_LIST = {
        "en": "Files available on FTP server: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    FTP_DOWNLOAD_ERROR = {
        "en": "There was an error downloading the file: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD_ADMIN.name
    }
    FTP_START_DOWNLOAD_FILE = {
        "en": "File start download: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    FTP_SUCCESSFULLY_DOWNLOAD_FILE = {
        "en": "Successfully download list of files: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    FTP_NUMBER_SUCCESSFULLY_DOWNLOADED_FILE = {
        "en": "The number of processing files: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    FTP_DOWNLOADED_AS = {
        "en": "Remote file {} saved as {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    FTP_DELETED_FILE = {
        "en": "File deleted: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    FTP_PATH_ERROR = {
        "en": "The server path to file does not exist. {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD_ADMIN.name
    }
    FILE_RIGHT_PATH = {
        "en": "Successfully changed to path directory on FTP server:  {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    FILE_EMPTY = {
        "en": "File has no content. Download {} examples and change file accordingly.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD_ADMIN.name
    }
    FILE_WRONG_FORMAT = {
        "en":  "File {} is of wrong type or extension. Please use .csv or xls/xlsx",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD_ADMIN.name
    }
    FILE_WITHOUT_HEADER = {
        "en": "File: {} has no header. Download {} examples and change file accordingly.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD_ADMIN.name
    }
    FILE_EXTENSION_ERROR = {
        "en": "Can't determine file format, there is unknown format. {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD_ADMIN.name
    }
    FILE_START_VALIDATION_FORMAT = {
        "en": "Start format validation process for file: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }

    FILE_IN_QUEUE_FOR_DATABASE_VALIDATION = {
        "en": "File: {} in Q for database validation",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }

    FILE_ZIP_PROCESSING= {
        "en": "Validation for file {} is processed with another hash, because there is a possibility of more than one file in zip archive.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD_ADMIN.name
    }
    FILE_WITH_OK_FORMAT = {
        "en": "OK format for file: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    VALIDATION_FILE_ERROR = {
        "en": "Error occurred on processing file: {}, error: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    FILE_START_VALIDATION_FIELDS = {
        "en": "Start fields validation process for file: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    FILE_WITHOUT_MANDATORY_FIELDS = {
        "en": "Mandatory fields are missing in file {}. Download {} examples and change file accordingly.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD_ADMIN.name
    }
    FILE_SEND_TO_rabbitMQ = {
        "en": "File has field with OK format type and data send to rabbitMQ, file: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    PLANOGRAM_MINIMUM_ROUTE_PICKUP = {
        "en": "Minimum route pickup: {} can't be larger than fill quantity: {} !",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    FILE_WITH_WRONG_HEADER_NAME= {
        "en": "File contain wrong header fields name: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD_ADMIN.name
    }
    API_VALIDATION = {
        "en": "Api passed successfully validation.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    SUCCESS_FILE_VALIDATION = {
        "en": "File passed successfully validation, the file path: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    IMPORT_PROCEDURE_PER_COMPANY = {
        "en": "Import process with elastic hash: {}, can't start, because import process with elastic hash: {} still active and running, company_id: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    IMPORT_PROCEDURE_RUN_NEXT_PROCESS = {
        "en": "Import process with elastic hash: {} finished, start process with elastic hash: {}, company_id: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    IMPORT_PROCEDURE_NO_ACTIVE_PROCESS = {
        "en": "There is no active import process for company_id: {}, starting new import with elastic hash: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    FAIL_FILE_VALIDATION = {
        "en": "File was not successfully validated. Download {} examples and change file accordingly.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD_ADMIN.name
    }

    JSON_SCHEMA_ERROR_DUMP = {
        "en": "File was not successfully validated, errors: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD_ADMIN.name
    }
    FAIL_MISSING_HEADER = {
        "en": "File contain wrong header fields name: {}. Download {} examples and change file accordingly.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD_ADMIN.name
    }
    USER_ERROR = {
        "en": "The user has no rights for this operation.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD_ADMIN.name
    }
    ALREADY_SUCCESS_PROCESSED_FILE = {
        "en": "No changes were made. This file was already succesfully imported at {}.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD_ADMIN.name
    }
    ALREADY_FAIL_PROCESSED_FILE = {
        "en": "No changes were made. Import of this file failed on {}. Download examples and change file accordingly.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD_ADMIN.name
    }
    CREATE_DIR_ERROR = {
        "en": "Can't create importer initial directory: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    RENAME_LOCAL_FILE_ERROR = {
        "en": "Can't move or rename file: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    VEND_INITIAL_FILE_ERROR = {
        "en": "File with wrong name convention: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    VEND_INITIAL_FILE_ERROR_SUB_MESSAGE = {
        "en": "File with wrong name convention: {}, you can find this message with elastic hash: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }

    IMPORT_TYPE_REDIS_KEY_DURATION_ERROR = {
        "en": "Can't load redis key duration from envdir configuration, please check your configuration in IMPORT_TYPE_REDIS_KEY_DURATION file, error: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    IMPORT_TYPE_REDIS_KEY_DURATION_DEFAULT = {
        "en": "Can't load redis key duration from envdir configuration, so default redis key duration: {} is used for all import type",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }

    DEFINED_IMPORT_TYPE_REDIS_KEY_DURATION = {
        "en": "Import type: {}, company_id: {}, used redis key duration: {} sec",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }

    DELETE_LOCAL_FILE_ERROR= {
        "en": "Can't delete local working directory: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }

    DELETE_LOCAL_DIR_ERROR_SYSTEM_LOG = {
        "en": "Can't delete local working directory: {}, error: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    DELETE_LOCAL_FILE_ERROR_SYSTEM_LOG = {
        "en": "Can't delete local working file: {}, error: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    VALIDATION_SYSTEM_LOG_INFO = {
        "en": "This is all error of file validation {}: ",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    VALIDATION_ELASTIC_INSERT_START_TIME = {
        "en": "Validator: {}, start insert message in elastic, total message: {}: ",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    VALIDATION_ELASTIC_INSERT_END_TIME = {
        "en": "Validator: {}, finish insert message in elastic, total message: {}: ",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    VALIDATION_SYSTEM_LOG_FTP_ERROR = {
        "en": 'FTP: empty directory for company_id {} and elastic hash: {} FTP Details -> Host: {} Port:{} Path: {}',
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    VALIDATION_MAIN_PROCESS_FTP = {
        "en": "There is no file on FTP server!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    VALIDATION_SYSTEM_LOG_ERROR_REMOVING_FTP_FILE = {
        "en": "Error removing remote file {}, error: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    VALIDATION_SYSTEM_LOG_WRONG_FORMAT_FTP = {
        "en": "File on FTP has wrong format {}, hostname: {}, filename: {}, error: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    VALIDATION_SYSTEM_LOG_FAIL_HISTORY = {
        "en": "Error moving file to HISTORY_FAIL_DIR {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    VALIDATION_SYSTEM_LOG_ERROR_CONVERTING_FILE = {
        "en": "Can't convert xlsx/xls file to csv, filename: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    VALIDATION_SYSTEM_LOG_INFO_HEADER_FILE = {
        "en": "This is file header {}:",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    VALIDATION_SYSTEM_LOG_FIELD_ERROR = {
        "en": "Error occurred on field {}: value: {}, error: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    VALIDATION_SYSTEM_LOG_INFO_INDEX_MESSAGE = {
        "en": "File index {}, message: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    VALIDATION_SYSTEM_LOG_INFO_CONTENT_FILE = {
        "en": "This is file content {}:",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    VALIDATION_SYSTEM_LOG_INFO_CONVERTING_FILE = {
        "en": "Successful second attempt of converting xlsx/xls to csv, filename: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    VALIDATION_SYSTEM_LOG_PANDAS_ERROR = {
        "en": "Pandas error, during handle xls/xlsx file: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    VALIDATION_SYSTEM_LOG_INFO_CONVERTED_FILE = {
        "en": "Successful convert xlsx/xls to csv, filename: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    VALIDATION_SYSTEM_LOG_CONTENT_INFO = {
        "en": "Converting xls/xlsx: file index {}, message: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.ADMIN.name
    }
    DATABASE_CONNECTION = {
        "en": "Can't connect to database: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    DATABASE_TIME_OUT = {
        "en": "Database timed out. {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    DATABASE_QUERY_ERROR = {
        "en": "Database query error occurred: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    DATABASE_VALIDATION_ERROR = {
        "en": "Database validation error occurred. {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    DATABASE_VALIDATION_WARNING = {
        "en": "Database validation warning occurred. {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    DATABASE_VALIDATION_SUCCESS = {
        "en": "Database validation finished with success: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    DATABASE_INSERT_ERROR = {
        "en": "Database insert received error: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    DATABASE_UPDATE_ERROR = {
        "en": "Database update received error: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    DATABASE_SUCCESS_INSERT_UPDATE = {
        "en": "Database action was successful: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    DATABASE_VALIDATION_STARTED = {
        "en": "Database validation started for import type: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    DATABASE_STATISTICS_RESULT = {
        "en": "Import result - Inserted: {}, Updated: {}, Deleted: {}, Reused: {}, Error: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    DATABASE_UNKNOWN_TYPE_ERROR = {
        "en": "All actions must be UNKNOWN (50)!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    DATABASE_REPEAT_IMPORT_ERROR = {
        "en": "The same data already processed.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    DATABASE_UNKNOWN_IMPORT_ERROR = {
        "en": "Unknown process type: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    DATABASE_WRONG_ACTION = {
        "en": "{}, wrong import action requested: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    DATABASE_NOT_FOUND = {
        "en": "{} {} not found in cloud, but {} requested",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    ACTIVE_REGION_ON_LOCATION = {
        "en": "{} is assigned on location {}, and couldn't be deleted",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    ASSIGNED_REGION = {
        "en": "{} {} is assigned on location, and couldn't be deleted",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    ASSIGNED_MACHINE_CLIENT = {
        "en": "Client id: {} has machines assigned, and couldn't be deleted",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    ALL_ENTITIES_REMOVED = {
        "en": "No entities remaining after removal of non processable entities",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    MACHINE_CLUSTER_ID = {
        "en": "Machine cluster id: {}, doesn't exists",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    
    DATABASE_FOUND = {
        "en": "{} {} found in cloud, but {} requested",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    DATABASE_NO_CLIENT_TYPE = {
        "en": "Client type id {} not found in cloud",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    DATABASE_NO_LOCATION = {
        "en": "Location {} for machine not found in cloud",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    DATABASE_NO_MACHINE_TYPE = {
        "en": "Machine type {} for machine not found in cloud",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    DATABASE_NO_REGION = {
        "en": "Region {} not found in cloud",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    DATABASE_NO_PARENT_REGION = {
        "en": "Requested parent region {} not found in cloud",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    DATABASE_NO_PARENT_CLIENT = {
        "en": "Requested parent client id {} not found in cloud",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    DATABASE_NO_MACHINE_CATEGORY = {
        "en": "Machine category {} not found in cloud",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    DATABASE_NO_WAREHOUSE = {
        "en": "Warehouse {} not found in cloud",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    CONSUMER_VALIDATION_ERROR_DETAILS = {
        "en": "CSV validator error {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    CSV_VALIDATOR_ERROR = {
        "en": "Field validator error for {} import type.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    CLOUD_VALIDATION_EXCEPTION_ERROR = {
        "en": "System error occurred. Please try to import the file again!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    CLOUD_VALIDATION_EXCEPTION_ERROR_DETAIL_MESSAGE = {
        "en": "System error occurred. Please try to import the file again, error details: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    COMPANY_NOT_EXISTS = {
        "en": "Company does not exist: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    TAX_RATE_NOT_EXISTS = {
        "en": "Tax rate {} does not exist for this company. Please choose an existing tax rate.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    TAX_RATE_DUPLICATE_FOUND = {
        "en": "Tax rates should be unique on company level. Currently you have multiple instances of the same tax rate, {}.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    FIELD_NOT_UNIQUE = {
        "en": "Field {} is expected to be unique for each product. You have several repeating values against the database: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    FIELD_NOT_UNIQUE_PRODUCT_EXTERNAL_ID = {
        "en": "Field {} is expected to be unique for each product. You have several repeating values against the database: {}, product external_id: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    FIELD_UNIQUENESS_IN_FILE = {
        "en": "Field {} should be a unique field. You have several repeating values in file: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PRODUCT_DELETE_ERROR_MACHINES = {
        "en": "Product {} cannot be deleted as it is present in {} machines. E.g.: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PRODUCT_DELETE_ERROR_PLANOGRAMS = {
        "en": "Product {} cannot be deleted as it is present in following planograms: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PLANOGRAM_ERROR_BUILD_ENTITY_FOR_DATABASE = {
        "en": "Error occurred on planogram import, during entity build, record: {}, error description: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }


    NO_PRODUCTS_ON_COMPANY = {
        "en": "There is no product added on company: {}, please first add some product on company and try again import planogram!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    GET_DATA_FOR_PLANOGRAM = {
        "en": "Get planogram init data from database: {}, {}, {}, {}, {}, {}, {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    SUCCESS_FETCHED_PLANOGRAM_DATA = {
        "en": "Success fetched planogram data",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    FETCH_PLANOGRAM_DATA = {
        "en": "Fetch main planogram data from db",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_MAIN_FILTER = {
        "en": "Start with basic filter on main planogram data",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_MAIN_FILTER_SUCCESS = {
        "en": "Successfully created basic filter on main planogram data",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    INIT_PLANOGRAM_VALIDATION_START = {
        "en": "Start init planogram validation",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    INIT_PLANOGRAM_VALIDATION_SUCCESS = {
        "en": "Successfully passed init planogram validation",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    CORE_PLANOGRAM_VALIDATION_START = {
        "en": "Start planogram validation, the major part",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PLANOGRAM_IMPORTER_BUILD_IMPORT_ENTITY = {
        "en": "Planogram importer start to build import entity",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_IMPORTER_SUCCESS_BUILD_IMPORT_ENTITY = {
        "en": "Planogram importer success build import entity",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_IMPORTER_POPULATE_IMPORT_OBJECTS = {
        "en": "Planogram importer start to populate import objects",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_IMPORTER_FINISH_POPULATE_IMPORT_OBJECTS = {
        "en": "Planogram importer finish with import objects",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_IMPORTER_CALL_PL_SQL_PROCEDURE = {
        "en": "Planogram importer call PL/SQL procedure",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_IMPORTER_PL_SQL_PROCEDURE_FINISHED = {
        "en": "Planogram importer PL/SQL procedure finished",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    ERROR_ON_PLANOGRAM_SAVE = {
        "en": "Database error on planogram save: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_START_VALIDATION = {
        "en": "Planogram importer start validation",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_FINISH_VALIDATION = {
        "en": "Planogram importer finish validation",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_FETCH_INIT_DATA = {
        "en": "Fetch data for successfully validated import planogram rows {}, {}, {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VALUE_INCORRECT_FORMAT = {
        "en": "Incorrect value format for field {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VEND_ERROR_ON_CREATING_ELASTIC_HASH = {
        "en": "Elastic search problem, can't generate elastic hash, vend importer stop processing, company: {}, import type: {}!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    VEND_PROCESSED_EXTENSION = {
        "en": "Compress filename hasn't allowed initial extension,  filename: {}, initial extension should be: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    VEND_PROCESSED_EXTENSION_SUB_MESSAGE = {
        "en": "Compress filename hasn't allowed initial extension,  filename: {}, you can find this message with elastic hash: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VEND_PROCESSED_WORKING_EXTENSION = {
        "en": "Extension is not in allowed extension for eva file : {}, wrong extension: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    VEND_PROCESSED_WORKING_EXTENSION_SUB_MESSAGE = {
        "en": "Extension is not allowed for eva file, you can find this message on elastic hash: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VEND_IMPORTER_WORKING_FILE = {
        "en": "There is no eva file in zip archive: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VEND_IMPORTER_WORKING_FILE_SUB_MESSAGE = {
        "en": "There is no eva file in zip archive: {}, you can find this message on elastic hash: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VEND_IMPORTER_EMPTY_WORKING_FILE = {
        "en": "Empty file: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VEND_IMPORTER_EMPTY_WORKING_FILE_SUB_MESSAGE = {
        "en": "Empty file: {}, you can find this message on elastic hash: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VEND_IMPORT_PID_EQUAL_TO_ARCHIVE_PID = {
        "en": "Match device pid in local history, device pid: {}, filename: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    ERROR_OCCUR_ON_OPEN_EVA_FILE = {
        "en": "Successful unzip file, but error occur on open eva file: {}, please check your file!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    ERROR_OCCUR_ON_OPEN_EVA_FILE_SUB_MESSAGE = {
        "en": "Successful unzip file, but error occur on open eva file: {}, you can find this message on elastic hash: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    FOUND_INITIAL_EVA = {
        "en": "Found initial eva, device pid: {}, filename: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VEND_IMPORTER_PUBLISH_DATA_TO_CLOUD_VALIDATOR = {
        "en": "Importer publish data to cloud validator, for company: {}, import type: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VEND_CLOUD_VALIDATOR_START_PROCESSING = {
        "en": "Start processing paired eva file, old eva filename: {}, new eva filename: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VEND_DEVICE_PAIRED_WITH_NEW_MACHINE = {
        "en": "Your device: {} is paired with new machine: {}, old machine: {}, your new init eva is in file: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    DETECT_PAIRED_EVA_WITHOUT_MACHINE_EXTERNAL_ID = {
        "en": "Eva paired with cloud machine, machine without external id detected, for company: {}, import type: {},  eva pid: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    DEX_EVA_WITHOUT_MACHINE_EXTERNAL_ID = {
        "en": "Eva paired with cloud machine, machine without external id detected, for company: {}, import type: {},  machine id: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    DETECT_PAIRED_EVA_DEVICE_INFO = {
        "en": "Eva paired with cloud machine, eva pid: {}, device id: {}, device alive: {}, device status: {}, device type id: {}, machine external id: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    DEX_DETECT_PAIRED_EVA_INFO = {
        "en": "Eva paired with cloud machine, machine id: {}, machine external id: {}, machine name: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    START_CALCULATE_VENDS = {
        "en": "Importer start calculate vends, for company: {}, import type: {},  eva pid: {}, old eva timestamp: {} new eva timestamp: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    DEX_START_CALCULATE_VENDS = {
        "en": "Importer start calculate vends, for company: {}, import type: {},  machine id: {}, old eva timestamp: {} new eva timestamp: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    MACHINE_IS_NOT_DEX = {
        "en": "Machine {}, is not DEX, device type: {}, skip this file {}.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VEND_IMPORTER_ERROR_OCCUR = {
        "en": "Error occur on vend calculating, eva pid: {}, filename: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VEND_IMPORTER_ERROR_OCCUR_SUB_MESSAGE = {
        "en": "Error occur on vend calculating, eva pid: {}, filename: {}, you can find this message with elastic hash: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VEND_IMPORTER_ERROR_OCCUR_DETAIL = {
        "en": "Error occur on vend calculating, for company: {}, import type: {},  eva pid: {}, error: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VEND_IMPORTER_DEVICE_PID_NOT_PAIRED_WITH_MACHINE = {
        "en": "There is no machine paired with device: {}, you can download a list of files that was not paired with machine, iteration occurred at: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    DEVICE_PID_NOT_PAIRED_WITH_MACHINE_SUB_MESSAGE = {
        "en": "There is no machine paired with device: {}, you can find this message with main elastic hash: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    DEVICE_PID_NOT_PAIRED_WITH_MACHINE_MAIL_MESSAGE = {
        "en": "There is no machine paired with device, please check this device pid: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VEND_IMPORTER_NO_VENDS_DETECTED = {
        "en": "There is no new vends for device pid: {}, machine external id: {}, filename : {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    DEX_VEND_IMPORTER_NO_VENDS_DETECTED = {
        "en": "There is no new vends for machine id: {}, machine external id: {}, filename : {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    ERROR_OCCUR_ON_CREATING_ZIP_ARCHIVE = {
        "en": "Error occur on generating success zip archive, error: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VEND_IMPORTER_ERROR_OCCUR_ON_VEND_CALCULATE = {
        "en": "Old archive have more vends than new eva, for device pid: {}, machine external id: {}, wrong vend difference: {}, old eva fields: {}, new eva fields : {}, filename: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    DEX_VEND_IMPORTER_ERROR_OCCUR_ON_VEND_CALCULATE = {
        "en": "Old archive have more vends than new eva, for machine id: {}, machine external id: {}, wrong vend difference: {}, old eva fields: {}, new eva fields : {}, filename: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    ERROR_OCCUR_ON_VEND_CALCULATE_SUB_MESSAGE = {
        "en": "Old archive have more vends than new eva, for device pid: {}, you can find this message with elastic hash: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    DEX_ERROR_OCCUR_ON_VEND_CALCULATE_SUB_MESSAGE = {
        "en": "Old archive have more vends than new eva, for machine id: {}, you can find this message with elastic hash: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VEND_IMPORTER_NEW_VEND_FOUND = {
        "en": "New vends, device pid: {}, machine external id: {}, filename: {}, vends: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    DEX_VEND_IMPORTER_NEW_VEND_FOUND = {
        "en": "New vends, machine id: {}, machine external id: {}, filename: {}, vends: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VEND_IMPORTER_NEW_VEND_FOUND_PER_DEVICE = {
        "en": "Importer calculate total vends for device pid: {}, machine id: {}, vends: {}, keep in mind this is total vends in processing files!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    DEX_VEND_IMPORTER_NEW_VEND_FOUND_PER_DEVICE = {
        "en": "Importer calculate total vends  for machine id: {}, vends: {}, keep in mind this is total vends in processing files!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    VEND_REIMPORT_INFO = {
        "en": "Reimport file found: {}, machine_id: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VEND_REIMPORT_FILE = {
        "en": "The vends of file: {} will be reduced for {} vends , machine_id: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    NEW_VEND_CLOUD_MESSAGE = {
        "en": "Response from cloud, total records: {}, this is all inserted records!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    DATA_IMPORT_INTO_CLOUD = {
        "en": "Data imported into cloud database: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    CLOUD_IMPORT_ERROR_LIST = {
        "en": "Following errors were reported by cloud: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    CLOUD_IMPORT_GENERAL_FAIL = {
        "en": "Error occur on cloud, during processing vends for insert into database!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PARTIALLY_VEND_IMPORT = {
        "en": "Data was partially imported into cloud database {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    ERROR_ON_REIMPORT = {
        "en": "Can't make reimport, because reimport file {} has less vends: {} than import file vends: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    NO_VEND_CLOUD_IMPORT = {
        "en": "Data was not imported into cloud database {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VEND_IMPORTER_ERROR_ON_CLOUD_MESSAGE = {
        "en": "Cloud error occur on vend insert: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VEND_EVA_DATA_PUBLISHED_TO_CLOUD = {
        "en": "Data publish to cloud, vend data: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VEND_EVA_DATA_PUBLISHED_TO_CLOUD_SYSTEM_LOG = {
        "en": "Data publish to cloud company: {}, import type: {}, data: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VENDON_DATA_PUBLISHED_TO_CLOUD = {
        "en": "Vendon Data publish to cloud queue.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VENDON_VENDS_START_REQUEST = {
        "en": "Vendon vends API call requested: {} with params {}.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VENDON_VENDS_END_REQUEST = {
        "en": "Vendon vends API call returned {} records.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VENDON_API_ERROR = {
        "en": "Error connecting to Vendon API: {}",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VENDON_UNKNOWN_ERROR = {
        "en": "Error processing Vendon data",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VENDON_UNKNOWN_ERROR_DETAILED = {
        "en": "Error processing Vendon data: {}",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VENDON_TRANSFORM_ERROR = {
        "en": "Data transformation error from Vendon API: {}",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VENDON_TRANSFORM_ERROR_FATAL = {
        "en": "Fatal data transformation error from Vendon API: {}",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VENDON_MACHINE_NOT_FOUND = {
        "en": "Machine with id {} not found in Vendon",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VENDON_MACHINE_NOT_FOUND_CLOUD = {
        "en": "Machine with Vendon id {} using external id {} not found in cloud",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VENDON_MACHINE_NOT_VENDON_DEVICE = {
        "en": "Machine with Vendon id {} using external id {} is not Vendon device",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VENDON_CLOUD_SKIPPED = {
        "en": "Following records were skipped due to validation errors: {} ",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VENDON_CLOUD_ALL_SKIPPED = {
        "en": "All vends were skipped due to validation errors. No valid vends remaining. ",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VENDON_MACHINE_ID_EMPTY = {
        "en": "Machine with id {} has empty machine_id value ",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    VENDON_DOUBLE_TRANSACTION_ID = {
        "en": "Transaction with id {} is already recorded ",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    NETWORK_ERROR = {
        "en": "Network error: {}",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PACKING_INVALID_DATA = {
        "en": "Packings configuration is invalid -> [{}].",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PACKING_SINGLEPACK_SKIPPED = {
        "en": "Single pack for product {} skipped.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PACKING_INVALID_DEFAULTS = {
        "en": "Packings for product {} have invalid default value configuration after import.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PACKING_INVALID_QUANTITIES = {
        "en": "Packings for product {} have repeated or invalid quantities.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PACKING_INVALID_NAME = {
        "en": "Packings for product {} use invalid name {}.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PACKING_INVALID_PRODUCT = {
        "en": "Packing  {} uses invalid product id {}.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PACKING_ANOTHER_PRODUCT = {
        "en": "Packing  {} already used by product {}.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PACKING_PRODUCT_NO_USE_PACKING = {
        "en": "Packing  {} product  {} does not use packing.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PACKING_REPEATED_NAME = {
        "en": "Packings for product {} use repeated name {}.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PACKING_REPEATED_EXTERNAL_ID = {
        "en": "Packings uses repeated external id {}.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PACKING_REPEATED_EXTERNAL_ID_PRODUCT = {
        "en": "Product uses external id {} used by existing packing .",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    DELETE_DEFAULT_BARCODE_WHEN_ADDITIONAL_BARCODE_EXIST_ERROR = {
        "en": "You try to delete default barcode, first you have to delete additional barcodes in order to be able to delete default barcode, product external_id: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PRODUCT_ROTATION_GROUP_STOP_DELETE = {
        "en": "You try to delete product assigned on product rotation group, first you have to delete product from rotation group, product: {}, product external_id: {}, rotation group: {}, rotation group external_id: {}, action: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    STOP_CREATE_ADDITIONAL_BARCODE = {
        "en": "First, you have to create defualt barcode in order to be able to create additional barcodes, product external_id: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PACKING_REPEATED_BARCODE_PRODUCT = {
        "en": "Product uses barcode {} used by existing packing .",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PLANOGRAM_NAME_ALREADY_EXISTS_ON_CLOUD = {
        "en": "Planogram with name {}, already exists in cloud database!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PLANOGRAM_IMPORT_FILE_TO_MANY_INCORRECT_ROWS = {
        "en": "Planogram file contains to many incorrect import rows, more than: {}, please try again with the correct records",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PLANOGRAM_EXTERNAL_ID_ALREADY_EXISTS_ON_CLOUD = {
        "en": "Planogram with external_id {}, already exists in cloud database!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_PRODUCT_NOT_FOUND = {
        "en": "Product with external_id {} doesn't exists in cloud database!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PLANOGRAM_PRODUCT_EMPTY = {
        "en": "There is no created products on company : {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PLANOGRAM_PRODUCT_WITH_DUPLICATE_EXT_ID = {
        "en": "Planogram: {} discarded, because the product has duplicate external_id : {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PLANOGRAM_PRODUCT_WITH_DUPLICATE_RECIPE_EXT_ID = {
        "en": "Planogram: {} discarded, because the product has recipe with duplicate external_id : {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PLANOGRAM_PRODUCT_WITH_DUPLICATE_COMBO_RECIPE_EXT_ID = {
        "en": "Planogram: {} discarded, because the product has combo recipe with duplicate external_id : {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PLANOGRAM_IMPORT_ROTATION_GROUP_NOT_FOUND_ON_CLOUD = {
        "en": "Product rotation group with external_id {} doesn't exist in cloud database!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PLANOGRAM_ROTATION_GROUP_WITHOUT_PRODUCTS = {
        "en": "Product Rotation Group for given external id: {} does not have any assigned products",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PLANOGRAM_IMPORT_RECIPE_NOT_FOUND = {
        "en": "Planogram: {} with recipe_external_id {} doesn't exists on cloud!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_IMPORT_COMBO_RECIPE_NOT_FOUND = {
        "en": "Planogram: {} with combo recipe_external_id {} doesn't exists on cloud!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_IMPORT_RECIPE_NOT_PAIRED_WITH_PRODUCT = {
        "en": "Planogram: {} with recipe_external_id {} discarded, because the product: {} does not have proper recipe_external_id",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_IMPORT_COMBO_RECIPE_NOT_PAIRED_WITH_PRODUCT = {
        "en": "Planogram: {} with combo recipe_external_id {} discarded, because the product: {} does not have proper recipe_external_id",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_IMPORT_RECIPE_NOT_SENT = {
        "en": "Planogram: {} with recipe with external_id {} for product_id: {} doesn't exists on cloud!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_IMPORT_COMBO_RECIPE_NOT_SENT = {
        "en": "Planogram: {} with combo recipe with external_id {} for product_id: {} doesn't exists on cloud!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_IMPORT_REGULAR_PRODUCT_WITH_RECIPE = {
        "en": "Regular product_id: {}, has recipe_id: {}, column: {} discard recipe_id for regular product! ",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_IMPORT_PRODUCT_IS_COMBO = {
        "en": "Product with external_id {} and recipe with external_id {} is combo product!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_IMPORT_PRICE_2_NOT_SENT = {
        "en": "Product with with external_id {} value for price_2 not sent!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_PRICE_LIST = {
        "en": "Discard whole file, because you sent wrong multiple price list: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_COMPANY_PRICE_CHECK = {
        "en": "Discard whole file, because you sent wrong multiple price list: {}, allowed price on company is: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PLANOGRAM_IMPORT_DISCARD_FILE_WRONG_COLUMN_SENT = {
        "en": "Discard whole file, because you sent multiple price: {}, without column: {}, you must send column based on multiple price list",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_IMPORT_PRICE_VALUE = {
        "en": "Planogram with external_id {}, have {} that was not sent, so this price has same value as price_1",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_IMPORT_PRICE_WRONG_PRICE_VALUE = {
        "en": "Planogram with with external_id {}, have {} value that was sent, but multi price is: {}, discard this row!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_IMPORT_HEADER_GENERATOR_ERROR = {
        "en": "Error occurred on reading planogram header, please check your delimiter or check 'multiple_pricelists' field is sent",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_IMPORT_PRICE_WRONG_COLUMN_EXCEPTION = {
        "en": "Discard whole file, because you sent wrong column, error: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_IMPORT_PRICE_WRONG_COLUMN = {
        "en": "Discard whole file, you define multi price: {}, but you sent wrong column: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PLANOGRAM_WRONG_PRICE = {
        "en": "Product with with external_id {}, have {} that was not configured on company",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_MINIMUM_ROUTE_PICKUP_POSITIVE_NUMBER = {
        "en": "Minimum route pickup: {} have to be positive integers.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_IMPORT_THERE_IS_NO_PRODUCT = {
        "en": "There is no added products in company {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PLANOGRAM_IMPORT_FILL_RATE_ERROR = {
        "en": "Capacity must be greater or equal to fill rate, capacity: {} fill_rate: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PLANOGRAM_WARNING_FIELD_ERROR = {
        "en": "Column warning cannot be bigger than capacity, warning: {}, capacity: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_IMPORT_COLUMN_REPEAT = {
        "en": "Planogram name: {} have column repeat, column: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_NAME_IMPORT_REPEAT = {
        "en": "Planogram name: {} repeat, with different external_id: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_EXTERNAL_ID_IMPORT_REPEAT = {
        "en": "Planogram external_id: {} repeat, planogram_name: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_IMPORT_FILL_RATE = {
        "en": "Planogram name: {} have wrong fill_rate filed, fill_rate: {}, fill_rate can't be decimal or empty!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PLANOGRAM_IMPORT_FILL_RATE_NEGATIVE = {
        "en": "Planogram name: {} have wrong fill_rate filed, fill_rate: {}, fill_rate can't be negative number!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PLANOGRAM_IMPORT_CAPACITY_NEGATIVE = {
        "en": "Planogram name: {} have wrong capacity filed, capacity: {}, fill_rate can't be negative number!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_IMPORT_COLUMN_NUMBER_LIMIT = {
        "en": "Planogram: {}, column: {}, column_number field must be number 0 - 32767.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PLANOGRAM_COLUMN_ERROR = {
        "en": "Planogram column must be integer, planogram name: {}, column : {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    METER_READINGS_LIST_DUPLICATE = {
        "en": "Meter reading: {} is duplicated for machine: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    METER_READINGS_LIST_DOES_NOT_EXISTS = {
        "en": "Meter reading type: {} does not exists!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    METER_READINGS_LIST_COMPANY_HAS_NO_SETTING = {
        "en": "Company does not have meter readings turned on!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    PRODUCT_AND_ROTATION_GROUP_BOTH_PRESENT = {
        "en": "Row {} has both product and product rotation group present!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    INVALID_APP_RIGHTS = {
        "en": "service_staff_mobile_technical_view field can't be set to true "
              "if service_staff_mobile_app is set to false",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    APP_NOT_ALLOWED = {
        "en": "Mobile app is not allowed for users that don't have Filler role "
              "and have assign_filling_route field set to false.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    PLANOGRAM_ROWS_LIMIT_ERROR = {
        "en": "Maximum import rows: {}, you sent: {}, please remove rows starting from: {}, and try again!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    IMPORT_FAIL = {
        "en": "Import failed due to inconsistencies with database data.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    DATABASE_IMPORT_STATS = {
        "en": "Import result - Inserted: {}, Updated: {}, Deleted: {}, Error: {}",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    MACHINE_TYPE_IS_USED = {
        "en": "Cannot delete, machine type {} is used in machines",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    MACHINE_TYPE_IS_DEFAULT = {
        "en": "Machine type {} is set as Default and cannot be deleted. Move default flag to another Machine type to successfully delete this Machine type.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    MACHINE_TYPE_NAME_IS_USED = {
        "en": "Cannot insert machine type name {}, because this machine type already exists!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    NO_ROWS_TO_IMPORT = {
        "en": "There are no valid rows to import.",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }
    GEO_LOCATION = {
        "en": "Location: {}, has wrong geo location data, please provide 'latitude' and 'longitude' or 'location_address'!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    GEO_LOCATION_LATITUDE = {
        "en": "You send address: {} and latitude: {}, longitude is not sent, only address is valid for this case!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }

    GEO_LOCATION_LONGITUDE = {
        "en": "You send address: {} and longitude: {}, latitude is not sent, only address is valid for this case!",
        "de": "",
        "it": "",
        "fr": "",
        "process_type": EnumMessageDescription.CLOUD.name
    }


def enum_message_on_specific_language(enum_message_key, language, *args):
    """

    :param enum_message_key: enum message for translate
    :param language: language
    :param args: format arguments for message
    :return: message on specific language
    """
    try:
        check_language = enum_message_key.get(language, None)

        if not check_language or check_language == "":
            # default language is eng
            language = 'en'
    except:
        logger_api.error("Can't determine language")
    if args and None not in args:
        try:
            return enum_message_key[language].format(*args)
        except IndexError:
            logger_api.error("Wrong number of args for enum message!")
    else:
        return enum_message_key[language]


# -------- Flask Validation constants --------------------------
COMPANY_NOT_EXISTS = "Company does not exist: {}"


def elastic_not_allowed_status():
    status = [
        EnumErrorType.STARTED.name, EnumErrorType.IN_PROGRESS.name
    ]

    return status


def return_enum_error_name(id):
    for x in EnumErrorType:
        if int(x.value) == id:
            return x.name
