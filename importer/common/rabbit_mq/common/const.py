from enum import Enum


class ValidationEnum(Enum):
    VALIDATION_Q = "validator_message"
    VALIDATION_Q_KEY = "importer_file"
    DATABASE_Q = "database_message"
    DATABASE_Q_KEY = "database_json"
    VALIDATION_FILE_API_Q = "validator_api"
    VALIDATION_Q_FILE_KEY = "validator_api_json"

    CUSTOM_PUBLISHER = "custom_q"
    CUSTOM_Q_PUBLISHER = "custom_q_publisher"

    EXPORT_PUBLISHER = "export_publisher"
    EXPORT_Q_PUBLISHER = "export_q_publisher"

    # <--------------------  VENDS Q ----------------------------------->
    # Download vends content Q
    VEND_DOWNLOADER_FILE_Q = "vend_downloader_file"
    VEND_DOWNLOADER_FILE_Q_KEY = "vend_downloader_file_key"

    VEND_DOWNLOADER_API_Q = "vend_downloader_api"
    VEND_DOWNLOADER_API_Q_KEY = "vend_downloader_api_key"

    # Cloud validator Q
    VEND_VALIDATION_FILE_Q = "vend_validator_file"
    VEND_VALIDATION_FILE_Q_KEY = "importer_vend_validator_file"

    DEX_VALIDATION_FILE_Q = "dex_validator_file"
    DEX_VALIDATION_FILE_Q_KEY = "importer_dex_vend_validator_file"

    VEND_VALIDATION_API_Q = "vend_validator_api"
    VEND_VALIDATION_API_Q_KEY = "importer_vend_validator_api"

    # Cloud Q
    VEND_DATABASE_Q = "vend_database_data"
    VEND_DATABASE_Q_KEY = "import_vend_database"
    # <-------------------- END VENDS Q --------------------------------->

