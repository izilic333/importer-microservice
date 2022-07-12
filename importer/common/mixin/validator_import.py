import common.mixin.enum_errors as const

import common.urls.urls as urls
from common.logging.setup import logger
import os.path

escape_list = ['.bashrc', '.bash_profile', '.bash_logout', '.bash_history']
logger_api = logger

# MASTER_DATA WORKING DIRECTORY
BASE_PATH = os.path.abspath(urls.load_log_basepath['importer'])
STORE_DIR = os.path.join(BASE_PATH, urls.DOWNLOAD_HISTORY_DIR)
WORKING_DIR = os.path.join(BASE_PATH, urls.WORK_DIR)
ZIP_WORKING_DIR = os.path.join(BASE_PATH, urls.ZIP_WORK_DIR)
HISTORY_FILES_DIR = os.path.join(BASE_PATH, urls.HISTORY_MAIN_DIR)
HISTORY_SUCCESS_DIR = os.path.join(BASE_PATH, urls.HISTORY_SUCCESS_DIR)
HISTORY_FAIL_DIR = os.path.join(BASE_PATH, urls.HISTORY_FAIL_DIR)


# VENDS WORKING DIRECTORY
VEND_BASE_PATH = os.path.abspath(urls.load_log_basepath['importer'])
DOWNLOAD_VENDS_DIR = os.path.join(VEND_BASE_PATH, urls.VEND_DOWNLOAD_HISTORY_DIR)
VENDS_WORKING_DIR = os.path.join(VEND_BASE_PATH, urls.VEND_WORK_DIR)
VENDS_ZIP_WORKING_DIR = os.path.join(VEND_BASE_PATH, urls.VEND_ZIP_WORK_DIR)
VENDS_HISTORY_FILES_DIR = os.path.join(VEND_BASE_PATH, urls.VEND_HISTORY_MAIN_DIR)
VENDS_HISTORY_SUCCESS_DIR = os.path.join(VEND_BASE_PATH, urls.VEND_HISTORY_SUCCESS_DIR)
VENDS_FAIL_DIR = os.path.join(VEND_BASE_PATH, urls.VEND_HISTORY_FAIL_DIR)

"""
    Check dir status before running
"""


def create_if_doesnt_exist(path):
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except OSError as e:
            logger_api.info(const.CREATE_DIR_ERROR.format(e))


# create master_data working dirs
create_if_doesnt_exist(os.path.join(BASE_PATH, urls.MAIN_DIR))
create_if_doesnt_exist(WORKING_DIR)
create_if_doesnt_exist(HISTORY_FILES_DIR)
create_if_doesnt_exist(HISTORY_SUCCESS_DIR)
create_if_doesnt_exist(HISTORY_FAIL_DIR)
create_if_doesnt_exist(ZIP_WORKING_DIR)
create_if_doesnt_exist(STORE_DIR)

# create vend working dirs
create_if_doesnt_exist(os.path.join(VEND_BASE_PATH, urls.MAIN_VEND_DIR))
create_if_doesnt_exist(VENDS_WORKING_DIR)
create_if_doesnt_exist(VENDS_HISTORY_FILES_DIR)
create_if_doesnt_exist(VENDS_HISTORY_SUCCESS_DIR)
create_if_doesnt_exist(VENDS_FAIL_DIR)
create_if_doesnt_exist(VENDS_ZIP_WORKING_DIR)
create_if_doesnt_exist(DOWNLOAD_VENDS_DIR)
