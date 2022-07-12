import logging
import os
from logging.handlers import TimedRotatingFileHandler

from common.urls.urls import load_log_basepath

"""

    Logger main script. 
    Script will provide logging for all interfaces :
    INFO, WARN, ERROR, DEBUG

"""
########################################################################################################################
#                                             Master data log file path

master_data_basepath = os.path.abspath(load_log_basepath['master_data_basepath'])

if not os.path.exists(master_data_basepath):
    os.makedirs(master_data_basepath)

master_data_debug = os.path.abspath(os.path.join(master_data_basepath, "debug.log"))
master_data_info = os.path.abspath(os.path.join(master_data_basepath,  "info.log"))
master_data_error = os.path.abspath(os.path.join(master_data_basepath, "error.log"))

########################################################################################################################
#                                              Vend logs file path

vends_basepath = load_log_basepath['vends_basepath']

if not os.path.exists(vends_basepath):
    os.makedirs(vends_basepath)

vends_basepath_debug = os.path.abspath(os.path.join(vends_basepath, "debug.log"))
vends_basepath_info = os.path.abspath(os.path.join(vends_basepath, "info.log"))
vends_basepath_error = os.path.abspath(os.path.join(vends_basepath, "error.log"))

########################################################################################################################
#                                           FTP working dirs

importer_working_dir = os.path.abspath(load_log_basepath['importer'])
save_flask_files = os.path.abspath(os.path.join(importer_working_dir, "ftp_files/history_files/download_history"))
export_csv_path = os.path.abspath(os.path.join(importer_working_dir, "exports/"))
export_email_path = os.path.abspath(os.path.join(importer_working_dir, "email_exports/"))

logging.basicConfig(datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("application")

logger.setLevel(logging.DEBUG)
logger.propagate = False

sh = logging.StreamHandler()
sh.setLevel(logging.ERROR)

sh.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s CONSOLE %(message)s'))
logger.addHandler(sh)

########################################################################################################################
#                                           Define master data log

# INFO LOGGER
fh_info = TimedRotatingFileHandler(master_data_info, backupCount=5)
fh_info.setLevel(logging.INFO)
fh_info.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s  %(message)s'))
logger.addHandler(fh_info)

# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# ERROR LOGGER
fh_error = TimedRotatingFileHandler(master_data_error, backupCount=5)
fh_error.setLevel(logging.ERROR)
fh_error.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s'))
logger.addHandler(fh_error)

# DEBUG LOGGER
fh_debug = TimedRotatingFileHandler(master_data_debug, backupCount=5)
fh_debug.setLevel(logging.DEBUG)
fh_debug.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s  %(message)s'))
logger.addHandler(fh_debug)

########################################################################################################################
#                                          Define vend logs

vend_logger = logging.getLogger('vend_system_logger')
vend_logger.setLevel(logging.DEBUG)
vend_logger.propagate = False

vend_sh = logging.StreamHandler()
vend_sh.setLevel(logging.ERROR)

vend_sh.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s CONSOLE %(message)s'))
vend_logger.addHandler(vend_sh)

# VEND INFO LOGGER
vend_fh_info = TimedRotatingFileHandler(vends_basepath_info, backupCount=7)
vend_fh_info.setLevel(logging.INFO)
vend_fh_info.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s  %(message)s'))
vend_logger.addHandler(vend_fh_info)

# VEND ERROR LOGGER
vend_fh_error = TimedRotatingFileHandler(vends_basepath_error, backupCount=7)
vend_fh_error.setLevel(logging.ERROR)
vend_fh_error.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s'))
vend_logger.addHandler(vend_fh_error)

# VEND DEBUG LOGGER
vend_fh_debug = TimedRotatingFileHandler(vends_basepath_debug, backupCount=1)
vend_fh_debug.setLevel(logging.DEBUG)
vend_fh_debug.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s  %(message)s'))
vend_logger.addHandler(vend_fh_debug)
