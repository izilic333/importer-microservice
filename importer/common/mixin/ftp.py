import re
import os
import ftplib
import os.path
import ftputil
import paramiko
import datetime
import traceback
from datetime import datetime
from common.logging.setup import logger
from common.mixin.enum_errors import EnumErrorType
from common.mixin.validator_import import escape_list
from common.mixin.enum_errors import EnumValidationMessage as enum_msg
from common.mixin.validation_const import ALLOWED_EXTENSIONS
from database.company_database.core.query_history import OldDevicePidHistory

logger_api = logger


class MySession(ftplib.FTP):
    """
    Make connection on FTP, except any port if FTP listening this port (if FTP is configured on
    that specific port)
    """
    def __init__(self, host, username, password, port):
        """Act like ftplib.FTP's constructor but connect to another port."""
        ftplib.FTP.__init__(self)
        self.connect(host, port)
        self.login(username, password)


def sftp_download_file(hostname, username,port, password, path, store_dir, emails, process_logger):
    """
    :param hostname: FTP hostname/IP address
    :param username: FTP username
    :param port: only 22 port
    :param password: FTP password
    :param path: FTP path
    :param store_dir: local directory for downloaded file
    :param emails: email
    :param process_logger: elastic search logging class, example:  ProcessLogger(company_id,
    import_type_execute, elastic_hash, user_token
    :return: dict with success status and list of downloaded files with last modification time
    """

    # This is only for port 22, sftp

    try:
        transport = paramiko.Transport(hostname, port)
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        logger_api.info(process_logger.update_system_log_flow(
            username, hostname, port,
            key_enum=enum_msg.FTP_CONNECTED.value)
        )

    except Exception:
        process_logger.create_process_and_cloud_flow_and_main(
            traceback.print_exc(),
            error=EnumErrorType.ERROR,
            file_path='',
            email=emails,
            language='en',
            key_enum=enum_msg.FTP_CONNECTION_ERROR.value
        )

        return {"success": False}
    try:
        sftp.chdir(path=path)
        logger_api.info(process_logger.update_system_log_flow(
            path,
            key_enum=enum_msg.FILE_RIGHT_PATH.value)
        )
    except Exception:
        process_logger.create_process_and_cloud_flow_and_main(
            traceback.print_exc(),
            error=EnumErrorType.ERROR,
            file_path='',
            email=emails,
            language='en',
            key_enum=enum_msg.FTP_PATH_ERROR.value
        )
        sftp.close()
        transport.close()
        return {"success": False}

    downloaded_files = []

    file_names = sftp.listdir()

    logger_api.info(process_logger.update_system_log_flow(
        file_names,
        key_enum=enum_msg.FTP_FILE_LIST.value)
    )

    for file_name in file_names:
        try:
            extension = os.path.splitext(file_name)[1]
            time_file = datetime.fromtimestamp(
                sftp.stat(os.path.join(path, file_name)).st_mtime).strftime('%Y-%m-%d %H:%M:%S')
            local_path = os.path.join(store_dir, file_name)

            logger_api.info(process_logger.update_system_log_flow(
                file_name,
                key_enum=enum_msg.FTP_START_DOWNLOAD_FILE.value)
            )

            sftp.get(file_name, local_path)

            new_filename_without_space = os.path.join(
                store_dir, re.sub('\s+', '_', file_name).strip())
            os.rename(os.path.join(store_dir, file_name),
                      new_filename_without_space)

            logger_api.info(process_logger.update_system_log_flow(
                file_name, new_filename_without_space,
                key_enum=enum_msg.FTP_DOWNLOADED_AS.value)
            )

            try:
                if extension not in ALLOWED_EXTENSIONS:
                    process_logger.update_process_and_cloud_flow(
                        file_name,
                        error=EnumErrorType.FAIL.name,
                        language='en',
                        key_enum=enum_msg.FILE_WRONG_FORMAT.value,
                    )

                    if os.path.isfile(os.path.join(store_dir, new_filename_without_space)):
                        os.remove(os.path.join(store_dir, new_filename_without_space))
                else:
                    downloaded_files.append(
                        {
                            'name': new_filename_without_space,
                            'time': time_file
                        }
                    )
                # In this part we except that file is success downloaded, so in this case we can delete this
                # processed file from FTP!
                try:
                    sftp.remove(os.path.join(path, file_name))
                    logger_api.info(process_logger.update_system_log_flow(
                        file_name,
                        key_enum=enum_msg.FTP_DELETED_FILE.value)
                    )
                except Exception:
                    logger_api.info(process_logger.update_system_log_flow(
                        traceback.print_exc(),
                        key_enum=enum_msg.VALIDATION_SYSTEM_LOG_ERROR_REMOVING_FTP_FILE.value)
                    )
            except Exception:
                logger_api.info(process_logger.update_system_log_flow(
                    hostname, new_filename_without_space,
                    key_enum=enum_msg.VALIDATION_SYSTEM_LOG_WRONG_FORMAT_FTP.value)
                )

                pass

        except Exception as e:
            logger_api.error("Error downloading {} -> {}".format(file_name, str(e)))
            logger_api.info(process_logger.update_process_and_cloud_flow(
                file_name,
                error=EnumErrorType.ERROR.name,
                language='en',
                key_enum=enum_msg.FTP_DOWNLOAD_ERROR.value,
            ))

    sftp.close()
    transport.close()
    return { "success": True, "file_list": downloaded_files }


def sftp_download_and_vends_logger(hostname, username, port, password, path, store_dir, process_logger, import_type,
                                   elastic_hash=None, company_id=None):
    """
    :param hostname: FTP hostname/IP address
    :param username: FTP username
    :param port: only 22 port
    :param password: FTP password
    :param path: FTP path
    :param import_type: vend import type
    :param store_dir: local directory for downloaded file
    :param company_id: company_id
    :param elastic_hash: elastic_hash: elastic hash for vends process logger
    :param process_logger: elastic search logger
    :return: list of downloaded file
    """

    try:
        transport = paramiko.Transport(hostname, port)
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
    except Exception as e:
        process_logger.update_general_process_flow(
            status=EnumErrorType.ERROR.name,
            language='en',
            key_enum=enum_msg.FTP_CONNECTION_ERROR.value,
            elastic_hash=elastic_hash
        )
        process_logger.update_system_log_flow(
            e, key_enum=enum_msg.FTP_CONNECTION_ERROR_SYSTEM_LOG.value,
            logs_level=EnumErrorType.ERROR.name
        )
        process_logger.update_main_elastic_process(
            file_path="",
            elastic_hash=elastic_hash,
            data_hash="",
        )
        return False

    try:
        sftp.chdir(path=path)
        process_logger.update_system_log_flow(path, key_enum=enum_msg.FILE_RIGHT_PATH.value,
                                              logs_level=EnumErrorType.IN_PROGRESS.name)
    except IOError as e:

        process_logger.update_general_process_flow(
            path,
            status=EnumErrorType.ERROR.name,
            language='en',
            key_enum=enum_msg.FTP_PATH_ERROR.value,
            elastic_hash=elastic_hash)
        process_logger.update_system_log_flow(
            hostname, e,
            key_enum=enum_msg.FTP_PATH_ERROR_SYSTEM_LOG.value,
            logs_level=EnumErrorType.ERROR.name
        )
        process_logger.update_main_elastic_process(
            elastic_hash=elastic_hash,
            data_hash="",
            file_path="",
        )
        return False

    downloaded_files = []
    file_names = sftp.listdir()

    for file_name in file_names:
        try:
            time_file = datetime.fromtimestamp(sftp.stat(os.path.join(path, file_name)).st_mtime).strftime(
                '%Y-%m-%d %H:%M:%S'
            )
            local_path = os.path.join(store_dir, file_name)
            already_process_file = OldDevicePidHistory.check_is_file_already_processing(
                file_name, company_id, import_type)

            if file_name != already_process_file:
                sftp.get(file_name, local_path)
                process_logger.update_system_log_flow(
                    file_name, key_enum=enum_msg.FTP_START_DOWNLOAD_FILE.value,
                    logs_level=EnumErrorType.IN_PROGRESS.name)

                downloaded_files.append({'name': file_name, 'time': time_file})

        except Exception:
            process_logger.update_general_process_flow(
                file_name,
                status=EnumErrorType.ERROR.name,
                language='en',
                key_enum=enum_msg.FTP_DOWNLOAD_ERROR.value,
                elastic_hash=elastic_hash
            )
    sftp.close()
    transport.close()
    return downloaded_files


def ftp_download(port, host, username, password, path, general_process_logger, elastic_hash, company_id, import_type,
                 emails, store_dir):
    """
    This method download files from FTP, and detect if file already processed!
    In this step method check in local database archive if file already processed!
    :return: sorted_files_list (list of downloaded files)
    """

    downloaded_files = []
    if port != 22:

        # Connect to ftp server!
        try:
            ftp_connect = ftputil.FTPHost(host, username, password, port, session_factory=MySession)
        except Exception as e:
            general_process_logger.update_general_process_flow(
                company_id, import_type,
                status=EnumErrorType.ERROR.name,
                language='en',
                key_enum=enum_msg.FTP_CONNECTION_ERROR.value,
                elastic_hash=elastic_hash
            )
            general_process_logger.update_system_log_flow(
                company_id, import_type, e,
                key_enum=enum_msg.FTP_CONNECTION_ERROR_SYSTEM_LOG.value,
                logs_level=EnumErrorType.ERROR.name
            )
            general_process_logger.update_main_elastic_process(
                elastic_hash=elastic_hash,
                data_hash="",
                file_path="",
            )
            return {"success": False}

        # Change remote FTP dir!
        with ftp_connect as ftp_host:
            try:
                ftp_host.chdir(path)
            except Exception as e:
                general_process_logger.update_general_process_flow(
                    path,
                    status=EnumErrorType.ERROR.name,
                    language='en',
                    key_enum=enum_msg.FTP_PATH_ERROR.value,
                    elastic_hash=elastic_hash
                )
                general_process_logger.update_system_log_flow(
                    host, company_id, import_type, e,
                    key_enum=enum_msg.FTP_PATH_ERROR_SYSTEM_LOG.value,
                    logs_level=EnumErrorType.ERROR.name
                )
                return {"success": False}

            # Download FTP file!
            ftp_files_for_check = []
            try:
                ftp_files = ftp_host.listdir(ftp_host.curdir)
                for file in ftp_files:
                    if ftp_host.path.isfile(file) and file not in escape_list:
                        if file not in ftp_files_for_check:
                            ftp_files_for_check.append(file)
            except Exception as e:
                general_process_logger.update_system_log_flow(
                    host, company_id, import_type, e,
                    key_enum=enum_msg.FTP_LIST_REMOTE_DIR_SYSTEM_LOG.value,
                    logs_level=EnumErrorType.ERROR.name
                )
            for ftp_file in ftp_files_for_check:
                already_process_file = OldDevicePidHistory.check_is_file_already_processing(
                    ftp_file, company_id, import_type)
                if ftp_file != already_process_file:
                    try:
                        ftp_host.download(ftp_file, os.path.join(store_dir, ftp_file))

                        time_file = datetime.fromtimestamp(
                            float(ftp_host.path.getmtime(ftp_file))
                        ).strftime('%Y-%m-%d %H:%M:%S')

                        downloaded_files.append(
                            {
                                'name': ftp_file,
                                'time': time_file
                            }
                        )
                    except Exception as e:
                        general_process_logger.update_general_process_flow(
                            ftp_file,
                            status=EnumErrorType.ERROR.name,
                            language='en',
                            key_enum=enum_msg.FTP_DOWNLOAD_ERROR.value,
                            elastic_hash=elastic_hash
                        )
                        general_process_logger.update_system_log_flow(
                            e, key_enum=enum_msg.FTP_DOWNLOAD_ERROR.value,
                            logs_level=EnumErrorType.ERROR.name
                        )
                else:
                    general_process_logger.update_system_log_flow(
                        host, company_id, import_type, ftp_file,
                        key_enum=enum_msg.FTP_FILE_ALREADY_PROCESSED.value,
                        logs_level=EnumErrorType.WARNING.name
                    )
        return downloaded_files
