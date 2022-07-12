import re
import os
import shutil
import itertools
from time import gmtime, strftime
from common.mixin.mixin import generate_hash_for_json
from common.mixin.validation_const import MainImportType
from common.mixin.ftp import ftp_download, sftp_download_and_vends_logger
from database.cloud_database.core.query import CustomUserQueryOnCloud, DeviceQueryOnCloud
from common.mixin.handle_file import MainVendsHelpers, get_import_type
from database.company_database.core.query_export import ExportHistory
from database.company_database.core.query_history import OldDevicePidHistory
from common.rabbit_mq.validator_file_q.vend_processing_publisher import publish_vend_file_processing
from common.mixin.validator_import import DOWNLOAD_VENDS_DIR, VENDS_WORKING_DIR, create_if_doesnt_exist
from common.rabbit_mq.vend_downloader_q.vend_file_download_publisher import vend_file_download_publisher
from common.mixin.vends_mixin import (VENDS_INITIAL_INFO, MainVendProcessLogger, new_device_pid_detect_operation,
                                      handle_insert_device_pid_local_history, create_elastic_hash, get_device_object,
                                      CleanLocalHistory, generate_elastic_login_for_group_file)
from common.mixin.enum_errors import (EnumValidationMessage as MainMessage, EnumErrorType,
                                      enum_message_on_specific_language)


class GetRemoteVendData(object):
    """
    This class download files from FTP or get vends content from API, and send this files(FTP)/vends content(API) on
    processing!
    """
    def __init__(self, data):
        self.user_token = CustomUserQueryOnCloud.get_auth_token_from_user_id(int(data['user_id']))
        self.import_type_execute = int(data['category_import'])
        self.import_type = get_import_type(self.import_type_execute)
        self.port = int(data['port']) if data['port'] else 21
        self.delimiter = data['file_delimiters']
        self.token = self.user_token['token']
        self.company_id = data['company']
        self.username = data['username']
        self.password = data['password']
        self.path = data['ftp_path']
        self.emails = data['email']
        self.host = data['url']
        self.language = "en"
        self.cpi_processor = 'CPI_VENDS'
        self.dex_processor = 'DEX_VENDS'
        self.store_dir = os.path.join(DOWNLOAD_VENDS_DIR, self.import_type, str(self.company_id))
        self.elastic_hash = create_elastic_hash(company_id=self.company_id, import_type=self.import_type,
                                                import_request_type='FILE')

        self.sorted_files_list = []
        self.general_process_logger = MainVendProcessLogger(
            company_id=self.company_id,
            import_type=self.import_type,
            process_request_type='FILE',
            token=self.token
        )

    def get_remote_file(self):
        if not self.elastic_hash:
            return
        create_if_doesnt_exist(self.store_dir)
        downloaded_files = []

        if self.port != 22:
            downloaded_files = ftp_download(
                port=self.port,
                host=self.host,
                username=self.username,
                password=self.password,
                path=self.path,
                general_process_logger=self.general_process_logger,
                elastic_hash=self.elastic_hash,
                emails=self.emails,
                company_id=self.company_id,
                import_type=MainImportType.vends.name,
                store_dir=self.store_dir
            )

        elif self.port == 22:
            sftp_download = sftp_download_and_vends_logger(
                hostname=self.host,
                username=self.username,
                port=22,
                password=self.password,
                path=self.path,
                store_dir=self.store_dir,
                process_logger=self.general_process_logger,
                elastic_hash=self.elastic_hash,
                company_id=self.company_id,
                import_type=self.import_type
            )

            if sftp_download:
                for item in sftp_download:
                    filename = os.path.basename(item.get('name'))
                    filename_time = os.path.basename(item.get('time'))
                    downloaded_files.append({
                        'name': filename,
                        'time': filename_time
                    })

        if downloaded_files:
            self.sorted_files_list = sorted(downloaded_files, key=lambda k: k["time"])

            if len(self.sorted_files_list):
                list_of_working_files = []
                for file in self.sorted_files_list:
                    list_of_working_files.append(file.get('name'))
                self.general_process_logger.update_importer_process_flow(
                    len(list_of_working_files),
                    status=EnumErrorType.IN_PROGRESS.name,
                    key_enum=MainMessage.FTP_NUMBER_SUCCESSFULLY_DOWNLOADED_FILE.value,
                    elastic_hash=self.elastic_hash,
                )

                self.general_process_logger.update_system_log_flow(
                    list_of_working_files,
                    key_enum=MainMessage.FTP_SUCCESSFULLY_DOWNLOAD_FILE.value,
                    logs_level=EnumErrorType.IN_PROGRESS.name
                )

            return self.sorted_files_list

    def processing_ftp_files(self, processor_type):
        """
        This method make grouping device pid and sorting based on file timestamp!
        It's very important the sequence/order of file processing, ordering must be based on file timestamp, so this
        method send on processing one group of device pid after that the next group of pids, etc ...
        This method every file from downloaded list send on processing!
        """
        # Basic operation on filename (generate file timestamp, device pid and real filename for processing)
        order_processing_file = []
        working_files = []
        filename_regex = False
        datetime_format = False
        if processor_type == self.cpi_processor:
            filename_regex = VENDS_INITIAL_INFO['cpi_import']['file_regex']
            datetime_format = VENDS_INITIAL_INFO['cpi_import']['vends']
        elif processor_type == self.dex_processor:
            filename_regex = VENDS_INITIAL_INFO['dex_import']['file_regex']
            datetime_format = VENDS_INITIAL_INFO['dex_import']['vends']

        if filename_regex and datetime_format:
            for file in self.sorted_files_list:
                if os.path.isfile(os.path.join(self.store_dir, file['name'])):
                    extension = MainVendsHelpers.get_file_extension(file['name'])
                    if extension == '.zip':
                        from datetime import datetime
                        device_pid, file_time = re.match(filename_regex, file['name']).groups()
                        import_datetime = datetime.strptime(file_time, datetime_format)
                        working_files.append({
                            'device_pid': device_pid,
                            'file_timestamp': import_datetime,
                            'filename': file['name']
                        })

        # Create groups of device pid and make sorting, for this pid group, based on timestamp of file
        general_file_sorting = []
        if len(working_files):
            sorted_device_pid = sorted(working_files, key=lambda k: k["device_pid"])
            group_by_pid = {
                k: [v for v in sorted_device_pid if v['device_pid'] == k]
                for k, val in itertools.groupby(sorted_device_pid, lambda x: x['device_pid'])
            }

            for k, v in group_by_pid.items():
                timestamp_sort = sorted(v, key=lambda k: k["file_timestamp"])
                general_file_sorting.append({k: timestamp_sort})

        # In this part we have correct order based on device pid groups and timestamp of file
        for file_dict in general_file_sorting:
            for k, v in file_dict.items():
                for elements in v:
                    processing_file = os.path.join(self.store_dir, elements['filename'])
                    order_processing_file.append(processing_file)

        # Publish downloaded file in Queue
        vend_file_download_publisher(company_id=self.company_id, process_request_type="FILE",
                                     elastic_hash=self.elastic_hash, email=self.emails,
                                     vend_type=self.import_type, token=self.token, language=self.language,
                                     file_path=order_processing_file)


class ProcessingVendFile(object):
    """
    Some basic description for this class:
        * make fail/success dirs archive based on filename timestamp
        * extract zip file and make basic validation on file extension in archive
    """

    def __init__(self, data):
        self.company_id = data['company_id']
        self.email = data['email']
        self.import_type = data['type']
        self.process_request_type = data['process_request_type']
        self.file_path_list = data['file_path']
        self.elastic_hash = data['elastic_hash']
        self.token = data['token']
        self.process_logger = MainVendProcessLogger(
            company_id=self.company_id,
            import_type=self.import_type,
            process_request_type='FILE',
            token=self.token
        )
        self.cpi_processor = 'CPI_VENDS'
        self.dex_processor = 'DEX_VENDS'
        self.working_directory = os.path.join(VENDS_WORKING_DIR, self.import_type, str(self.company_id))
        create_if_doesnt_exist(self.working_directory)

    def processing_vends_ftp_file(self):
        """
        This method is core of vend processing based on file from FTP!

        This is main method of processing vends data, some description of this method is described bellow:
            * handle only zip extension (because in zip filename we have timestamp and device pid)
            * handle only .eva files (because if .dex file detected in zip archive it will be renamed to .eva file)
                - new_eva_filename = DevicePid_year_month_day_hour_minute_second.eva
            * detect if we have new pid for import or if this pid already imported and then detect if we have new
                content for import
            * check if sent eva file is empty
            * parse eva field, and save it into local database archive (JSON field)
            * generate eva content for three cases
                - detect new eva (new device pid, new eva content)
                - detect newest eva (old device pid - pid that already previous imported, new data content)
            * make local archive for every above steps

        :return: send old and new eva to processing
        """
        process_request_type = self.process_request_type if self.process_request_type else 'CLOUD'
        clean_local_archive = CleanLocalHistory(process_logger=self.process_logger)
        datetime_format = False
        filename_regex = False
        if self.cpi_processor == self.import_type:
            filename_regex = VENDS_INITIAL_INFO['cpi_import']['file_regex']
            datetime_format = VENDS_INITIAL_INFO['cpi_import']['vends']
        elif self.dex_processor == self.import_type:
            filename_regex = VENDS_INITIAL_INFO['dex_import']['file_regex']
            datetime_format = VENDS_INITIAL_INFO['dex_import']['vends']
        allowed_extension = ['.rsp', '.dex', '.zip', '.eva']
        processing_vend_files = []
        zip_extension = ['.zip']
        main_fail_dir = []
        helper_methods_validations = False
        if datetime_format and filename_regex:
            helper_methods_validations = MainVendsHelpers(
                import_type=self.import_type, company_id=self.company_id, token=self.token, regex=filename_regex,
                datetime_format=datetime_format, process_logger=self.process_logger, elastic_hash=self.elastic_hash
            )
        iteration_time = strftime("%Y-%m-%d %H:%M", gmtime())
        # Iterate through list of zip file of cron job
        for zip_path in self.file_path_list:
            filename_info = helper_methods_validations.get_file_info(zip_path, self.import_type)
            device_pid = filename_info['device_pid']
            file_timestamp = filename_info['import_datetime_timestamp']
            fail_dir = filename_info['fail_dir']
            success_dir = filename_info['success_dir']
            extension = helper_methods_validations.get_file_extension(zip_path)
            zip_filename = os.path.basename(zip_path)
            fail_zip_path = os.path.join(fail_dir, zip_filename)
            if fail_dir not in main_fail_dir:
                main_fail_dir.append(fail_dir)
            if extension in zip_extension:
                # This is extracted zip file content
                zip_file_data = helper_methods_validations.zip_file_handler(zip_file_path=zip_path)

                if zip_file_data:
                    all_extensions_per_file = []

                    # Check all extension in extracted file
                    for file in zip_file_data:
                        ext = helper_methods_validations.get_file_extension(file)
                        all_extensions_per_file.append(ext)

                    # This is core procedure for every extracted file
                    for working_file_path in zip_file_data:
                        work_filename = os.path.basename(working_file_path)
                        extension = helper_methods_validations.get_file_extension(working_file_path)
                        # Eliminate not allowed extension
                        fail_path = os.path.join(fail_dir, work_filename)

                        if extension not in allowed_extension:
                            shutil.copy2(working_file_path, fail_dir)
                            sub_elastic_hash = create_elastic_hash(
                                company_id=self.company_id, import_type=self.import_type,
                                import_request_type=process_request_type
                            )

                            self.process_logger.update_importer_process_flow(
                                work_filename, sub_elastic_hash,
                                status=EnumErrorType.WARNING.name,
                                key_enum=MainMessage.VEND_PROCESSED_WORKING_EXTENSION_SUB_MESSAGE.value,
                                elastic_hash=self.elastic_hash
                            )
                            MainVendProcessLogger.update_system_log_flow(
                                work_filename, extension,
                                key_enum=MainMessage.VEND_PROCESSED_WORKING_EXTENSION.value,
                                logs_level=EnumErrorType.ERROR.name
                            )
                            self.process_logger.update_general_process_flow(
                                work_filename, extension,
                                status=EnumErrorType.ERROR.name,
                                key_enum=MainMessage.VEND_PROCESSED_WORKING_EXTENSION.value,
                                elastic_hash=sub_elastic_hash
                            )

                            self.process_logger.update_main_elastic_process(
                                file_path=fail_path,
                                elastic_hash=sub_elastic_hash,
                                data_hash="",
                                main_elastic_hash=self.elastic_hash
                            )
                            clean_local_archive.delete_local_working_file(working_file_path)
                            continue

                        if extension in ['.eva']:
                            if os.path.getsize(working_file_path) == 0:
                                shutil.copy2(working_file_path, fail_dir)
                                sub_elastic_hash = create_elastic_hash(company_id=self.company_id,
                                                                       import_type=self.import_type,
                                                                       import_request_type=process_request_type)

                                self.process_logger.update_importer_process_flow(
                                    work_filename, sub_elastic_hash,
                                    status=EnumErrorType.WARNING.name,
                                    key_enum=MainMessage.VEND_IMPORTER_EMPTY_WORKING_FILE_SUB_MESSAGE.value,
                                    elastic_hash=self.elastic_hash
                                )
                                self.process_logger.update_system_log_flow(
                                    work_filename,
                                    key_enum=MainMessage.VEND_IMPORTER_EMPTY_WORKING_FILE.value,
                                    logs_level=EnumErrorType.ERROR.name
                                )
                                self.process_logger.update_general_process_flow(
                                    work_filename,
                                    status=EnumErrorType.ERROR.name,
                                    key_enum=MainMessage.VEND_IMPORTER_EMPTY_WORKING_FILE.value,
                                    elastic_hash=sub_elastic_hash
                                )
                                self.process_logger.update_main_elastic_process(
                                    file_path=fail_path,
                                    elastic_hash=sub_elastic_hash,
                                    data_hash="",
                                    main_elastic_hash=self.elastic_hash
                                )
                                clean_local_archive.delete_local_working_file(working_file_path)
                                continue

                            # Generate main working eva structure
                            processing_vend_files.append({
                                'file': work_filename,
                                'fail_dir': fail_dir,
                                'success_dir': success_dir,
                                'file_path': working_file_path,
                                'device_pid': device_pid,
                                'import_datetime': file_timestamp,
                                'file_data_and_info': {
                                    'cash_collection_status': True if '.rsp' in all_extensions_per_file else False,
                                },
                                'zip_filename': os.path.basename(zip_path),
                            })
                        # If file is not eva
                        else:
                            shutil.copy2(working_file_path, fail_dir)
                            sub_elastic_hash = create_elastic_hash(company_id=self.company_id,
                                                                   import_type=self.import_type,
                                                                   import_request_type=process_request_type)

                            self.process_logger.update_importer_process_flow(
                                work_filename, sub_elastic_hash,
                                status=EnumErrorType.WARNING.name,
                                key_enum=MainMessage.VEND_IMPORTER_WORKING_FILE_SUB_MESSAGE.value,
                                elastic_hash=self.elastic_hash
                            )
                            self.process_logger.update_system_log_flow(
                                work_filename,
                                key_enum=MainMessage.VEND_IMPORTER_WORKING_FILE.value,
                                logs_level=EnumErrorType.FAIL.name
                            )

                            self.process_logger.update_general_process_flow(
                                work_filename,
                                status=EnumErrorType.ERROR.name,
                                key_enum=MainMessage.VEND_IMPORTER_WORKING_FILE.value,
                                elastic_hash=sub_elastic_hash
                            )
                            self.process_logger.update_main_elastic_process(
                                file_path=fail_path,
                                elastic_hash=sub_elastic_hash,
                                data_hash="",
                                main_elastic_hash=self.elastic_hash
                            )
                            clean_local_archive.delete_local_working_file(working_file_path)
                            continue

            # If extension is not zip initial working file
            else:
                shutil.copy2(working_file_path, fail_dir)
                sub_elastic_hash = create_elastic_hash(company_id=self.company_id,
                                                       import_type=self.import_type,
                                                       import_request_type=process_request_type)

                self.process_logger.update_importer_process_flow(
                    zip_filename, sub_elastic_hash,
                    status=EnumErrorType.WARNING.name,
                    key_enum=MainMessage.VEND_PROCESSED_EXTENSION_SUB_MESSAGE.value,
                    elastic_hash=sub_elastic_hash
                )
                self.process_logger.update_system_log_flow(
                    zip_filename, '.zip',
                    key_enum=MainMessage.VEND_PROCESSED_EXTENSION.value,
                    logs_level=EnumErrorType.ERROR.name
                )
                self.process_logger.update_general_process_flow(
                    zip_filename, '.zip',
                    status=EnumErrorType.ERROR.name,
                    key_enum=MainMessage.VEND_PROCESSED_EXTENSION.value,
                    elastic_hash=sub_elastic_hash
                )
                self.process_logger.update_main_elastic_process(
                    file_path=fail_zip_path,
                    elastic_hash=sub_elastic_hash,
                    data_hash="",
                    main_elastic_hash=self.elastic_hash
                )
                clean_local_archive.delete_local_working_file(working_file_path)
                continue

        iteration_eva_processing = {}
        device_pid_without_machine = []
        file_without_machine = []
        error_during_open_file = []

        if len(processing_vend_files):
            for vends_file_dict in processing_vend_files:
                # Generate working structure for every eva file
                data_hash = generate_hash_for_json(vends_file_dict['file'])
                generate_fail_path = os.path.join(vends_file_dict['fail_dir'], vends_file_dict['file'])
                if self.cpi_processor == self.import_type:
                    device = get_device_object(device_pid=vends_file_dict['device_pid'], company_id=self.company_id)
                    if device:
                        machine_id = device.get('machine_id')
                    else:
                        if vends_file_dict['device_pid'] not in device_pid_without_machine:
                            device_pid_without_machine.append(vends_file_dict['device_pid'])

                        eva_file = os.path.basename(vends_file_dict['file_path'])
                        if eva_file not in file_without_machine:
                            file_without_machine.append(eva_file)
                        machine_id = ''
                    generate_new_eva_content = {
                        'company_id': self.company_id,
                        'import_type': 'CPI_VENDS',
                        'data': {
                            'fail_path': generate_fail_path,
                            'data_hash': data_hash,
                            'eva_file': vends_file_dict['file'],
                            'file_timestamp': str(vends_file_dict['import_datetime']),
                            'fail_dir': vends_file_dict['fail_dir'],
                            'success_dir': vends_file_dict['success_dir'],
                            'device_pid': vends_file_dict['device_pid'],
                            'zip_filename': vends_file_dict['zip_filename'],
                            'import_filename': vends_file_dict['file_path'],
                            'cash_collection': vends_file_dict['file_data_and_info']['cash_collection_status'],
                            'machine_id': machine_id
                        }
                    }
                    # Check if we have this pid in local history (device that already imported before)
                    if generate_new_eva_content:
                        lower_timestamp, bigger_timestamp = OldDevicePidHistory.get_old_device_pid(
                            device_pid=vends_file_dict['device_pid'],
                            company_id=int(self.company_id),
                            file_timestamp=vends_file_dict['import_datetime'],
                            import_type=self.import_type
                        )
                        if lower_timestamp or bigger_timestamp:
                            new_import_eva = new_device_pid_detect_operation(
                                data=generate_new_eva_content, import_type=self.import_type, company_id=self.company_id
                            )

                            if new_import_eva['status']:
                                iteration_eva_processing.setdefault('processing_eva', [])
                                iteration_eva_processing['processing_eva'].append(new_import_eva['data'])
                            else:
                                error_during_open_file.append(new_import_eva['data'])

                        else:
                            new_import_device_pid = new_device_pid_detect_operation(
                                data=generate_new_eva_content, company_id=self.company_id, import_type=self.import_type
                            )

                            if new_import_device_pid['status']:
                                self.process_logger.update_importer_process_flow(
                                    vends_file_dict['device_pid'], vends_file_dict['zip_filename'],
                                    status=EnumErrorType.IN_PROGRESS.name,
                                    key_enum=MainMessage.FOUND_INITIAL_EVA.value,
                                    elastic_hash=self.elastic_hash
                                )

                                MainVendProcessLogger.update_system_log_flow(
                                    vends_file_dict['device_pid'], vends_file_dict['zip_filename'],
                                    key_enum=MainMessage.FOUND_INITIAL_EVA.value,
                                    logs_level=EnumErrorType.IN_PROGRESS.name
                                )
                                handle_insert_device_pid_local_history(new_import_device_pid['data'])
                                iteration_eva_processing.setdefault('init_eva', [])
                                iteration_eva_processing['init_eva'].append(vends_file_dict['file_path'])
                            else:
                                error_during_open_file.append(new_import_device_pid['data'])

                elif self.dex_processor == self.import_type:
                    machine = DeviceQueryOnCloud.check_existing_machine_on_cloud(
                        machine_id=vends_file_dict['device_pid'], company_id=self.company_id)
                    if machine:
                        generate_new_eva_content = {
                            'company_id': self.company_id,
                            'import_type': 'DEX_VENDS',
                            'data': {
                                'fail_path': generate_fail_path,
                                'data_hash': data_hash,
                                'eva_file': vends_file_dict['file'],
                                'file_timestamp': str(vends_file_dict['import_datetime']),
                                'fail_dir': vends_file_dict['fail_dir'],
                                'success_dir': vends_file_dict['success_dir'],
                                'device_pid': vends_file_dict['device_pid'],
                                'zip_filename': vends_file_dict['zip_filename'],
                                'import_filename': vends_file_dict['file_path'],
                                'cash_collection': vends_file_dict['file_data_and_info']['cash_collection_status'],
                                'machine_id': machine.get('machine_id')
                            }
                        }
                        # Check if we have this pid in local history (device that already imported before)
                        if generate_new_eva_content:
                            lower_timestamp, bigger_timestamp = OldDevicePidHistory.get_old_device_pid(
                                device_pid=vends_file_dict['device_pid'],
                                company_id=int(self.company_id),
                                file_timestamp=vends_file_dict['import_datetime'],
                                import_type=self.import_type
                            )
                            if lower_timestamp or bigger_timestamp:
                                new_import_eva = new_device_pid_detect_operation(
                                    data=generate_new_eva_content, import_type=self.import_type,
                                    company_id=self.company_id
                                )

                                if new_import_eva['status']:
                                    iteration_eva_processing.setdefault('processing_eva', [])
                                    iteration_eva_processing['processing_eva'].append(new_import_eva['data'])
                                else:
                                    error_during_open_file.append(new_import_eva['data'])

                            else:
                                new_import_device_pid = new_device_pid_detect_operation(
                                    data=generate_new_eva_content, company_id=self.company_id,
                                    import_type=self.import_type
                                )

                                if new_import_device_pid['status']:
                                    self.process_logger.update_importer_process_flow(
                                        vends_file_dict['device_pid'], vends_file_dict['zip_filename'],
                                        status=EnumErrorType.IN_PROGRESS.name,
                                        key_enum=MainMessage.FOUND_INITIAL_EVA.value,
                                        elastic_hash=self.elastic_hash
                                    )

                                    MainVendProcessLogger.update_system_log_flow(
                                        vends_file_dict['device_pid'], vends_file_dict['zip_filename'],
                                        key_enum=MainMessage.FOUND_INITIAL_EVA.value,
                                        logs_level=EnumErrorType.IN_PROGRESS.name
                                    )
                                    handle_insert_device_pid_local_history(new_import_device_pid['data'])
                                    iteration_eva_processing.setdefault('init_eva', [])
                                    iteration_eva_processing['init_eva'].append(vends_file_dict['file_path'])
                                else:
                                    error_during_open_file.append(new_import_device_pid['data'])

            if self.cpi_processor == self.import_type:
                # Generate unique elastic message per device pid (device not paired with machine)
                if device_pid_without_machine:
                    sub_elastic_hash = create_elastic_hash(self.company_id, self.import_type,
                                                           self.process_request_type)
                    for pid in device_pid_without_machine:

                        self.process_logger.update_importer_process_flow(
                            pid, sub_elastic_hash,
                            status=EnumErrorType.WARNING.name,
                            language='en',
                            key_enum=MainMessage.DEVICE_PID_NOT_PAIRED_WITH_MACHINE_SUB_MESSAGE.value,
                            elastic_hash=self.elastic_hash
                        )
                        self.process_logger.update_system_log_flow(
                            pid, iteration_time,
                            key_enum=MainMessage.VEND_IMPORTER_DEVICE_PID_NOT_PAIRED_WITH_MACHINE.value,
                            logs_level=EnumErrorType.WARNING.name
                        )
                        self.process_logger.update_general_process_flow(
                            pid, iteration_time,
                            status=EnumErrorType.WARNING.name,
                            language='en',
                            key_enum=MainMessage.VEND_IMPORTER_DEVICE_PID_NOT_PAIRED_WITH_MACHINE.value,
                            elastic_hash=sub_elastic_hash
                        )
                        if not len(iteration_eva_processing):
                            mail_message = enum_message_on_specific_language(
                                MainMessage.DEVICE_PID_NOT_PAIRED_WITH_MACHINE_MAIL_MESSAGE.value,
                                'en', ''.join(device_pid_without_machine)
                            )
                            data = {
                                'company_id': self.company_id,
                                'hash': self.elastic_hash,
                                'email': self.email,
                                'import_type': self.import_type,
                                'message': mail_message

                            }
                            ExportHistory.vend_export_specific_hash_only_fail_history(data)

        if len(error_during_open_file):
            generate_elastic_login_for_group_file(process_logger=self.process_logger, file_paths=error_during_open_file,
                                                  company_id=self.company_id, import_type=self.import_type,
                                                  fail_dir=main_fail_dir[0], elastic_hash=self.elastic_hash,
                                                  request_type='FILE')

        if len(iteration_eva_processing):
            newest_eva_content_import = ProcessingVendFields(company_id=self.company_id, language='',
                                                             email=self.email, import_type=self.import_type,
                                                             token=self.token,
                                                             process_request_type=process_request_type,
                                                             elastic_hash=self.elastic_hash,
                                                             process_logger=self.process_logger,
                                                             iteration_processing_list=iteration_eva_processing,
                                                             cpi_processor=self.cpi_processor,
                                                             dex_processor=self.dex_processor)

            newest_eva_content_import.processing_vend_ftp_file_fields()


class ProcessingVendFields(object):
    def __init__(self, company_id, language, email, import_type, token, process_request_type, elastic_hash,
                 process_logger, iteration_processing_list, cpi_processor, dex_processor):
        self.email = email
        self.import_type = import_type
        self.token = token
        self.process_request_type = process_request_type
        self.elastic_hash = elastic_hash
        self.language = language
        self.company_id = company_id
        self.process_logger = process_logger
        self.iteration_processing_list = iteration_processing_list
        self.cpi_processor = cpi_processor
        self.dex_processor = dex_processor

    def processing_vend_ftp_file_fields(self):
        """
        This method detect if we have old and new eva for nex processing, if both eva content detected it will
        be publish to cloud validator for next processing.

        :return: appropriate old and new eva (JSON)
        """

        self.process_logger.update_system_log_flow(
            self.company_id, self.import_type,
            key_enum=MainMessage.VEND_IMPORTER_PUBLISH_DATA_TO_CLOUD_VALIDATOR.value,
            logs_level=EnumErrorType.IN_PROGRESS.name
        )
        publish_vend_file_processing(company_id=self.company_id, elastic_hash=self.elastic_hash, emails=self.email,
                                     vend_type=self.process_request_type, token=self.token, language=self.language,
                                     data=self.iteration_processing_list, import_type=self.import_type,
                                     dex_processor=self.dex_processor, cpi_processor=self.cpi_processor)





