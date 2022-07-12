import os
import shutil
import zipfile
import datetime
from dateutil import parser
from common.mixin.enum_errors import EnumErrorType
from database.cloud_database.core.query import CompanyQueryOnCloud
from common.mixin.enum_errors import EnumValidationMessage as MainMessage
from database.company_database.core.query_history import OldDevicePidHistory
from common.rabbit_mq.database_interaction_q.vend_db_publisher import vend_publish_to_database
from common.mixin.validator_import import VENDS_WORKING_DIR, create_if_doesnt_exist, DOWNLOAD_VENDS_DIR
from common.mixin.vends_mixin import (VENDS_INITIAL_INFO, MainVendProcessLogger, get_device_object, EvaHandler,
                                      handle_insert_device_pid_local_history, create_elastic_hash, CleanLocalHistory,
                                      EvaAfterCloudValidationTasks)


class CpiCloudValidator(object):
    """
    This is main class for cloud validation, in this part we make some basic validation on cloud:
        - check if device pid is paired with machine on cloud (for specific company)
        - check if exists machine external id (machine that is paired with device pid)

    On this part we compare old eva and new eva field (field that is characteristic for vends), and calculate
    number of vends based on since last reset.

    """

    def __init__(self, data):
        self.data = data
        self.main_content = self.data['data']
        self.iteration_content = self.main_content.get('processing_eva', None)
        self.init_eva_files = self.main_content.get('init_eva', None)
        self.elastic_hash = self.data.get('elastic_hash')
        self.email = self.data.get('email')
        self.request_type = self.data.get('type')
        self.token = self.data.get('token')
        self.company_id = self.data.get('company_id')
        self.import_type = self.data.get('import_type')
        self.import_file_path = ""
        self.general_process_logger = MainVendProcessLogger(
            company_id=self.company_id, import_type=self.import_type, process_request_type=self.request_type,
            token=self.token
        )
        self.working_directory = os.path.join(VENDS_WORKING_DIR, self.import_type, str(self.company_id))
        create_if_doesnt_exist(self.working_directory)

    def main_basic_cloud_cpi_validations(self):
        cpi_payment_type = {'CA': 0, 'DB': 1, 'DC': 2, 'DD': 3, 'DA': 1}
        vends_array = []
        reimport_list = {}
        reimport_filename = []
        total_vends_per_device_pid = {}
        success_processing_eva_file = []
        fail_processing_eva_file = []
        all_processing_eva_file = []
        file_with_wrong_counter_details = []
        wrong_counter_file = []
        generate_new_eva_content = False
        if self.init_eva_files:
            for init_eva in self.init_eva_files:
                if init_eva not in success_processing_eva_file:
                    init_eva_filename = os.path.basename(init_eva)
                    success_processing_eva_file.append(init_eva_filename)
                    all_processing_eva_file.append(init_eva)

        final_validation_processing = EvaAfterCloudValidationTasks(
            company_id=self.company_id, general_process_logger=self.general_process_logger, token=self.token,
            import_type=self.import_type, request_type=self.request_type, working_directory=self.working_directory,
            elastic_hash=self.elastic_hash, email=self.email)

        for x in self.iteration_content:

            # Get vend import info
            new_eva = x['data']['eva_content']
            file_timestamp = parser.parse(x['data']['file_timestamp'])
            device_pid = x['data']['device_pid']
            zip_filename = x['data']['zip_filename']
            import_filename = x['data']['import_filename']
            eva_file = x['data']['eva_file']
            success_dir = x['data']['success_dir']
            fail_dir = x['data']['fail_dir']

            if eva_file not in all_processing_eva_file:
                all_processing_eva_file.append(eva_file)

            # Handle eva timestamp (useful for reimport)
            lower_timestamp, bigger_timestamp = OldDevicePidHistory.get_old_device_pid(
                device_pid=device_pid, company_id=self.company_id, file_timestamp=file_timestamp,
                import_type=self.import_type)

            if lower_timestamp or bigger_timestamp:
                if not len(lower_timestamp):
                    # Found oldest eva, this is eva for reimport,for this eva we do not have an old file in the archive!
                    oldest_device = get_device_object(device_pid=device_pid, company_id=self.company_id)
                    if oldest_device:
                        oldest_machine_id = oldest_device.get('machine_id')
                        generate_oldest_eva = {
                            'company_id': self.company_id,
                            'import_type': self.import_type,
                            'data': {
                                'file_timestamp': str(file_timestamp),
                                'device_pid': device_pid,
                                'zip_filename': zip_filename,
                                'import_filename': import_filename,
                                'eva_content': new_eva,
                                'machine_id': oldest_machine_id
                            }
                        }
                        handle_insert_device_pid_local_history(generate_oldest_eva)
                        continue

                # Get some information from old eva (eva from database that was already processed),
                # and only if device pid is equal to new eva pid, we make vends calculation!
                for timestamp_first_lower in lower_timestamp:
                    for old_pid in timestamp_first_lower:
                        if str(old_pid['device_pid']) == str(device_pid):
                            self.general_process_logger.update_importer_process_flow(
                                device_pid, zip_filename, status=EnumErrorType.IN_PROGRESS.name,
                                key_enum=MainMessage.VEND_IMPORT_PID_EQUAL_TO_ARCHIVE_PID.value,
                                elastic_hash=self.elastic_hash)
                            # Get device information
                            device = get_device_object(device_pid=old_pid['device_pid'], company_id=self.company_id)
                            if device:
                                device_pid_info = device.get("device_pid")
                                device_alive = device.get("device_alive")
                                device_status = device.get("device_status")
                                device_type_id = device.get("device_type_id")
                                device_id = device.get("device_id")
                                machine_external_id = device.get("machine_external_id")
                                machine_id = device.get('machine_id')

                                # Check if importer local machine_id is same as cloud machine id
                                check_machine_id = OldDevicePidHistory.check_archive_machine_id(
                                    cloud_machine_id=machine_id, device_pid=device_pid, company_id=self.company_id,
                                    import_type=self.import_type)

                                # Generate new eva content structure
                                generate_new_eva_content = {
                                    'company_id': self.company_id,
                                    'import_type': self.import_type,
                                    'data': {
                                        'file_timestamp': str(file_timestamp),
                                        'device_pid': device_pid,
                                        'zip_filename': zip_filename,
                                        'import_filename': import_filename,
                                        'eva_content': new_eva,
                                        'machine_id': machine_id
                                    }
                                }
                                eva_file = os.path.basename(import_filename)
                                if eva_file not in success_processing_eva_file:
                                    success_processing_eva_file.append(eva_file)

                                if not check_machine_id['status']:
                                    handle_insert_device_pid_local_history(generate_new_eva_content)
                                    self.general_process_logger.update_importer_process_flow(
                                        device_pid, machine_id, check_machine_id['old_machine_id'], zip_filename,
                                        status=EnumErrorType.IN_PROGRESS.name,
                                        key_enum=MainMessage.VEND_DEVICE_PAIRED_WITH_NEW_MACHINE.value,
                                        elastic_hash=self.elastic_hash)
                                    continue

                                self.general_process_logger.update_importer_process_flow(
                                    old_pid['zip_filename'], zip_filename,
                                    status=EnumErrorType.IN_PROGRESS.name,
                                    key_enum=MainMessage.VEND_CLOUD_VALIDATOR_START_PROCESSING.value,
                                    elastic_hash=self.elastic_hash)

                                self.general_process_logger.update_importer_process_flow(
                                    device_pid, device_id, device_alive, device_status, device_type_id,
                                    machine_external_id,
                                    status=EnumErrorType.IN_PROGRESS.name,
                                    key_enum=MainMessage.DETECT_PAIRED_EVA_DEVICE_INFO.value,
                                    elastic_hash=self.elastic_hash)

                                if not machine_external_id:
                                    self.general_process_logger.update_importer_process_flow(
                                        self.company_id, self.import_type, old_pid['device_pid'],
                                        status=EnumErrorType.IN_PROGRESS.name,
                                        language='en',
                                        key_enum=MainMessage.DETECT_PAIRED_EVA_WITHOUT_MACHINE_EXTERNAL_ID.value,
                                        elastic_hash=self.elastic_hash)
                                    continue

                                self.general_process_logger.update_importer_process_flow(
                                    self.company_id, self.import_type, device_pid_info, old_pid['file_timestamp'],
                                    file_timestamp,
                                    status=EnumErrorType.IN_PROGRESS.name,
                                    key_enum=MainMessage.START_CALCULATE_VENDS.value,
                                    elastic_hash=self.elastic_hash)

                                total_vends = 0
                                old_eva_pa_fields = old_pid['data']['data']['eva_content']["pa_field"]
                                new_eva_pa_fields = new_eva["pa_field"]
                                new_eva_id_fields = new_eva["id_field"]
                                new_eva_decimal_points = [x for x in new_eva_id_fields if x.startswith('ID4*')]
                                # Detect EVA fields
                                pa7_handler = False
                                pa1pa2 = False
                                for pa_fields in new_eva_pa_fields:
                                    if pa_fields.startswith('PA7'):
                                        pa7_handler = True
                                    elif pa_fields.startswith('PA1') or pa_fields.startswith('PA1') and not \
                                            pa_fields.startswith('PA7'):
                                        pa1pa2 = True

                                eva_handler = EvaHandler(
                                    company_id=self.company_id,
                                    import_type=self.import_type,
                                    request_type=self.request_type,
                                    main_elastic_hash=self.elastic_hash,
                                    general_process_logger=self.general_process_logger,
                                    zip_filename=zip_filename,
                                    cpi_payment_type=cpi_payment_type)

                                # Two types of calculating EVA vend: a) PA1 and PA2 EVA fields, b) PA7 EVA fields
                                if pa7_handler:
                                    pa7_result = eva_handler.eva_pa7_fields_handler(
                                        old_eva_pa_fields, new_eva_pa_fields, eva_file, machine_id,
                                        machine_external_id,
                                        new_eva_decimal_points, file_timestamp)
                                    if len(pa7_result['vends_array']):
                                        for vends_data_pa7 in pa7_result['vends_array']:
                                            vends_array.append(vends_data_pa7)

                                    total_vends = pa7_result['total_vends'] + total_vends
                                    wrong_counter_file = pa7_result['wrong_counter_file'] + wrong_counter_file
                                    fail_processing_eva_file = pa7_result['fail_processing_eva_file'] + fail_processing_eva_file
                                    file_with_wrong_counter_details = pa7_result['file_with_wrong_counter_details'] + file_with_wrong_counter_details

                                elif pa1pa2:
                                    pa1_pa2_old = []
                                    pa1_pa2_new = []
                                    for old_eva_pa in old_eva_pa_fields:
                                        if old_eva_pa.startswith('PA1') or old_eva_pa.startswith('PA2'):
                                            pa1_pa2_old.append(old_eva_pa)

                                    for new_eva_pa in new_eva_pa_fields:
                                        if new_eva_pa.startswith('PA1') or new_eva_pa.startswith('PA2'):
                                            pa1_pa2_new.append(new_eva_pa)

                                    eva_position = eva_handler.eva_pa_fields_handler(
                                        pa_fields=pa1_pa2_old, eva_type='old_eva_pa'
                                    ) + eva_handler.eva_pa_fields_handler(
                                        pa_fields=pa1_pa2_new, eva_type='new_eva_pa')

                                    new_pa_fields, old_pa_fields = eva_handler.group_eva_pa_fields(eva_position)

                                    pa1_pa2_result = eva_handler.eva_pa_vend_calculation(
                                        new_pa_fields=new_pa_fields, old_pa_fields=old_pa_fields, eva_file=eva_file,
                                        new_eva_decimal_points=new_eva_decimal_points, file_timestamp=file_timestamp,
                                        machine_id=machine_id, machine_external_id=machine_external_id)

                                    if len(pa1_pa2_result['vends_array']):
                                        for vends_data_pa1_pa2 in pa1_pa2_result['vends_array']:
                                            vends_array.append(vends_data_pa1_pa2)
                                    total_vends = pa1_pa2_result['total_vends'] + total_vends
                                    wrong_counter_file = pa1_pa2_result['wrong_counter_file'] + wrong_counter_file
                                    fail_processing_eva_file = pa1_pa2_result['fail_processing_eva_file'] + fail_processing_eva_file
                                    file_with_wrong_counter_details = pa1_pa2_result['file_with_wrong_counter_details'] + file_with_wrong_counter_details

                                if total_vends > 0 and zip_filename not in wrong_counter_file:
                                    total_vends_per_device_pid.setdefault('vends', [])
                                    total_vends_per_device_pid['vends'].append(total_vends)
                                    total_vends_per_device_pid['machine_device'] = {
                                        'machine_id': machine_id,
                                        'device_pid': device_pid}

                                    self.general_process_logger.update_importer_process_flow(
                                        device_pid, machine_external_id, zip_filename, total_vends,
                                        status=EnumErrorType.IN_PROGRESS.name,
                                        key_enum=MainMessage.VEND_IMPORTER_NEW_VEND_FOUND.value,
                                        elastic_hash=self.elastic_hash)

                                elif total_vends == 0 and zip_filename not in wrong_counter_file:

                                    self.general_process_logger.update_importer_process_flow(
                                        device_pid, machine_external_id, zip_filename,
                                        status=EnumErrorType.IN_PROGRESS.name,
                                        language='en', key_enum=MainMessage.VEND_IMPORTER_NO_VENDS_DETECTED.value,
                                        elastic_hash=self.elastic_hash
                                    )
                                    self.general_process_logger.update_system_log_flow(
                                        device_pid, machine_external_id, zip_filename,
                                        key_enum=MainMessage.VEND_IMPORTER_NO_VENDS_DETECTED.value,
                                        logs_level=EnumErrorType.IN_PROGRESS.name)

                                if bigger_timestamp and zip_filename not in wrong_counter_file:
                                    reimport_filename.append(zip_filename)
                                    for timestamp_first_bigger in bigger_timestamp:
                                        for old_device_pid_history in timestamp_first_bigger:
                                            if str(old_device_pid_history['device_pid']) == str(device_pid):

                                                first_bigger_zip_filename = ''.join(
                                                    [y['zip_filename'] for x in bigger_timestamp for y in x])

                                                reimport_list.setdefault(first_bigger_zip_filename, [])
                                                reimport_list[first_bigger_zip_filename].append(
                                                    {zip_filename: total_vends})
                            continue
                        continue

            # insert into the local archive files that have been successfully processed!
            # This is in comment, because with this condition EVA file with partial success status causes Infinity
            # vend import, so we make local archive for all processed file...
            # if generate_new_eva_content and zip_filename not in wrong_counter_file:
            if generate_new_eva_content:
                handle_insert_device_pid_local_history(generate_new_eva_content)

        final_validation_processing.eva_final_validation(
            file_with_wrong_counter_details, total_vends_per_device_pid, fail_dir, success_dir)

        clean_local_archive = CleanLocalHistory(process_logger=self.general_process_logger)

        # Publish dex vend import data to cloud ...
        if len(vends_array):
            final_validation_processing.publish_eva_data_to_cloud(
                success_processing_eva_file=success_processing_eva_file,
                success_dir=success_dir,
                reimport_list=reimport_list,
                reimport_filename=reimport_filename,
                vends_array=vends_array,
                machine_id=machine_id,
                all_processing_eva_file=all_processing_eva_file,
                fail_processing_eva_file=fail_processing_eva_file,
                datetime_format=VENDS_INITIAL_INFO['cpi_import']['vends'],
                filename_regex=VENDS_INITIAL_INFO['cpi_import']['vends']
            )
        store_dir = os.path.join(DOWNLOAD_VENDS_DIR, self.import_type, str(self.company_id))
        if os.listdir(store_dir):
            clean_local_archive.clean_directory(store_dir)
        if os.listdir(self.working_directory):
            clean_local_archive.clean_directory(self.working_directory)
