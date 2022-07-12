import json
import requests

from common.mixin.elastic_login import ElasticCloudLoginFunctions
from common.mixin.enum_errors import EnumErrorType
from common.mixin.validation_const import ImportType, get_import_type_by_name
from common.urls.urls import machine_url, location_url, region_url, machine_type_url
from common.logging.setup import logger
from elasticsearch_component.core.logger import CompanyProcessLogger
from elasticsearch_component.core.query_vends import VendImportProcessLogger


class ParseRabbitRequests(object):
    """

        Send data to cloud and write log to ElasticSearch.
        It will call CLOUD on specific URL and take care of any exception.

    """

    @classmethod
    def call_methods_based_on_type(cls, data):
        """

        :param data:   Data is JSON from Q process
        :return: it will not return any data only for calling other functions
        """
        CompanyProcessLogger.create_process_flow(
            data['elastic_hash'],
            'Consumer of Q for database insert type {}'.format(data['type']),
            EnumErrorType.IN_PROGRESS.name
        )
        import_type = get_import_type_by_name(data['type'])
        if import_type == ImportType.MACHINES:
            url = machine_url
            cls.send_data_to_cloud(data, url)
        elif import_type == ImportType.LOCATIONS:
            url = location_url
            cls.send_data_to_cloud(data, url)
        elif import_type == ImportType.REGIONS:
            url = region_url
            cls.send_data_to_cloud(data, url)
        elif import_type == ImportType.MACHINE_TYPES:
            url = machine_type_url
            cls.send_data_to_cloud(data, url)

    @classmethod
    def save_logging_message(cls, data, error, message):
        """

        :param data:  Data is JSON from Q process
        :param error: Error status
        :param message: Message to write on process flow in Elasticsearch
        :return: is only to close function
        """
        ElasticCloudLoginFunctions.create_process_flow(
            data['elastic_hash'], error, message)
        ElasticCloudLoginFunctions.create_cloud_process_flow(
            data['elastic_hash'], error, message)
        ElasticCloudLoginFunctions.update_main_process(
            hash=data['elastic_hash'], error=error)

        return

    @classmethod
    def vend_save_logging_message(cls, data, error, message):
        """

        :param data:  Data is JSON from Q process
        :param error: Error status
        :param message: Message to write on process flow in Elasticsearch
        :return: is only to close function
        """
        VendImportProcessLogger.create_cloud_validation_process_flow(
            process_hash=data['elastic_hash'], status=error, message=message)
        VendImportProcessLogger.create_importer_validation_process_flow(
            process_hash=data['elastic_hash'], status=error, message=message)
        VendImportProcessLogger.update_main_vend_process(
            process_hash=data['elastic_hash'], status=error)

        return

    @classmethod
    def send_data_to_cloud(cls, data, url_path):
        """

        :param data: Data is JSON from process
        :param url_path: URL path is path from ENVDIR where all URL-s to cloud are stored
                         by specific call on cloud
        :return: it will return True or False based on cloud availability
        """
        token = data['token']
        data_convert = json.dumps(data)
        logger.info(">>> Sending data to cloud: {} {}".format(url_path, data_convert))
        try:
            send_to_cloud = requests.post(
                url=url_path,
                json=data_convert,
                headers={
                    'Content-type': 'application/json',
                    'Authorization': '%s' % token
                },
                timeout=120.00
            )
            if send_to_cloud.status_code in [200, 201]:
                logger.info(">>> Data passed to cloud Q: {} --> Data: {}".format(
                    url_path, send_to_cloud.content)
                )
                return True
            else:
                logger.warn(">>> Error cloud: {} --> Data: {}".format(
                    url_path, send_to_cloud.content)
                )
                return False
        except Exception as e:
            logger.error(">>> Error cloud: {} --> Error: {}".format(url_path, e))
            return False
