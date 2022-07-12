from elasticsearch_dsl import (DocType, Date, Integer, Text, Nested, InnerObjectWrapper,
                               Index)
from elasticsearch_component.connection.connection import elastic_conn  # Don't delete
from datetime import datetime
from common.mixin.enum_errors import UserEnum, ProcessEnum

class VendImporterLog(InnerObjectWrapper):
    def return_obj(self):
        return {
            'data_process': self.data_process,
            'data_process_type': self.data_process_type,
            'data_message': self.message,
            'data_status': self.status,
            'data_created_at': self.process_created_at.strftime("%Y-%m-%d %H:%M:%S")
        }


class VendCloudLog(InnerObjectWrapper):
    def return_obj(self):
        return {
            'cloud_process': self.data_process,
            'cloud_process_type': self.data_process_type,
            'cloud_process_message': self.message,
            'cloud_process_status': self.status,
            'cloud_process_created_at': self.process_created_at.strftime("%Y-%m-%d %H:%M:%S")
        }


class VendImportProcess(DocType):
    """
    Document Type used to describe vend import processes.
    company_id : id of the company for which vend import is run
    company_name: name of said company, not required
    import_type: which type of device the import is run for, e.g. CPI
    import_request_type: input type, FTP or API
    status: process status, can be one of following: STARTED, IN PROGRESS, FAIL, ERROR, WARN, SUCCESS
    created_at: date and time of process creation
    updated_at: date and time of process update
    import_data_process: subprocess object: data validation, cloud validation, cloud insert
    """
    company_id = Integer()
    company_name = Text(required=False)
    import_type = Text()
    import_request_type = Text()
    status = Text()
    created_at = Date()
    updated_at = Date()

    # Importer elastic model
    import_data_process = Nested(
        doc_class=VendImporterLog,
        properties={
            'data_process': Integer(),
            'data_process_type': Integer(),
            'data_process_message': Text(),
            'data_process_status': Text(),
            'data_process_created_at': Date()
        }
    )

    # Cloud elastic model
    import_data_cloud = Nested(
        doc_class=VendCloudLog,
        properties={
            'cloud_process': Integer(),
            'cloud_process_type': Integer(),
            'cloud_process_message': Text(),
            'cloud_process_status': Text(),
            'cloud_process_created_at': Date()
        }
    )

    def add_importer_validation_process(self, message, status):
        self.import_data_process.append(
            {
                'data_process': ProcessEnum.PROCESS_FILE_VALIDATION.value,
                'data_process_type': UserEnum.ADMIN.value,
                'data_process_message': message,
                'data_process_status': status,
                'data_process_created_at': datetime.now()
            }
        )
        self.updated_at = datetime.now()
        self.status = status

    def add_cloud_validation_process(self, message, status):
        self.import_data_cloud.append(
            {
                'cloud_process': ProcessEnum.PROCESS_CLOUD_VALIDATION.value,
                'cloud_process_type': UserEnum.ADMIN.value,
                'cloud_process_message': message,
                'cloud_process_status': status,
                'cloud_process_created_at': datetime.now()
            }
        )
        self.updated_at = datetime.now()
        self.status = status

    def save(self, **kwargs):
        self.updated_at = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        return super().save(**kwargs)

    def process_status(self):
        return self.status


class VendImportIndex(object):
    """
    Index subclass for creating dynamic indices based on date of document
    """
    @classmethod
    def init(cls, index_name):
        # Create new index if requested index does not exist
        es = elastic_conn
        if not es.indices.exists(index_name):
            index = Index(index_name)
            index.settings(number_of_shards=1,
                           number_of_replicas=0)
            index.doc_type(VendImportProcess)
            index.create()
        return index_name


if __name__ == "__main__":
    # Do something with models
    pass
