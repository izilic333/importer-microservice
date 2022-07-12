import uuid
from elasticsearch.exceptions import RequestError
from elasticsearch_dsl import DocType, Date, Integer, Text, Nested, InnerObjectWrapper, MetaField, Mapping, Index
from elasticsearch_component.connection.connection import elastic_conn, import_index_prefix  # Don't delete
from datetime import datetime

INDEX_SUFFIX = "company_process"


class CompanyProcessLogByType(InnerObjectWrapper):
    def return_obj(self):
        return {'message': self.message,
                'status': self.status,
                'process_created_at': self.process_created_at.strftime("%Y-%m-%d %H:%M:%S")
                }


class CompanyCloudLog(InnerObjectWrapper):
    def return_obj(self):
        return {'cloud_message': self.message,
                'cloud_status': self.status,
                'cloud_process_created_at': self.process_created_at.strftime("%Y-%m-%d %H:%M:%S")
                }


class CompanyProcess(DocType):
    company_id = Integer()
    company_name = Text(required=False)
    process_type = Text()
    status = Text()
    process_request_type = Text()
    created_at = Date()
    updated_at = Date()

    process = Nested(
        doc_class=CompanyProcessLogByType,
        properties={
            'message': Text(),
            'status': Text(),
            'process_created_at': Date()
        }
    )

    process_cloud = Nested(
        doc_class=CompanyCloudLog,
        properties={
            'cloud_message': Text(),
            'cloud_status': Text(),
            'cloud_process_created_at': Date()
        }
    )

    class Meta:
        timestamp = MetaField(enabled=True, store=True)
        index = '{}{}'.format(import_index_prefix, INDEX_SUFFIX)

    def add_process(self, message, status):
        self.process.append(
            {
                'message': message,
                'status': status,
                'process_created_at': datetime.now()
            }
        )
        self.updated_at = datetime.now()

    def add_cloud_process(self, message, status):
        self.process_cloud.append(
            {
                'cloud_message': message,
                'cloud_status': status,
                'cloud_process_created_at': datetime.now()
            }
        )
        self.updated_at = datetime.now()

    def save(self, **kwargs):
        self.updated_at = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        return super().save(**kwargs)

    def process_status(self):
        return self.status


try:
    # Create new index if requested index does not exist
    es = elastic_conn
    full_index = '{}{}'.format(import_index_prefix, INDEX_SUFFIX)
    if not es.indices.exists(full_index):
        index = Index(full_index)
        index.settings(number_of_shards=1,
                       number_of_replicas=0)
        try:
            # just try to create valid index
            index.create()
            process_id = uuid.uuid4().hex
            company_process = CompanyProcess(meta={'id': process_id})
            company_process.company_id = 0
            company_process.company_name = 'TEST'
            company_process.process_type = 'API'
            company_process.status = 'STARTED'
            company_process.process_request_type = 'EVENTS'
            company_process.created_at = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            company_process.save()
        except RequestError:
            pass
except Exception as e:
    print(e)


if __name__ == "__main__":
    CompanyProcess.init()
    CompanyProcess._doc_type.refresh()
