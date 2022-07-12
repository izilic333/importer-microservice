from common.urls.urls import elasticsearch_connection_url
from elasticsearch_dsl.connections import connections

host = elasticsearch_connection_url['host']
port = elasticsearch_connection_url['port']
import_index_prefix = elasticsearch_connection_url.get('import_index_prefix', '')

elastic_conn = connections.create_connection(
    hosts=[{'host': host, 'port': port}], timeout=20
)


