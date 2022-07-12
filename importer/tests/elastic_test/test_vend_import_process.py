import unittest
from elasticsearch_component.core.query_vends import VendImportProcessLogger
from common.urls.urls import elasticsearch_connection_url
from elasticsearch_dsl.connections import connections


class ElasticComponentTestCase(unittest.TestCase):

    def setUp(self):
        host = elasticsearch_connection_url['host']
        port = elasticsearch_connection_url['port']
        connections.create_connection('local_test',
                                      hosts=[{'host': host, 'port': port}],
                                      timeout=20)

    def tearDown(self):
        connections.remove_connection('local_test')

    def test_create_new_process(self):
        result = VendImportProcessLogger.create_new_process(485, "CPI", "FILE")
        self.assertEqual(result['process_created'], True)

    def test_update_process_with_file_validation_subprocess(self):
        self.process = VendImportProcessLogger.create_new_process(485, "CPI", "FILE")
        self.res = VendImportProcessLogger.create_importer_validation_process_flow(self.process['id'],
                                                                                  "IN PROGRESS",
                                                                                  "File validation started!"
                                                                                  )
        self.assertEqual(self.res['process_updated'], True)

    def test_update_process_with_cloud_validation_subprocess(self):
        self.process = VendImportProcessLogger.create_new_process(485, "CPI", "FILE")
        self.res = VendImportProcessLogger.create_cloud_validation_process_flow(self.process['id'],
                                                                                   "IN PROGRESS",
                                                                                   "Cloud validation started!"
                                                                                   )
        self.assertEqual(self.res['process_updated'], True)


if __name__ == '__main__':
    unittest.main()