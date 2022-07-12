import unittest
import requests
import json
import os


class ElasticViewTestCase(unittest.TestCase):

    def setUp(self):
        # When running tests, make sure to provide a token here for authorization
        self.host_data = json.loads(os.environ['FLASK_CONFIG'])
        self.connection = 'http://' + str(self.host_data['host']) + ":" + str(self.host_data['port'])
        self.token = ''

    def test_get_history_by_hash(self):
        """
        Assert that the API view for hash search returns either a 200 if token is valid and data is found,
        or 404 if token is valid and data is not found.
        """
        vend_hash = ""
        response = requests.get(self.connection + '/vend/history/hash/'+vend_hash,
                                headers={'Authorization': self.token})
        self.assertEqual(response.status_code, 404)

    def test_get_history_by_company_id(self):
        """
        Assert that the API view for company search returns either a 200 if token is valid and data is found,
        or 404 if token is valid and data is not found.
        """
        response = requests.get(self.connection + '/vend/history/company/all',
                                headers={'Authorization': self.token})
        self.assertIn(json.loads(response.text)['code'], [403])

    def test_get_history_by_type(self):
        """
        Assert that the API view for company search returns either a 200 if token is valid and data is found,
        or 404 if token is valid and data is not found.
        """
        vend_type = ""
        response = requests.get(self.connection + '/vend/history/type/'+vend_type,
                                headers={'Authorization': self.token})
        self.assertEqual(response.status_code, 404)

    def test_get_history_by_date_range(self):
        """
        Assert that the API view for date search returns either a 200 if token is valid and data is found,
        or 404 if token is valid and data is not found.
        """
        start_date = ''
        end_date = ''
        response = requests.get(self.connection + '/vend/history/query',
                                headers={'Authorization': self.token},
                                params={'start': start_date,
                                        'end': end_date})
        self.assertIn(json.loads(response.text)['code'], [403])


if __name__ == '__main__':
    unittest.main()
