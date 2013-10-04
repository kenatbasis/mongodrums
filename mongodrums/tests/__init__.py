from unittest import TestCase

import pymongo

from mongodrums.config import get_config, configure, update

# TODO: use ming's "mongo in memory"?


class BaseTest(TestCase):
    TEST_DB = 'mongodrums_test'
    def setUp(self):
        self.saved_config = get_config()
        for config in ['collector', 'index_profile_sink',
                       'query_profile_sink']:
            update({config: {'mongo_uri': 'mongodb://127.0.0.1/%s' %
                             (BaseTest.TEST_DB)}})
        self.client = pymongo.MongoClient()
        self.client.drop_database(self.__class__.TEST_DB)
        self.db = self.client[self.__class__.TEST_DB]

    def tearDown(self):
        configure(self.saved_config)
        self.client.drop_database(self.__class__.TEST_DB)

