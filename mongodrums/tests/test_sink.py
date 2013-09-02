import pymongo
import random

import mongodrums.instrument

from . import BaseTest
from mongodrums.config import get_config, update_config
from mongodrums.instrument import instrument
from mongodrums.sink import IndexProfileSink


class IndexProfileSinkTest(BaseTest):
    def setUp(self):
        super(IndexProfileSinkTest, self).setUp()
        self._setup_test_collection()
        self._sink = IndexProfileSink()
        self._real_push = mongodrums.instrument.push
        self._msgs = []
        mongodrums.instrument.push = self._push
        self._old_sample_frequency = \
            get_config()['instrument']['sample_frequency']
        update_config({'instrument': {'sample_frequency': 1}})

    def tearDown(self):
        super(IndexProfileSinkTest, self).tearDown()
        mongodrums.instrument.push = self._real_push
        update_config(
            {'instrument':
                {'sample_frequency': self._old_sample_frequency}})

    def _push(self, msg):
        self._msgs.append(msg)

    def _setup_test_collection(self):
        self.db.foo.ensure_index([('store', pymongo.ASCENDING),
                                  ('widget', pymongo.ASCENDING),
                                  ('sold', pymongo.DESCENDING)])
        self.db.foo.ensure_index([('store', pymongo.ASCENDING),
                                  ('widget', pymongo.ASCENDING),
                                  ('in_stock', pymongo.ASCENDING)])
        sold = range(1000)
        random.shuffle(sold)
        in_stock = range(1000)
        random.shuffle(in_stock)
        stores = ['store_%d' for i in xrange(10)]
        widgets = ['widget_%d' for i in xrange(100)]
        docs = []
        for i in xrange(len(stores)):
            for j in xrange(len(widgets)):
                docs.append({'store': stores[i],
                             'widget': widgets[j],
                             'sold': sold[len(widgets) * i + j],
                             'in_stock': in_stock[len(widgets) * i + j]})
        self.db.foo.insert(docs)

    def test_find_explain_logging(self):
        with instrument():
            count = self.db.foo.find({'sold': {'$gt': 100}}).count()
        self.assertEqual(count, 899)
        self.assertEqual(len(self._msgs), 2)

    def test_update_explain_logging(self):
        pass

