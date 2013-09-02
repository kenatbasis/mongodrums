import inspect

import pymongo

from mock import patch

from . import BaseTest
from mongodrums.instrument import (
    UpdateWrapper, FindWrapper, start, stop, instrument
)

from mongodrums.config import update_config


class InstrumentTest(BaseTest):
    def setUp(self):
        super(InstrumentTest, self).setUp()
        self.db.foo.insert([{'_id': 1, 'name': 'bob'},
                            {'_id': 2, 'name': 'alice'},
                            {'_id': 3, 'name': 'zed'},
                            {'_id': 4, 'name': 'yohan'}])
        self.db.foo.ensure_index('name')

    def test_instrument_update(self):
        update = pymongo.collection.Collection.update
        UpdateWrapper.wrap()
        try:
            self.assertNotEqual(pymongo.collection.Collection.update, update)
            self.assertIsInstance(pymongo.collection.Collection.update, UpdateWrapper)
        finally:
            UpdateWrapper.unwrap()
        self.assertEqual(pymongo.collection.Collection.update, update)
        self.assertNotIsInstance(pymongo.collection.Collection.update, UpdateWrapper)

    def test_instrument_insert(self):
        find = pymongo.collection.Collection.find
        FindWrapper.wrap()
        try:
            self.assertNotEqual(pymongo.collection.Collection.find, find)
            self.assertIsInstance(pymongo.collection.Collection.find, FindWrapper)
        finally:
            FindWrapper.unwrap()
        self.assertEqual(pymongo.collection.Collection.find, find)
        self.assertNotIsInstance(pymongo.collection.Collection.find,
                                 FindWrapper)

    def test_find_push(self):
        update_config({'instrument': {'sample_frequency': 1}})
        with patch('mongodrums.instrument.push') as push_mock, \
             FindWrapper.instrument():
            doc = self.db.foo.find_one({'name': 'bob'})
            self.assertEqual(doc, {'_id': 1, 'name': 'bob'})
            self.assertEqual(push_mock.call_count, 1)
            self.assertIn('allPlans', push_mock.call_args[0][0]['explain'])
        self.assertNotIsInstance(pymongo.collection.Collection.find,
                                 FindWrapper)
        self.assertNotIsInstance(self.db.foo.find,
                                 FindWrapper)

    def test_config_update(self):
        with instrument():
            self.assertEqual(pymongo.collection.Collection.find._frequency,
                             self.saved_config.instrument.sample_frequency)
            update_config({'instrument': {'sample_frequency': 1}})
            self.assertEqual(pymongo.collection.Collection.find._frequency, 1)

    def test_get_source(self):
        update_config({'instrument': {'sample_frequency': 1}})
        with patch('mongodrums.instrument.push') as push_mock, \
             FindWrapper.instrument():
            doc = self.db.foo.find_one({'name': 'bob'})
            frame_info = inspect.getframeinfo(inspect.currentframe())
            source = '%s:%d' % (frame_info[0], frame_info[1] - 1)
            self.assertEqual(push_mock.call_args[0][0]['source'], source)

