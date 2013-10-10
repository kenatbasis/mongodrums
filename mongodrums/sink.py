from abc import ABCMeta, abstractmethod

from pymongo.errors import DuplicateKeyError

from .config import get_config
from .util import get_default_database, sanitize, skeleton


class Sink(object):
    __metaclass__ = ABCMeta

    def filter(self, data, address):
        return False

    def handle(self, data, address):
        if not self.filter(data, address):
           self.send(data, address)

    @abstractmethod
    def send(self, data, address):
        pass


class ProfileSink(Sink):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_MongoClient'):
            from gevent import monkey; monkey.patch_socket()
            from pymongo import MongoClient
            cls._MongoClient = MongoClient
        return super(ProfileSink, cls).__new__(cls, *args, **kwargs)

    def __init__(self):
        self._config = get_config()
        self._db = None
        self._session_col = None

    def filter(self, data, address):
        return data['collection'].startswith('$')

    @property
    def db(self):
        if self._db is None:
            mongo_uri = self._config.index_profile_sink.mongo_uri
            client = self.__class__._MongoClient(mongo_uri)
            self._db = get_default_database(client, mongo_uri)
        return self._db

    @property
    def session_col(self):
        if self._session_col is None:
            from .collection import SessionCollection
            col_name = SessionCollection.get_collection_name()
            self._session_col = SessionCollection(self.db[col_name])
        return self._session_col


class IndexProfileSink(ProfileSink):
    def __init__(self):
        super(IndexProfileSink, self).__init__()
        self._index_profile_col = None

    @property
    def index_profile_col(self):
        if self._index_profile_col is None:
            from .collection import IndexProfileCollection
            col_name = IndexProfileCollection.get_collection_name()
            self._index_profile_col = IndexProfileCollection(self.db[col_name])
        return self._index_profile_col

    def send(self, data, address):
        q = {'session': data['session'],
             'collection': data['collection'],
             'index': data['explain']['cursor']}
        query_skeleton = skeleton(data['query'])
        try:
            doc = {'queries': []}
            doc.update(q)
            self.index_profile_col.insert(doc)
        except DuplicateKeyError:
            pass

        q.update({'queries.query': {'$ne': query_skeleton}})
        self.index_profile_col.update(
            q,
            {
                '$push': {
                    'queries': {
                        'query': query_skeleton,
                        'count': 0,
                        'durations': []
                    }
                }
            })

        self.index_profile_col.update(
            {'session': data['session'],
             'collection': data['collection'],
             'index': data['explain']['cursor'],
             'queries.query': query_skeleton},
            {'$inc': {'queries.$.count': 1},
             '$set': {
                 'queries.$.covered': data['explain']['indexOnly']
             },
             '$push': {
                 'queries.$.durations': data['explain']['millis']
            }})


class QueryProfileSink(ProfileSink):
    def __init__(self):
        super(QueryProfileSink, self).__init__()
        self._query_profile_col = None

    def filter(self, data, address):
        return data['collection'].startswith('$')

    @property
    def query_profile_col(self):
        if self._query_profile_col is None:
            from .collection import QueryProfileCollection
            col_name = QueryProfileCollection.get_collection_name()
            self._query_profile_col = QueryProfileCollection(self.db[col_name])
        return self._query_profile_col

    def send(self, data, address):
        query_profile_doc = \
            {'function': data['function'],
             'database': data['database'],
             'collection': data['collection'],
             'session': data['session'],
             'explain': sanitize(data['explain']),
             'query': skeleton(data['query']),
             'source': data['source']}
        self.query_profile_col.save(query_profile_doc)

