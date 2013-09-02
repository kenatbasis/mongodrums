from abc import ABCMeta, abstractmethod

from .config import get_config


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


class IndexProfileSink(Sink):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_MongoClient'):
            from gevent import monkey; monkey.patch_socket()
            from pymongo import MongoClient
            cls._MongoClient = MongoClient

    def __init__(self):
        self._config = get_config()
        self._db = None
        self._session_col = None
        self._index_usage_col = None
        self._query_explain_col = None

    @property
    def db(self):
        if self._db is None:
            mongo_uri = self._config.index_profile_sink.mongo_uri
            self._db = \
                self.__class__._MongoClient(
                    mongo_uri).get_default_database()
        return self._db

    @property
    def session_col(self):
        if self._session_col is None:
            from .collection import SessionCollection
            col_name = SessionDocument.get_collection_name()
            self._session_col = SessionCollection(self.db[col_name])
        return self._session_col

    @property
    def index_usage_col(self):
        if self._index_usage_col is None:
            from .collection import IndexUsageCollection
            col_name = IndexUsageCollection.get_collection_name()
            self._index_usage_col = IndexUsageCollection(self.db[col_name])
        return self._index_usage_col

    @property
    def query_explain_col(self):
        if self._query_explain_col is None:
            from .collection import QueryExplainCollection
            col_name = QueryExplainCollection.get_collection_name()
            self._query_explain_col = QueryExplainCollection(self.db[col_name])
        return self._query_explain_col

    def handle(self, data, address):
        self.query_explain_col.save({'session': data.get('session', None),
                                     'function': data.get('function', None),
                                     'database': data.get('database', None),
                                     'explain': data['explain'],
                                     'source': data['source']})


