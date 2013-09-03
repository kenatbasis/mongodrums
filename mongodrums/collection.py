import pymongo

from inflection import underscore
from makerpy.object_collection import ObjectCollection

from .bindings import bindings
from .document import (
    Document, SessionDocument, IndexProfileDocument, QueryProfileDocument
)


class MongoDrumsCollection(ObjectCollection):
    _default_class = None

    def __init__(self, collection):
       super(MongoDrumsCollection, self).__init__(
                                            collection,
                                            self.__class__._default_class,
                                            bindings)

    @property
    def collection_name(self):
        return self.__class__.get_collection_name()

    @classmethod
    def get_collection_name(cls):
        return '_'.join(underscore(cls.__name__).rsplit('_', 1)[:-1])

    def insert(self, document):
        if isinstance(document, Document):
            document = document.to_document()
        self.collection.insert(document)

    def save(self, document):
        if isinstance(document, Document):
            document = document.to_document()
        self.save_document(document)

    def remove(self, document):
        if isinstance(document, Document):
            document = document.to_document()
        if '_id' not in document:
            raise ValueError('document has no id')
        self.remove_by_id(document['_id'])


class SessionCollection(MongoDrumsCollection):
    pass


class IndexProfileCollection(MongoDrumsCollection):
    def __init__(self, collection):
        super(IndexProfileCollection, self).__init__(collection)
        self.collection.ensure_index([('session', pymongo.ASCENDING), 
                                      ('collection', pymongo.ASCENDING),
                                      ('index', pymongo.ASCENDING)],
                                     unique=True)


class QueryProfileCollection(MongoDrumsCollection):
    pass

