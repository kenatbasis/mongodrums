from .bindings import bindings


class DocumentMetaclass(type):
    def __new__(cls, name, bases, attrs):
        if name not in bindings:
            bindgins[name] = '%s.%s' % (attrs['__module__'], name)
        return super(DocumentMetaclass, cls).__new__(cls, name, bases, attrs)


class Document(object):
    __metaclass__ = DocumentMetaclass

    def _to_document(self):
        doc = {'_type': self.__class__.__name__}
        for attr in filter(lambda x: x.startswith('_') and \
                                     not x.startswith('__'),
                           self.__dict__.keys()):
            value = getattr(self, attr)
            if value is not None:
                doc[attr[1:]] = value
        return doc

    def to_document(self):
        return self._to_document()


class Document(BaseDocument):
    def __init__(self):
        self._id = None

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, _id):
        self._id = id


class EmbeddedDocument(BaseDocument):
    pass


class SessionDocument(Document):
    def __init__(self):
        super(SessionDocument, self).__init__(self)

        self._name = None
        self._start_time = None
        self._end_time = None

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def start_time(self):
        return self._start_time

    @start_time.setter
    def start_time(self, start_time):
        self._start_time = start_time

    @property
    def end_time(self):
        return self._end_time

    @end_time.setter
    def end_time(self, end_time):
        self._end_time = end_time


class QueryDocument(EmbeddedDocument):
    def __init__(self):
        self._query = None
        self._covered = None
        self._count = None
        self._durations = None

    def _canonicalize_query(self, query):
        pass

    @property
    def query(self):
        return self._query

    @query.setter
    def query(self, query):
        self._query = self._canonicalize_query(query)

    @property
    def covered(self):
        return self._covered

    @covered.setter
    def covered(self, covered):
        self._covered = covered

    @property
    def count(self):
        return self._count

    @count.setter
    def count(self, count):
        self._count = count

    @property
    def durations(self):
        return self._durations

    @durations.setter
    def durations(self, durations):
        self._durations = durations


class IndexDocument(EmbeddedDocument):
    def __init__(self):
        self._index = None
        self._queries = None

    @property
    def index(self):
        return self._index

    @index.setter
    def index(self, index):
        self._index = index

    @property
    def queries(self):
        return self._queries

    @queries.setter
    def queries(self, queries):
        self._queries = queries

    def to_document(self):
        doc = self._to_document()
        doc['queries'] = dict([(k, v.to_document()) for k, v in
                               doc['queries'].iteritems()])
        return doc


class IndexUsageDocument(Document):
    def __init__(self):
        super(IndexUsageDocument, self).__init__(self)

        self._session = None
        self._collection = None
        self._indexes = None

    @property
    def session(self):
        return self._session

    @session.setter
    def session(self, session):
        self._session = session

    @property
    def collection(self):
        return self._collection

    @collection.setter
    def (self, collection):
        self._collection = collection

    @property
    def indexes(self):
        return self._indexes

    @indexes.setter
    def indexes(self, indexes):
        self._indexes = indexes

    def to_document(self):
        doc = self._to_document()
        doc['indexes'] = dict([(k, v.to_document()) for k, v in
                                doc['indexes'].iteritems()])
        return doc


class QueryExplainDocument(Document):
    def __init__(self):
        self._session = None
        self._environment = None
        self._source = None
        self._function = None
        self._explain = None

    @property
    def session(self):
        return self._session

    @session.setter
    def (self, session):
        self._session = session

    @property
    def environment(self):
        return self._environment

    @environment.setter
    def (self, environment):
        self._environment = environment

    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, source):
        self._source = source

    @property
    def function(self):
        return self._function

    @function.setter
    def function(self, function):
        self._function = function

    @property
    def explain(self):
        return self._explain

    @explain.setter
    def explain(self, explain):
        self._explain = explain

