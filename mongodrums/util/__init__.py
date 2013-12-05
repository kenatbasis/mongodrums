import re
import urlparse

from datetime import datetime

from bson.json_util import loads, dumps
from bson.binary import Binary
from bson.code import Code
from bson.dbref import DBRef
from bson.errors import InvalidDocument
from bson.objectid import ObjectId
from bson.son import SON


BSON_TYPES = set([
    int,
    long,
    str,
    unicode,
    bool,
    float,
    datetime,
    ObjectId,
    type(re.compile('')),
    Code,
    type(None),
    Binary,
    DBRef,
    SON,
])


# _p_skeleton function courtesy of https://github.com/dcrosta/professor
def _p_skeleton(query_part):
    """
    Generate a "skeleton" of a document (or embedded document). A
    skeleton is a (unicode) string indicating the keys present in
    a document, but not the values, and is used to group queries
    together which have identical key patterns regardless of the
    particular values used. Keys in the skeleton are always sorted
    lexicographically.

    Raises :class:`~bson.errors.InvalidDocument` when the document
    cannot be converted into a skeleton (this usually indicates that
    the type of a key or value in the document is not known to
    Professor).

    """
    t = type(query_part)
    if t == list:
        out = []
        for element in query_part:
            sub = skeleton(element)
            if sub is not None:
                out.append(sub)
        return u'[%s]' % ','.join(out)
    elif t in (dict, SON):
        out = []
        for key in sorted(query_part.keys()):
            sub = skeleton(query_part[key])
            if sub is not None:
                out.append('%s:%s' % (key, sub))
            else:
                out.append(key)
        return u'{%s}' % ','.join(out)
    elif t not in BSON_TYPES:
        raise InvalidDocument('unknown BSON type %r' % t)


def skeleton(o):
    if isinstance(o, basestring):
        o = loads(o)
    return dumps(_p_skeleton(o))


def get_default_database(client, mongo_uri):
    return client[urlparse.urlparse(mongo_uri).path.strip('/')]
