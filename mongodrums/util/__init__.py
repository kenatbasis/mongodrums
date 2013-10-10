import urlparse

from bson.json_util import loads, dumps
from professor.skeleton import skeleton as p_skeleton, sanitize, desanitize


def skeleton(o):
    if isinstance(o, basestring):
        o = loads(o)
    return dumps(p_skeleton(o))


def get_default_database(client, mongo_uri):
    return client[urlparse.urlparse(mongo_uri).path.strip('/')]
