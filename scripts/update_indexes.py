import argparse
import copy
import os
import json
import logging
import subprocess
import sys

from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.uri_parser import parse_uri

from mongodrums.util import get_default_database


_DEFAULT_URI = 'mongodb://localhost:27017/mongodrums'


def backup_indexes(database, backup_path):
    logging.info('backing current indexes to %s...' % (backup_path))
    indexes = [d for d in database['system.indexes'].find()]
    with open(backup_path, 'w') as backup_file:
        backup_file.write(json.dumps(indexes, indent=4, sort_keys=True))


def get_index_definition(name, keys):
    if len(keys) == 1:
        return keys.keys()[0]

    definition = []
    while name:
        for key in keys:
            key_str = '%s_%d' % (key, keys[key])
            if name.startswith(key_str):
                definition.append((key, keys[key]))
                name = name[len(key_str) + 1:]
                break

    return definition


INDEX_DOC_IGNORE_KEYS = ['v', 'key', 'ns', 'name']


def get_ensure_index_kwargs(index_doc):
    return dict([(k, v) for k, v in index_doc.items()
                 if k not in INDEX_DOC_IGNORE_KEYS])


def add_indexes(database, new, old):
    logging.info('adding indexes')
    for col in new:
        logging.debug('working on column %s...' % (col))
        missing_indexes = []
        if col not in old:
            missing_indexes = new[col].keys()
        else:
            missing_indexes = set(new[col].keys()) - set(old[col].keys())
        for index in missing_indexes:
            logging.debug('adding index %s...' % (index))
            database[col].ensure_index(
                get_index_definition(index, new[col][index]['key']),
                **get_ensure_index_kwargs(new[col][index]['key']))


def drop_indexes(database, new, old):
    logging.info('dropping indexes')
    for col in old:
        logging.debug('working on column %s...' % (col))
        missing_indexes = []
        if col not in new:
            missing_indexes = old[col].keys()
        else:
            missing_indexes = set(old[col].keys()) - set(new[col].keys())
        for index in missing_indexes:
            if index == '_id_':
                continue
            logging.debug('dropping index %s...' % (index))
            database[col].drop_index(index)


def update_indexes(args):
    client = MongoClient(args.uri)
    database = client[args.database] if args.database is not None \
                                     else get_default_database(client,
                                                               args.uri)
    if args.backup is not None:
        backup_indexes(database, args.backup)

    def organize_index_list(list_):
        indexes = {}
        for index in list_:
            col = index['ns'].split('.')[-1]
            if col not in indexes:
                indexes[col] = {}
            indexes[col][index['name']] = index
        return indexes

    new_indexes = organize_index_list(json.loads(open(args.indexes).read()))
    old_indexes = \
        organize_index_list([d for d in database['system.indexes'].find()])

    add_indexes(database, new_indexes, old_indexes)
    drop_indexes(database, new_indexes, old_indexes)


def main():
    parser = argparse.ArgumentParser('update all indexes in a database given '
                                     'an existing dump of `db.system.indexes`')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='log debug output [default: %(default)s]')
    parser.add_argument('-d', '--database', metavar='DB',
                        help='the database to connect to [default: '
                             '%(default)s]')
    parser.add_argument('-u', '--uri', metavar='URI', default=_DEFAULT_URI,
                        help='mongod/database to connect to (-d overrides the '
                             'database in the uri) [default: %(default)s]')
    parser.add_argument('-b', '--backup', metavar='PATH',
                        help='backup the current indexes to %(metavar)s '
                             'before updating [default: <no backup>]')
    parser.add_argument('indexes', metavar='INDEXES',
                        help='a json file containing all current indexes (use '
                             'dump_indexes.js on the target server to '
                             'generate this file')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    update_indexes(args)

    return 0


if __name__ == '__main__':
    sys.exit(main())

