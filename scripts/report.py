import argparse
import logging
import json
import os
import sys

from pymongo import MongoClient

from mongodrums.collection import (
    SessionCollection, IndexProfileCollection, QueryProfileCollection
)
from mongodrums.util import get_default_database


_DEFAULT_URI = 'mongodb://localhost:27017/mongodrums'


class Report(object):
    def __init__(self, database, current_indexes, output_stream, session=None):
        self._database = database
        self._current_indexes = current_indexes
        self._output_stream = output_stream
        self._session = session

    def build(self):
        index_col = \
            IndexProfileCollection(
                self._database[IndexProfileCollection.get_collection_name()])
        query = {} if self._session is None else {'session': self._session}
        for doc in index_col.find_iter(query):
            if 'BasicCursor' in doc['index']:
                continue
            index = doc['index'].split()[1]
            try:
                self._current_indexes[doc['collection']][index]['query_count'] = \
                    len(doc['queries'])
                self._current_indexes[doc['collection']][index]['used_count'] = \
                    sum([q['count'] for q in doc['queries']])
            except KeyError:
                logging.warning('skipping index %s on collection %s...' %
                                (index, doc['collection']))

    def _print(self, str_):
        self._output_stream.write(str_ + '\n')

    def dump_mark_down(self):
        self._print('# index use by collection')
        for col in sorted(self._current_indexes.keys()):
            self._print('\n## %s' % (col))
            for name, index in sorted(self._current_indexes[col].items(),
                                      key=lambda x: x[1].get('used_count', 0)):
                self._print('\n### `%s`' % (name))
                if index.get('used_count', 0) == 0:
                    self._print('* used *NEVER*')
                else:
                    self._print('* used %d times by %d quer%s' %
                                (index['used_count'], index['query_count'],
                                 ['y', 'ies'][index['query_count'] > 0]))
            self._print('\n---\n')


def run_report(args):
    client = MongoClient(args.uri)
    database = client[args.database] if args.database is not None \
                                     else get_default_database(client,
                                                               args.uri)

    index_dump = json.loads(open(args.indexes).read())
    indexes = {}
    for entry in index_dump:
        col = entry['ns'].split('.')[-1]
        if col not in indexes:
            indexes[col] = {}
        indexes[col][entry['name']] = entry

    stream = sys.stdout if args.out is None else open(args.out, 'w')

    report = Report(database, indexes, stream, args.session)
    report.build()
    report.dump_mark_down()

    stream.close()

def main():
    parser = argparse.ArgumentParser('generate a report from '
                                     'instrumentation data')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='log debug output [default: %(default)s')
    parser.add_argument('-s', '--session', metavar='SESSION',
                        help='the instrumentation session to report on '
                             '[default: <all sessions>]')
    parser.add_argument('-d', '--database', metavar='DB',
                        help='the database to connect to [default: '
                             '%(default)s]')
    parser.add_argument('-o', '--out', metavar='PATH',
                        help='where to write the report [default: stdout]')
    parser.add_argument('-u', '--uri', metavar='URI', default=_DEFAULT_URI,
                        help='mongod/database to connect to (-d overrides the '
                             'database in the uri) [default: %(default)s]')
    parser.add_argument('indexes', metavar='INDEXES',
                        help='a json file containing all current indexes (use '
                             'dump_indexes.js on the target server to '
                             'generate this file')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    run_report(args)

    return 0


if __name__ == '__main__':
    sys.exit(main())

