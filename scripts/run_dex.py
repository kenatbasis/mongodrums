#!/usr/bin/env python

import json
import logging
import logging.handlers
import os
import signal
import subprocess
import sys
import threading
import time
import urlparse
import uuid

from argparse import ArgumentParser
from datetime import datetime
from StringIO import StringIO

import pymongo

from mongodrums.util import get_default_database
from mongodrums.util.daemon import Daemonize



should_exit = False


class DexRunner(threading.Thread):
    def __init__(self, mongo_uri, slow_query_threshold=100):
        threading.Thread.__init__(self)
        self.daemon = True

        self._mongo_uri = mongo_uri
        self._namespace = \
            urlparse.urlparse(self._mongo_uri).path.strip('/') + '.*'
        self._slow_query_threshold = str(slow_query_threshold)
        self._output = StringIO()
        self._proc = None

    @property
    def output(self):
        return self._output.getvalue()

    @property
    def pid(self):
        return self._proc.pid if self._proc is not None else None

    def run(self):
        self._proc = subprocess.Popen(
                        ['dex', '--slowms', self._slow_query_threshold, '-n',
                         self._namespace, '-p', '-w', self._mongo_uri],
                        stdout=subprocess.PIPE)
        logging.info('dex spawned')
        try:
            data, _ = self._proc.communicate()
            while len(data) > 0:
                logging.info('DEX> %s' % (data))
                self._output.write(data)
                data, _ = self._proc.communicate()
        except ValueError:
            pass

class Dex(Daemonize):
    def __init__(self, args):
        super(Dex, self).__init__(args.pid_file)
        self.args = args
        self._dex = None

    def kill(self):
        if self._dex is not None and self._dex.is_alive():
            os.kill(self._dex.pid, signal.SIGINT)

    def run(self):
        logging.info('running dex...')
        self._dex = DexRunner(self.args.monitor_uri, self.args.slowms)
        self._dex.start()
        while self._dex.is_alive():
            if should_exit:
                self.kill()
            time.sleep(.1)
        self._dex.join()
        logging.info('dex stopped...')
        if self.args.output_uri is None:
            print self._dex.output
        else:
            parts = urlparse.urlparse(self.args.output_uri)
            if parts.scheme == 'file':
                with open(parts.path, 'w') as out_file:
                    out_file.write(self._dex.output)
            elif parts.scheme == 'mongodb':
                client = pymongo.MongoClient(self.args.output_uri)
                db = get_default_database(client, args.output_uri)
                db.dex.insert({'session': self.args.session,
                               'created': datetime.utcnow(),
                               'output': json.loads(self._dex.output)})


def run_dex(args):
    dex = Dex(args)
    if args.action == 'foreground':
        dex.run()
    else:
        getattr(dex, args.action)()


def signal_handler(sig_num, frame):
    global should_exit
    if sig_num == signal.SIGINT or sig_num == signal.SIGTERM:
        should_exit = True


def main():
    parser = ArgumentParser('run and collect dex output')

    parser.add_argument('-v', '--verbose', action='store_true',
                        help='be verbose [default: %(default)s]')

    parser.add_argument(
        '-m', '--monitor-uri',
        default='mongodb://127.0.0.1:27017/foo',
        metavar='URI', help='the database to profile with dex [default: '
                            '%(default)s]')
    parser.add_argument(
        '--slowms', default=100, type=int, metavar='SLOW_MS',
        help='set the mongo profiling slow query threshold [default: '
             '%(default)s]')
    parser.add_argument(
        '-o', '--output-uri', metavar='URI',
        help='dump dex output to %(metavar)s [default: dump to stdout]')
    parser.add_argument(
        '-s', '--sesion', default=str(uuid.uuid1()), metavar='SESSION',
        help='name the current session [default: %(default)s]')
    parser.add_argument(
        '-p', '--pid-file', metavar='PATH', default='/tmp/md_dex.pid',
        help='pid file path [default: %(default)s]')
    parser.add_argument(
        '-l', '--log-file', metavar='PATH', default='/tmp/md_dex.log',
        help='log file path [default: %(default)s]')
    parser.add_argument(
        'action', default='foreground', metavar='ACTION',
        choices=['start', 'stop', 'restart', 'foreground'],
        help='control the daemon process [choices: %(choices)s; default: '
             '%(default)s]')

    args = parser.parse_args()

    level = logging.INFO if args.verbose else logging.WARNING
    logging.basicConfig(level=level)
    logger = logging.getLogger()
    logger.addHandler(
        logging.handlers.RotatingFileHandler(args.log_file, 'a', 2**20, 2))

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    run_dex(args)


if __name__ == '__main__':
    sys.exit(main())

