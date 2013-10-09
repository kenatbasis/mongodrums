#!/usr/bin/env python

import logging
import logging.handlers
import os
import signal
import sys
import time
import uuid

from argparse import ArgumentParser

from mongodrums.collector import CollectorRunner
from mongodrums.config import get_config, update
from mongodrums.sink import QueryProfileSink, IndexProfileSink
from mongodrums.util.daemon import Daemonize


should_exit = False


class Collector(Daemonize):
    def __init__(self, args):
        super(Collector, self).__init__(args.pid_file)
        self.args = args

    def run(self):
        logging.info('running collector...')
        update({'collector': {
                    'session': self.args.session,
                    'mongo_uri': self.args.uri,
                    'addr': self.args.addr,
                    'port': self.args.port
                },
                'index_profile_sink': {
                    'mongo_uri': self.args.uri
                 },
                'query_profile_uri': {
                    'mongo_uri': self.args.uri
                }})
        collector = CollectorRunner([IndexProfileSink(), QueryProfileSink()])
        collector.start()
        while not should_exit:
            time.sleep(.1)
        collector.stop()
        collector.join()
        logging.info('collector stopped...')


def run_collector(args):
    collector = Collector(args)
    if args.action == 'foreground':
        collector.run()
    else:
        getattr(collector, args.action)()


def signal_handler(sig_num, frame):
    global should_exit
    if sig_num == signal.SIGINT or sig_num == signal.SIGTERM:
        should_exit = True


def main():
    config = get_config()

    parser = ArgumentParser('run collector')

    parser.add_argument('-v', '--verbose', action='store_true',
                        help='be verbose [default: %(default)s]')
    parser.add_argument(
        '-u', '--uri', default='mongodb://127.0.0.1:27017/mongodrums',
        metavar='URI', help='the database to push collected profiling stats '
                            'to [default: %(default)s]')
    parser.add_argument(
        '-s', '--session', default=str(uuid.uuid1()), metavar='SESSION',
        help='name the current session [default: %(default)s]')
    parser.add_argument(
        '-p', '--pid-file', metavar='PATH', default='/tmp/md_collector.pid',
        help='pid file path [default: %(default)s]')
    parser.add_argument(
        '-l', '--log-file', metavar='PATH', default='/tmp/md_collector.log',
        help='log file path [default: %(default)s]')
    parser.add_argument(
        'action', default='foreground', metavar='ACTION',
        choices=['start', 'stop', 'restart', 'foreground'],
        help='control the daemon process [choices: %(choices)s; default: '
             '%(default)s]')
    parser.add_argument(
        '--port', default=config.collector.port, type=int, metavar='PORT',
        help='the port to listen on [default: %(default)s]')
    parser.add_argument(
        '--addr', default=config.collector.addr, metavar='ADDR',
        help='the address to listen on [default: %(default)s]')

    args = parser.parse_args()

    level = logging.INFO if args.verbose else logging.WARNING
    logging.basicConfig(level=level)
    logger = logging.getLogger()
    logger.addHandler(
        logging.handlers.RotatingFileHandler(args.log_file, 'a', 2**20, 2))

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    run_collector(args)


if __name__ == '__main__':
    sys.exit(main())

