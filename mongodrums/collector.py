"""
TODO: add dtls support

"""
import threading

from datetime import datetime

import pymongo
import gevent

from bson.json_util import loads
from gevent.server import DatagramServer

from mongodrums.config import get_config
from mongodrums.collection import SessionCollection


class CollectorRunner(threading.Thread):
    def __init__(self, sinks=None):
        threading.Thread.__init__(self)
        self.daemon = False

        self._server = None
        self._stop = threading.Event()
        self._sinks = [] if sinks is None else sinks

    @property
    def server(self):
        return self._server

    def _check_stopped(self):
        while not self._stop.is_set():
            gevent.sleep(.1)
        if self._server is not None:
            self._server.stop()

    def run(self):
        config = get_config()
        self._server = Collector((config.collector.addr,
                                  config.collector.port))
        for sink in self._sinks:
            self._server.add_sink(sink)
        stop_check = gevent.spawn(self._check_stopped)
        try:
            self._server.serve_forever()
        finally:
            stop_check.join()

    def stop(self):
        self._stop.set()


class Collector(DatagramServer):
    def __init__(self, listener, spawn='default'):
        DatagramServer.__init__(self, listener, self.handle, spawn)
        self._sinks = []
        self._session = get_config().collector.session

    def add_sink(self, sink):
        self._sinks.append(sink)

    def _run(self):
        if self._session is not None:
            col = SessionCollection(pymongo.MongoClient(
                        get_config().collector.mongo_uri).session)
            col.insert({'name': self._session, 'start_time': datetime.utcnow()})

    def handle(self, data, address):
        if isinstance(data, basestring):
            try:
                if data.strip()[0] == '{':
                    data = loads(data)
                    data.update({'session': self._session})
            except (ValueError, IndexError):
                pass
        for sink in self._sinks:
            sink.handle(data, address)

