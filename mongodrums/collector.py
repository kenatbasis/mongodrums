"""
TODO: add dtls support

"""

from bson.json_util import loads
from gevent.server import DatagramServer

from mongodrums.config import get_config


class Collector(DatagramServer):
    def __init__(self, listener, spawn='default'):
        DatagramServer.__init__(self, listener, self.handle, spawn)
        self._sinks = []
        self._session = get_config().collector.session

    def add_sink(self, sink):
        self._sinks.append(sink)

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

