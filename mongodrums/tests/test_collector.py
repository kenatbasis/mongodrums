import socket
import time
import ssl

import gevent
import mock

from . import BaseTest
from mongodrums.collector import Collector, CollectorRunner
from mongodrums.config import get_config
from mongodrums.sink import Sink


class _BufferSink(Sink):
    def __init__(self):
        self.msgs = []

    def send(self, data, address):
        self.msgs.append((data, address))


class CollectorTest(BaseTest):
    def setUp(self):
        super(CollectorTest, self).setUp()
        self._server = None

    def tearDown(self):
        super(CollectorTest, self).tearDown()
        self._stop_server()

    def _start_server(self, sinks=None):
        if self._server is None or not self._server.is_alive():
            self._server = CollectorRunner(sinks)
            self._server.start()

    def _stop_server(self):
        if self._server is not None and self._server.is_alive():
            self._server.stop()
            self._server.join()
        self._server = None

    def test_handle(self):
        config = get_config()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        addr = (config.collector.addr, config.collector.port)
        sink = _BufferSink()
        for i in xrange(5):
            sock.sendto('blah', addr)
        self._start_server([sink])
        # FIXME: adding an event signaling that a socket is bound and ready to
        #        receive data at the end of BaseServer.start does not seem to
        #        work, falling back to sleep for the time being
        time.sleep(1)
        for i in xrange(5):
            sock.sendto('blah', addr)
            time.sleep(.1)
        self._stop_server()
        for i in xrange(5):
            sock.sendto('blah', addr)
            time.sleep(.1)
        self.assertEqual([x[0] for x in sink.msgs], ['blah'] * 5)

