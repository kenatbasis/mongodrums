import select
import socket
import threading

from bson import ObjectId
from bson.json_util import dumps

from . import BaseTest
from mongodrums.config import get_config, configure
from mongodrums.pusher import push


class _TestCollector(threading.Thread):
    def __init__(self, address):
        threading.Thread.__init__(self)
        self._sock = socket.socket(socket.AF_INET,
                                   socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
        self._sock.bind(address)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.msg = None

    def run(self):
        rlist, wlist, xlist = select.select([self._sock], [], [], 10)
        try:
            self.msg = rlist[0].recv(4096)
        except IndexError:
            pass
        self._sock.close()


class PusherTest(BaseTest):
    def setUp(self):
        super(PusherTest, self).setUp()
        self._config = get_config()
        self._collector = _TestCollector((self._config.collector.addr,
                                          self._config.collector.port))

    def test_push(self):
        self._collector.start()
        msg = {'blah': {'blah': ObjectId()}}
        push(msg)
        self._collector.join()
        self.assertEqual(dumps(msg),
                         self._collector.msg)

    def test_push_reconfigure(self):
        pass
