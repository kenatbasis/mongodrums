import socket

from bson.json_util import dumps

from .config import get_config, register_update_callback

class Pusher(object):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super(cls, Pusher).__new__(cls, *args, **kwargs)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self._push_addr = None
            self._push_port = None
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._configure(get_config())
            register_update_callback(self._configure)

    def _configure(self, config):
        self._push_addr = config.collector_addr
        self._push_port = config.collector_port

    def push(self, explain, source):
        try:
            self._sock.sendto(dumps({'explain': explain, 'source': source}),
                              (self._push_addr, self._push_port))
        except Exception:
            pass

def push(explain, source):
    Pusher().push(explain, source)

