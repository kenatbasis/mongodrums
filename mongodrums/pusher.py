"""
TODO: add dtls support

"""

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
        self._push_addr = config.pusher.addr
        self._push_port = config.pusher.port

    def push(self, msg):
        try:
            self._sock.sendto(dumps(msg), (self._push_addr, self._push_port))
        except Exception:
            pass

def push(msg):
    Pusher().push(msg)

