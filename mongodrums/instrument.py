import inspect
import random
import socket
import Queue
import threading

from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from functools import partial, update_wrapper
from types import MethodType

import pymongo

from bunch import Bunch

from .config import (
    configure, get_config, register_update_callback, unregister_update_callback
)
from .pusher import push


class Wrapper(object):
    __metaclass__ = ABCMeta

    _lock = threading.RLock()

    def __init__(self, func):
        self._lock = threading.RLock()
        self._func = func
        self._configure(get_config())

    def _configure(self, config):
        self._frequency = config.instrument.sample_frequency
        self._filter_packages = config.instrument.filter_packages

    def __get__(self, owner, owner_type):
        if owner is None:
            return self
        return partial(self, owner)

    @abstractmethod
    def __call__(self, *args, **kwargs):
        pass

    def get_source(self):
        frame = \
            filter(lambda f: f[0].f_globals['__package__'] \
                             not in self._filter_packages,
                   inspect.stack()[2:])[0]
        try:
            return '%s:%d' % (frame[1], frame[2])
        finally:
            del frame

    @classmethod
    @abstractmethod
    def wrap(cls):
        pass

    @classmethod
    @abstractmethod
    def unwrap(cls):
        pass

    @classmethod
    @contextmanager
    def instrument(cls):
        cls.wrap()
        try:
            yield
        finally:
            cls.unwrap()

# TODO: extract source line from stack

class FindWrapper(Wrapper):
    def __call__(self, self_, *args, **kwargs):
        curs = self._func(self_, *args, **kwargs)
        if random.random() < self._frequency:
            push({'type': 'explain',
                  'function': 'find',
                  'database': self_.database.name,
                  'explain': curs.explain(),
                  'source': self.get_source()})
        return curs

    @classmethod
    def wrap(cls):
        with cls._lock:
            if not isinstance(pymongo.collection.Collection.find, cls):
                pymongo.collection.Collection.find = \
                    cls(pymongo.collection.Collection.find)
                register_update_callback(
                    pymongo.collection.Collection.find._configure)

    @classmethod
    def unwrap(cls):
        with cls._lock:
            if isinstance(pymongo.collection.Collection.find, cls):
                unregister_update_callback(
                    pymongo.collection.Collection.find._configure)
                pymongo.collection.Collection.find = \
                    pymongo.collection.Collection.find._func


class UpdateWrapper(Wrapper):
    def __call__(self, self_, *args, **kwargs):
        if random.random() < self._frequency:
            curs = self_.find(args[0])
            push({'type': 'explain',
                  'function': 'update',
                  'database': self_.database.name,
                  'explain': curs.explain(),
                  'source': self.get_source()})
        return self._func(self_, *args, **kwargs)

    @classmethod
    def wrap(cls):
        with cls._lock:
            if not isinstance(pymongo.collection.Collection.update, cls):
                pymongo.collection.Collection.update = \
                    cls(pymongo.collection.Collection.update)
                register_update_callback(
                    pymongo.collection.Collection.update._configure)

    @classmethod
    def unwrap(cls):
        with cls._lock:
            if isinstance(pymongo.collection.Collection.update, cls):
                unregister_update_callback(
                    pymongo.collection.Collection.update._configure)
                pymongo.collection.Collection.update = \
                    pymongo.collection.Collection.update._func


def start(config=None):
    if config is not None:
        configure(config)
    FindWrapper.wrap()
    UpdateWrapper.wrap()


def stop():
    FindWrapper.unwrap()
    UpdateWrapper.unwrap()


@contextmanager
def instrument(config=None):
    start(config)
    try:
        yield
    finally:
        stop()

