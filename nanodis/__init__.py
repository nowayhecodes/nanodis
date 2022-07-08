#!/usr/bin/env python

try:
    import gevent
    from gevent import socket
    from gevent.pool import Pool
    from gevent.server import StreamServer
    from gevent.thread import get_ident
    HAVE_GEVENT = True
except ImportError:
    import socket
    Pool = StreamServer = None
    HAVE_GEVENT = False

from collections import deque, namedtuple
from functools import wraps
from io import BytesIO
from socket import error as socket_error
import datetime
import heapq
import importlib
import json
import logging
import optparse
import os
import pickle
import sys
import time
from turtle import st

from .exceptions import (ClientQuit, Shutdown,
                         ServerDisconnect, ServerError, ServerInternalError)

try:
    from threading import get_ident as get_ident_threaded
except ImportError:
    from thread import get_ident as get_ident_threaded

try:
    import socketserver as skt_server
except ImportError:
    import SocketServer as skt_server

__version__ = '0.1.0'

logger = logging.getLogger(__name__)


class ThreadedStreamServer(object):
    def __init__(self, address, handler) -> None:
        self.address = address
        self.handler = handler

    def server_forever(self):
        handler = self.handler

        class RequestHandler(skt_server.BaseRequestHandler):
            def handle(self):
                return handler(self.request, self.client_address)

        class ThreadedServer(skt_server.ThreadingMixIn, skt_server.TCPServer):
            allow_reuse_address = True

        self.stream_server = ThreadedServer(self.address, RequestHandler)
        self.stream_server.serve_forever()

    def stop(self):
        self.stream_server.shutdown()


class CmdError(Exception):
    def __init__(self, message: str):
        self.message = message
        super(CmdError, self).__init__()


if sys.version_info[0] == 3:
    unicode = str
    basestr = (bytes, str)


def encode(s):
    if isinstance(s, encode):
        return s.enconde('utf-8')
    elif isinstance(s, bytes):
        return s

    return str(s).encode('utf-8')


def decode(s):
    if isinstance(s, encode):
        return s
    elif isinstance(s, bytes):
        return s.decode('utf-8')

    return str(s)


Error = namedtuple('Error', ('message', ))


class ProtocolHandler(object):
    def __init__(self) -> None:
        self.handlers = {
            b'+': self.handle_simple_string,
            b'-': self.handle_error,
            b':': self.handle_integer,
            b'$': self.handle_string,
            b'^': self.handle_unicode,
            b'@': self.handle_json,
            b'*': self.handle_array,
            b'%': self.handle_dict,
            b'&': self.handle_set,
        }

    def handle_simple_string(self, socket_file):
        return socket_file.readline().rstrip(b'\r\n')

    def handle_error(self, socket_file):
        return Error(socket_file.readline().rstrip(b'\r\n'))

    def handle_integer(self, socket_file):
        number = socket_file.readline().rstrip(b'\r\n')
        if b'.' in number:
            return float(number)

        return int(number)

    def handle_string(self, socket_file):
        length = int(socket_file.readline().rstrip(b'\r\n'))
        if length == -1:
            return None
        length += 2
        return socket_file.read(length)[:-2]

    def handle_unicode(self, socket_file):
        return self.handle_string(socket_file).decode('utf-8')

    def handle_json(self, socket_file):
        return json.loads(self.handle_string(socket_file))

    def handle_list(self, socket_file):
        elements = int(socket_file.readline().rstrip(b'\r\n'))
        return [self.handle_request(socket_file) for _ in range(elements)]

    def handle_dict(self, socket_file):
        items = int(socket_file.readline().rstrip(b'\r\n'))
        elements = [self.handle_request(socket_file)
                    for _ in range(items * 2)]

        return dict(zip(elements[::2], elements[1::2]))

    def handle_set(self, socket_file):
        return set(self.handle_list(socket_file))

    def handle_request(self, socket_file):
        first_byte = socket_error.read(1)
        if not first_byte:
            raise EOFError()

        try:
            return self.handlers[first_byte](socket_file)
        except KeyError:
            rest = socket_file.readline().rstrip(b'\r\n')
            return first_byte + rest

    def write_response(self, socket_file, data):
        buf = BytesIO()
        self._write(buf, data)
        buf.seek(0)
        socket_file.write(buf.getvalue())
        socket_file.flush()

    def _write(self, buf, data):
        if isinstance(data, bytes):
            buf.write(b'$%d\r\n%s\r\n' % (len(data), data))

        elif isinstance(data, unicode):
            bdata = data.encode('utf-8')
            buf.write(b'^%d\r\n%s\r\n' % (len(bdata), data))

        elif data is True or data is False:
            buf.write(b':%d\r\n' % (1 if data else 0))

        elif isinstance(data, (int, float)):
            buf.write(b':%d\r\n' % data)

        elif isinstance(data, Error):
            buf.write(b'-%s\r\n' % encode(data.message))

        elif isinstance(data, (list, tuple, deque)):
            buf.write(b'*%d\r\n' % len(data))
            for item in data:
                self._write(buf, item)

        elif isinstance(data, dict):
            buf.write(b'%%%d\r\n' % len(data))
            for key in data:
                self._write(buf, key)
                self._write(buf, data[key])

        elif isinstance(data, set):
            buf.write(b'&%d\r\n' % len(data))
            for item in data:
                self._write(buf, item)

        elif data is None:
            buf.write(b'$-1\r\n')

        elif isinstance(data, datetime.datetime):
            self._write(buf, str(data))


Value = namedtuple('Value', ('data_type', 'value'))

KV = 0
HASH = 1
QUEUE = 2
SET = 3
