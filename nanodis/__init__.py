#!/usr/bin/env python

try:
    import gevent
    from gevent import socket
    from gevent.pool import Pool
    from gevent.server import StreamServer
    from gevent.thread import get_ident
    from numba import njit()
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


@njit(parallel=True)
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


@njit(parallel=True)
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


@njit(parallel=True)
class QueueServer(object):
    def __init__(self, host='127.0.0.1',
                 port=33737, max_clients=1024, use_gevent=True) -> None:
        self._host = host
        self._port = port
        self._max_clients = max_clients

        if use_gevent:
            if Pool is None or StreamServer is None:
                raise Exception('gevent not installed. Please install gevent '
                                'or instantiate QueueServer with '
                                'use_gevent=False')

            self._pool = Pool(max_clients)
            self._server = StreamServer(
                (self._host, self._port),
                self.connection_handler,
                spawn=self._pool
            )

        else:
            self._server = ThreadedStreamServer(
                (self._host, self._port),
                self.connection_handler)

        self._commands = self.get_commands()
        self._protocol = ProtocolHandler()

        self._kv = {}
        self._schedule = []
        self._expiry = []
        self._expiry_map = {}

        self._active_connections = 0
        self._commands_processed = 0
        self._command_errors = 0
        self._connections = 0

    def check_expired(self, key, ts=None):
        ts = ts or time.time()
        return key in self._expiry_map and ts > self._expiry_map[key]

    def unexpire(self, key):
        self._expiry_map.pop(key, None)

    def clean_expired(self, ts=None):
        ts = ts or time.time()
        n = 0
        while self._expiry:
            expires, key = heapq.heappop(self._expiry)
            if expires > ts:
                heapq.heappush(self._expiry, (expires, key))
                break

            if self._expiry_map.get(key) == expires:
                del self._expiry_map[key]
                del self._kv[key]
                n += 1
        return n

    def enforce_datatype(data_type, set_missing=True, subtype=None):
        def decorator(method):
            @wraps(method)
            def inner(self, key, *args, **kwargs):
                self.check_datatype(data_type, key, set_missing, subtype)
                return method(self, key, *args, **kwargs)
            return inner
        return decorator

    def check_datatype(self, data_type, key, set_missing=True, subtype=None):
        if key in self._kv and self.check_expired(key):
            del self._kv[key]

        if key in self._kv:
            value = self._kv[key]
            if value.data_type != data_type:
                raise CmdError('Operation agains wrong key type.')
            if subtype is not None and not isinstance(value.value, subtype):
                raise CmdError('Operation agains wrong key type.')
        elif set_missing:
            if data_type == HASH:
                value = {}
            elif data_type == QUEUE:
                value = deque()
            elif data_type == SET:
                value = set()
            elif data_type == KV:
                value = ''
            self._kv[key] = Value(data_type, value)

    def _get_state(self):
        return {'kv': self._kv, 'schedule': self._schedule}

    def _set_state(self, state, merge=False):
        if not merge:
            self._kv = state['kv']
            self._schedule = state['schedule']
        else:
            def merge(original, updates):
                original.update(updates)
                return original
            self._kv = merge(state['kv'], self._kv)
            self._schedule = state['schedule']

    def save_to_disk(self, filename):
        with open(filename, 'wb') as f:
            pickle.dump(self._get_state(), f, pickle.HIGHEST_PROTOCOL)
        return True

    def load_from_disk(self, filename, merge=False):
        if not os.path.exists(filename):
            return False
        with open(filename, 'rb') as f:
            state = pickle.load(f)
        self._set_state(state, merge=merge)
        return True

    def merge_from_disk(self, filename):
        return self.load_from_disk(filename, merge=True)

    def get_commands(self):
        timestamp_re = (r'(?P<timestamp>\d{4}-\d{2}-\d{2} '
                        '\d{2}:\d{2}:\d{2}(?:\.\d+)?)')
        return dict({
            # Queue cmds
            (b'LPUSH', self.lpush),
            (b'RPUSH', self.rpush),
            (b'LPOP', self.lpop),
            (b'RPOP', self.rpop),
            (b'LREM', self.lrem),
            (b'LLEN', self.llen),
            (b'LINDEX', self.lindex),
            (b'LRANGE', self.lrange),
            (b'LSET', self.lset),
            (b'LTRIM', self.ltrim),
            (b'RPOPLPUSH', self.rpopl_plush),
            (b'LFLUSH', self.lflush),

            # KV cmds
            (b'APPEND', self.kv_append),
            (b'DECR', self.kv_decr),
            (b'DECRBY', self.kv_decrby),
            (b'DELETE', self.kv_delete),
            (b'EXISTS', self.kv_exists),
            (b'GET', self.kv_get),
            (b'GETSET', self.kv_getset),
            (b'INCR', self.kv_incr),
            (b'INCRBY', self.kv_incrby),
            (b'MDELETE', self.kv_mdelete),
            (b'MGET', self.kv_mget),
            (b'MPOP', self.kv_mpop),
            (b'MSET', self.kv_mset),
            (b'MSETEX', self.kv_msetex),
            (b'POP', self.kv_pop),
            (b'SET', self.kv_set),
            (b'SETNX', self.kv_setnx),
            (b'SETEX', self.kv_setex),
            (b'LEN', self.kv_len),
            (b'FLUSH', self.kv_flush),

            # Hash cmds.
            (b'HDEL', self.hdel),
            (b'HEXISTS', self.hexists),
            (b'HGET', self.hget),
            (b'HGETALL', self.hgetall),
            (b'HINCRBY', self.hincrby),
            (b'HKEYS', self.hkeys),
            (b'HLEN', self.hlen),
            (b'HMGET', self.hmget),
            (b'HMSET', self.hmset),
            (b'HSET', self.hset),
            (b'HSETNX', self.hsetnx),
            (b'HVALS', self.hvals),

            # Set cmd.
            (b'SADD', self.sadd),
            (b'SCARD', self.scard),
            (b'SDIFF', self.sdiff),
            (b'SDIFFSTORE', self.sdiffstore),
            (b'SINTER', self.sinter),
            (b'SINTERSTORE', self.sinterstore),
            (b'SISMEMBER', self.sismember),
            (b'SMEMBERS', self.smembers),
            (b'SPOP', self.spop),
            (b'SREM', self.srem),
            (b'SUNION', self.sunion),
            (b'SUNIONSTORE', self.sunionstore),

            # Schedule cmd.
            (b'ADD', self.schedule_add),
            (b'READ', self.schedule_read),
            (b'FLUSH_SCHEDULE', self.schedule_flush),
            (b'LENGTH_SCHEDULE', self.schedule_length),

            # Misc.
            (b'EXPIRE', self.expire),
            (b'INFO', self.info),
            (b'FLUSHALL', self.flush_all),
            (b'SAVE', self.save_to_disk),
            (b'RESTORE', self.restore_from_disk),
            (b'MERGE', self.merge_from_disk),
            (b'QUIT', self.client_quit),
            (b'SHUTDOWN', self.shutdown),
        })

    def expire(self, key, seconds):
        eta = time.time() + seconds
        self._expiry_map[key] = eta
        heapq.heappush(self._expiry, (eta, key))

    @enforce_datatype(QUEUE)
    def lpush(self, key, *values):
        self._kv[key].value.extend_left(values)
        return len(values)

    @enforce_datatype(QUEUE)
    def rpush(self, key, *values):
        self._kv[key].value.extend(values)
        return len(values)

    @enforce_datatype(QUEUE)
    def lpop(self, key):
        try:
            return self._kv[key].value.popleft()
        except IndexError:
            pass

    @enforce_datatype(QUEUE)
    def rpop(self, key):
        try:
            return self._kv[key].value.pop()
        except IndexError:
            pass

    @enforce_datatype(QUEUE)
    def lrem(self, key, value):
        try:
            self._kv[key].value.remove(value)
        except ValueError:
            return 0
        else:
            return 1

    @enforce_datatype(QUEUE)
    def llen(self, key):
        return len(self._kv[key].value)

    @enforce_datatype(QUEUE)
    def lindex(self, key, idx):
        try:
            return self._kv[key].value[idx]
        except IndexError:
            pass

    @enforce_datatype(QUEUE)
    def lset(self, key, idx, value):
        try:
            self._kv[key].value[idx] = value
        except IndexError:
            return 0
        else:
            return 1

    @enforce_datatype(QUEUE)
    def ltrim(self, key, begin, end):
        trimmed = list(self._kv[key].value)[begin:end]
        self._kv[key] = Value(QUEUE, deque(trimmed))
        return len(trimmed)

    @enforce_datatype(QUEUE)
    def rpop_lpush(self, src, dest):
        self.check_datatype(QUEUE, dest, set_missing=True)
        try:
            self._kv[dest].value.append_left(self._kv[src].value.pop())
        except IndexError:
            return 0
        else:
            return 1

    @enforce_datatype(QUEUE)
    def lrange(self, key, begin, end=None):
        return list(self._kv[key].value)[begin:end]

    @enforce_datatype(QUEUE)
    def lflush(self, key):
        qlen = len(self._kv[key].value)
        self._kv[key].value.clear()
        return qlen
