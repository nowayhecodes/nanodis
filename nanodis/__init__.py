#!/usr/bin/env python

try:
    import gevent
    from gevent import socket
    from gevent.pool import Pool
    from gevent.server import StreamServer
    from gevent.thread import get_ident
    from numba import njit
    HAVE_GEVENT = True
except ImportError:
    import socket
    Pool = StreamServer = None
    HAVE_GEVENT = False

from ast import arg
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
import socketserver as skt_server
import sys
import time
from typing import List, Set


from exceptions import (ClientQuit, Shutdown,
                        ServerDisconnect, ServerError, ServerInternalError)

try:
    from threading import get_ident as get_ident_threaded
except ImportError:
    from _thread import get_ident as get_ident_threaded


__version__ = '1.0.0'

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
            b'*': self.handle_list,
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
            (b'RPOPLPUSH', self.rpop_lpush),
            (b'LFLUSH', self.lflush),

            # KV cmds
            (b'APPEND', self.kv_append),
            (b'DECR', self.kv_decr),
            # (b'DECRBY', self.kv_decrby),
            (b'DELETE', self.kv_delete),
            (b'EXISTS', self.kv_exists),
            (b'GET', self.kv_get),
            (b'GETSET', self.kv_getset),
            (b'INCR', self.kv_incr),
            (b'INCRBY', self.kv_incr_by),
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
            (b'SINTERSEC', self.sintersec),
            (b'SINTERSTORE', self.sintersec_store),
            (b'SISMEMBER', self.sismember),
            (b'SMEMBERS', self.smembers),
            (b'SPOP', self.spop),
            (b'SREM', self.sremove),
            (b'SUNION', self.sunion),
            (b'SUNIONSTORE', self.sunion_store),

            # Schedule cmd.
            (b'ADD', self.schedule_add),
            (b'READ', self.schedule_read),
            (b'FLUSH_SCHEDULE', self.schedule_flush),
            (b'LENGTH_SCHEDULE', self.schedule_len),

            # Misc.
            (b'EXPIRE', self.expire),
            (b'INFO', self.info),
            (b'FLUSHALL', self.flush_all),
            (b'SAVE', self.save_to_disk),
            (b'RESTORE', self.load_from_disk),
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

    def kv_append(self, key, value):
        if key not in self._kv:
            self.kv_set(key, value)
        else:
            kv_value = self._kv[key]
            if kv_value.data_type == QUEUE:
                if isinstance(value, list):
                    kv_value.value.extend(value)
                else:
                    kv_value.value.append(value)
            else:
                try:
                    kv_value = Value(kv_value.data_type,
                                     kv_value.value + value)
                except:
                    raise CmdError('Incompatible data types')
        return self._kv[key].value

    def _kv_incr(self, key, n):
        if key in self._kv:
            value = self._kv[key].value + n
        else:
            value = n
        self._kv[key] = Value(KV, value)
        return value

    @enforce_datatype(KV, set_missing=False, subtype=(float, int))
    def kv_decr(self, key):
        return self._kv_incr(key, -1)

    def kv_delete(self, key):
        if key in self._kv:
            del self._kv[key]
            return 1
        return 0

    def kv_exists(self, key):
        return 1 if key in self._kv and not self.check_expired(key) else 0

    def kv_get(self, key):
        if key in self._kv and not self.check_expired(key):
            return self._kv[key].value

    def kv_getset(self, key, value):
        if key in self._kv and not self.check_expired(key):
            original = self._kv[key].value
        else:
            original = None
        self._kv[key] = Value(KV, value)
        return original

    @enforce_datatype(KV, set_missing=False, subtype=(float, int))
    def kv_incr(self, key):
        return self._kv_incr(key, 1)

    @enforce_datatype(KV, set_missing=False, subtype=(float, int))
    def kv_incr_by(self, key, n):
        return self._kv_incr(key, n)

    def kv_mdelete(self, *keys):
        n = 0
        for key in keys:
            try:
                del self._kv[key]
            except KeyError:
                pass
            else:
                n += 1
        return n

    def kv_mget(self, *keys):
        acc = []
        for key in keys:
            if key in self._kv and not self.check_expired(key):
                acc.append(self._kv[key].value)
            else:
                acc.append(None)
        return acc

    def kv_mpop(self, *keys):
        acc = []
        for key in keys:
            if key in self._kv and not self.check_expired(key):
                acc.append(self._kv.pop(key).value)
            else:
                acc.append(None)
        return acc

    def kv_mset(self, __data=None, **kw):
        n = 0
        data = {}

        if __data is not None:
            data.update(__data)
        if kw is not None:
            data.update(kw)

        for key in data:
            self.unexpire(key)
            self._kv[key] = Value(KV, data[key])
            n += 1
        return n

    def kv_msetex(self, data, expires):
        self.kv_mset(data)
        for key in data:
            self.expire(key, expires)

    def kv_pop(self, key):
        if key in self._kv and not self.check_expired(key):
            return self._kv.pop(key).value

    def kv_set(self, key, value):
        if isinstance(value, dict):
            data_type = HASH
        elif isinstance(value, list):
            data_type = QUEUE
            value = deque(value)
        elif isinstance(value, set):
            data_type = SET
        else:
            data_type = KV

        self.unexpire(key)
        self._kv[key] = Value(data_type, value)
        return 1

    def kv_setnx(self, key, value):
        if key in self._kv and not self.check_expired(key):
            return 0
        else:
            self.unexpire(key)
            self._kv[key] = Value(KV, value)
            return 1

    def kv_setex(self, key, value, expires):
        self.kv_set(key, value)
        self.expire(key, expires)
        return 1

    def kv_len(self):
        return len(self._kv)

    def kv_flush(self):
        kvlen = self.kv_len()
        self._kv.clear()
        self._expiry = []
        self._expiry_map = {}
        return kvlen

    def _decode_timestamp(self, timestamp):
        timestamp = decode(timestamp)
        fmt = '%Y-%m-%d %H:%M:%S'

        if '.' in timestamp:
            fmt = fmt + '.%f'

        try:
            return datetime.datetime.strptime(timestamp, fmt)
        except ValueError:
            raise CmdError('Timestamp must be formatted Y-m-d H:M:S')

    @enforce_datatype(HASH)
    def hdel(self, key, field):
        value = self._kv[key].value
        if field in value:
            del value[field]
            return 1
        return 0

    @enforce_datatype(HASH)
    def hexists(self, key, field):
        value = self._kv[key].value
        return 1 if field in value else 0

    @enforce_datatype(HASH)
    def hget(self, key, field):
        return self._kv[key].value.get(field)

    @enforce_datatype(HASH)
    def hgetall(self, key):
        return self._kv[key].value

    @enforce_datatype(HASH)
    def hincrby(self, key, field, incr=1):
        self._kv[key].value.setdefault(field, 0)
        self._kv[key].value[field] += incr
        return self._kv[key].value[field]

    @enforce_datatype(HASH)
    def hkeys(self, key):
        return list(self._kv[key].value)

    @enforce_datatype(HASH)
    def hlen(self, key):
        return len(self._kv[key].value)

    @enforce_datatype(HASH)
    def hmget(self, key, *fields):
        accum = {}
        value = self._kv[key].value
        for field in fields:
            accum[field] = value.get(field)
        return accum

    @enforce_datatype(HASH)
    def hmset(self, key, data):
        self._kv[key].value.update(data)
        return len(data)

    @enforce_datatype(HASH)
    def hset(self, key, field, value):
        self._kv[key].value[field] = value
        return 1

    @enforce_datatype(HASH)
    def hsetnx(self, key, field, value):
        kval = self._kv[key].value
        if field not in kval:
            kval[field] = value
            return 1
        return 0

    @enforce_datatype(HASH)
    def hvals(self, key):
        return list(self._kv[key].value.values())

    @enforce_datatype(SET)
    def sadd(self, key, *members):
        self._kv[key].value.update(members)
        return len(self._kv[key].value)

    @enforce_datatype(SET)
    def scard(self, key):
        return len(self._kv[key].value)

    @enforce_datatype(SET)
    def sdiff(self, key, *keys):
        src = set(self._kv[key].value)
        for key in keys:
            self.check_datatype(SET, key)
            src -= self._kv[key].value
        return list(src)

    @enforce_datatype(SET)
    def sdiffstore(self, dest, key, *keys):
        src = set(self._kv[key].value)
        for key in keys:
            self.check_datatype(SET, key)
            src -= self._kv[key].value
        self.check_datatype(SET, dest)
        self._kv[dest] = Value(SET, src)
        return len(src)

    @enforce_datatype(SET)
    def sintersec(self, key, *keys):
        src = set(self._kv[key].value)
        for key in keys:
            self.check_expired(SET, key)
            src &= self._kv[key].value
        return list(src)

    @enforce_datatype(SET)
    def sintersec_store(self, dest, key, *keys):
        src = set(self._kv[key].value)
        for key in keys:
            self.check_expired(SET, key)
            src &= self._kv[key].value
        self.check_datatype(SET, dest)
        self._kv[dest] = Value(SET, src)
        return len(src)

    @enforce_datatype(SET)
    def sismember(self, key, member) -> bool:
        return 1 if member in self._kv[key].value else 0

    @enforce_datatype(SET)
    def smembers(self, key) -> Set:
        return self._kv[key].value

    @enforce_datatype(SET)
    def spop(self, key, n=1):
        accum = []
        for _ in range(n):
            try:
                accum.append(self._kv[key].value.pop())
            except KeyError:
                break
        return accum

    @enforce_datatype(SET)
    def sremove(self, key, *members):
        count = 0
        for member in members:
            try:
                self._kv[key].value.remove(member)
            except KeyError:
                pass
            else:
                count += 1
        return count

    @enforce_datatype(SET)
    def sunion(self, key, *keys) -> List:
        src = set(self._kv[key].value)
        for key in keys:
            self.check_datatype(SET, key)
            src |= self._kv[key].value
        return list(src)

    @enforce_datatype(SET)
    def sunion_store(self, dest, key, *keys):
        src = set(self._kv[key].value)
        for key in keys:
            self.check_datatype(SET, key)
            src |= self._kv[key].value
        self.check_datatype(SET, dest)
        self._kv[dest] = Value(SET, src)
        return len(src)

    def schedule_add(self, timestamp, data):
        dt = self._decode_timestamp(timestamp)
        heapq.heappush(self._schedule, (dt, data))
        return 1

    def schedule_read(self, timestamp=None):
        dt = self._decode_timestamp(timestamp)
        accum = []
        while self._schedule and self._schedule[0][0] <= dt:
            ts, data = heapq.heappop(self._schedule)
            accum.append(data)
        return accum

    def schedule_flush(self):
        schedlen = self.schedule_len()
        self._schedule = []
        return schedlen

    def schedule_len(self):
        return len(self._schedule)

    def info(self):
        return {
            'active_connections': self._active_connections,
            'commands_processed': self._commands_processed,
            'command_errors': self._command_errors,
            'connections': self._connections,
            'keys': len(self._kv),
            'timestamp': time.time()
        }

    def flush_all(self):
        self.kv_flush()
        self.schedule_flush()
        return 1

    def add_command(self, cmd, callback):
        if isinstance(cmd, unicode):
            cmd = cmd.encode('utf-8')
        self._commands[cmd] = callback

    def client_quit(self):
        raise ClientQuit('client closed the connection')

    def shutdown(self):
        raise Shutdown('shuttin down')

    def run(self):
        self._server.serve_forever()

    def connection_handler(self, conn, address):
        logger.info(f'connection received: {address}')
        socket_file = conn.makefile('rwb')
        self._active_connections += 1

        while True:
            try:
                self.request_response(socket_file)
            except EOFError:
                logger.info(f'client went away: {address}')
                socket_file.close()
                break

            except ClientQuit:
                logger.info(f'Client exited: {address}')
                break

            except Exception as ex:
                logger.exception('Error processing command.')

        self._active_connections -= 1

    def request_response(self, socket_file):
        data = self._protocol.handle_request(socket_file)

        try:
            resp = self.respond(data)
        except Shutdown:
            logger.info('shutting down')
            self._protocol.write_response(socket_file, 1)
            raise KeyboardInterrupt

        except ClientQuit:
            self._protocol.write_response(socket_file, 1)
            raise

        except CmdError as cmd_error:
            resp = Error(cmd_error.message)
            self._command_errors += 1

        except Exception as ex:
            logger.exception('unhandled error')
            resp = Error('unhandled server error')

        else:
            self._commands_processed += 1

        self._protocol.write_response(socket_file, resp)

    def respond(self, data):
        if not isinstance(data, list):
            try:
                data = data.split()
            except:
                raise CmdError('unrecognized request type')

        if not isinstance(data[0], basestr):
            raise CmdError('First parameter must be a command name')

        cmd = data[0].upper()

        if cmd not in self._commands:
            raise CmdError(f'unrecognized command {cmd}')
        else:
            logger.debug(f'received {decode(cmd)}')

        return self._commands[cmd](*data[1:])


class SocketPool(object):
    def __init__(self, host, port, max_age=60) -> None:
        self.host = host
        self.port = port
        self.max_age = max_age
        self.free = []
        self.in_use = {}
        self._tid = get_ident if HAVE_GEVENT else get_ident_threaded

    def checkout(self):
        now = time.time()
        tid = self._tid()

        if tid in self.in_use:
            sock = self.in_use[tid]

            if sock.closed:
                del self.in_use[tid]
            else:
                return self.in_use[tid]

        while self.free:
            ts, sock = heapq.heappop(self.free)
            if ts < now - self.max_age:
                try:
                    sock.close()
                except OSError:
                    pass

            else:
                self.in_use[tid] = sock
                return sock

        sock = self.create_socket_file()
        self.in_use[tid] = sock
        return sock

    def create_socket_file(self):
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        conn.connect((self.host, self.port))
        return conn.makefile('rwb')

    def checkin(self):
        tid = self._tid()
        if tid in self.in_use:
            sock = self.in_use.pop(tid)
            if not sock.closed:
                heapq.heappush(self.free, (time.time(), sock))
            return True
        return False

    def close(self):
        tid = self._tid()
        sock = self.in_use.pop(tid, None)

        if sock:
            try:
                sock.close()
            except OSError:
                pass
            return True
        return False


class Client(object):
    def __init__(self, host='127.0.0.1', port=33738, pool_max_age=60) -> None:
        self._host = host
        self._port = port
        self._socket_pool = SocketPool(host, port, pool_max_age)
        self._protocol = ProtocolHandler()

    def execute(self, *args):
        conn = self._socket_pool.checkout()
        close_conn = args[0] in (b'QUIT', b'SHUTDOWN')
        self._protocol.write_response(conn, args)

        try:
            resp = self._protocol.handle_request(conn)

        except EOFError:
            self._socket_pool.close()
            raise ServerDisconnect('server went away')

        except Exception:
            self._socket_pool.close()
            raise ServerInternalError('internal server error')

        else:
            if close_conn:
                self._socket_pool.close()
            else:
                self._socket_pool.checkin()

        if isinstance(resp, Error):
            raise CmdError(resp.message)

        return resp

    def close(self):
        self.execute(b'QUIT')

    def command(cmd):
        def method(self, *args):
            return self.execute(cmd.encode('utf-8'), *args)
        return method

    lpush = command('LPUSH')
    rpush = command('RPUSH')
    lpop = command('LPOP')
    rpop = command('RPOP')
    lrem = command('LREM')
    llen = command('LLEN')
    lindex = command('LINDEX')
    lrange = command('LRANGE')
    lset = command('LSET')
    ltrim = command('LTRIM')
    rpopl_plush = command('RPOPLPUSH')
    lflush = command('LFLUSH')

    append = command('APPEND')
    decr = command('DECR')
    decrby = command('DECRBY')
    delete = command('DELETE')
    exists = command('EXISTS')
    get = command('GET')
    getset = command('GETSET')
    incr = command('INCR')
    incrby = command('INCRBY')
    mdelete = command('MDELETE')
    mget = command('MGET')
    mpop = command('MPOP')
    mset = command('MSET')
    msetex = command('MSETEX')
    pop = command('POP')
    set = command('SET')
    setex = command('SETEX')
    setnx = command('SETNX')
    length = command('LEN')
    flush = command('FLUSH')

    hdel = command('HDEL')
    hexists = command('HEXISTS')
    hget = command('HGET')
    hgetall = command('HGETALL')
    hincrby = command('HINCRBY')
    hkeys = command('HKEYS')
    hlen = command('HLEN')
    hmget = command('HMGET')
    hmset = command('HMSET')
    hset = command('HSET')
    hsetnx = command('HSETNX')
    hvals = command('HVALS')

    sadd = command('SADD')
    scard = command('SCARD')
    sdiff = command('SDIFF')
    sdiffstore = command('SDIFFSTORE')
    sinter = command('SINTER')
    sinterstore = command('SINTERSTORE')
    sismember = command('SISMEMBER')
    smembers = command('SMEMBERS')
    spop = command('SPOP')
    srem = command('SREM')
    sunion = command('SUNION')
    sunionstore = command('SUNIONSTORE')

    add = command('ADD')
    read = command('READ')
    flush_schedule = command('FLUSH_SCHEDULE')
    length_schedule = command('LENGTH_SCHEDULE')

    expire = command('EXPIRE')
    info = command('INFO')
    flushall = command('FLUSHALL')
    save = command('SAVE')
    restore = command('RESTORE')
    merge = command('MERGE')
    quit = command('QUIT')
    shutdown = command('SHUTDOWN')

    def __getitem__(self, key):
        if isinstance(key, (list, tuple)):
            return self.mget(*key)
        else:
            return self.get(key)

    def __setitem__(self, key, value):
        self.set(key, value)

    def __delitem__(self, key):
        self.delete(key)

    def __contains__(self, key):
        return self.exists(key)

    def __len__(self):
        return self.length()


def get_opt_parser():
    opt = optparse.OptionParser()

    opt.add_option('-d', '--debug', action='store_true', dest='debug')
    opt.add_option('-e', '--errors', action='store_true', dest='error')

    opt.add_option('-t', '--threaded', action='store_false',
                   default=True, dest='use_gevent')

    opt.add_option('-H', '--host', default='127.0.0.1',
                   dest='host', help='host to listen on')

    opt.add_option('-m', '--max-clients', default=1024,
                   dest='max_clients', help='maximum number of clients', type=int)

    opt.add_option('-p', '--port', default=33737, dest='port',
                   help='port to listen on', type=int)

    opt.add_option('-l', '--log-file', dest='log_file')
    opt.add_option('-x', '--extension', action='append', dest='extensions',
                   help="import path for Python extension module(s)")

    return opt


def setup_logger(options):
    logger.addHandler(logging.StreamHandler())

    if options.log_file:
        logger.addHandler(logging.FileHandler(options.log_file))

    if options.debug:
        logger.setLevel(logging.DEBUG)

    elif options.error:
        logger.setLevel(logging.ERROR)

    else:
        logger.setLevel(logging.INFO)


def load_extensions(server, extensions):
    for ext in extensions:
        try:
            module = importlib.import_module(ext)
        except ImportError:
            logger.exception(f'could not import extension {ext}')

        else:
            try:
                initialize = getattr(module, 'initialize')
            except AttributeError:
                logger.exception(
                    f'could not find "initialize" function in extension {ext}')
                raise

            else:
                initialize(server)
                logger.info(f'loaded extension {ext}')


if __name__ == '__main__':
    options, args = get_opt_parser().parse_args()

    if options.use_gevent:
        try:
            from gevent import monkey
            monkey.patch_all()
        except ImportError:
            logger.error('gevent is not installed')
            sys.stderr.write(
                f'gevent is not installed. To run using threads specify the -t parameter')
            sys.stderr.flush()
            sys.exit(1)

        setup_logger(options)

        server = QueueServer(host=options.host, port=options.port,
                             max_clients=options.max_clients, use_gevent=options.use_gevent)

        load_extensions(server, options.extensions or ())
        print(f'Nanodis {(options.host, options.port)}')

        try:
            server.run()
        except KeyboardInterrupt:
            print(f'shutting down')
