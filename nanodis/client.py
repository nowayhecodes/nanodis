from collections import namedtuple

from nanodis import ProtocolHandler, SocketPool
from exceptions import ServerDisconnect, ServerInternalError, CmdError

Error = namedtuple('Error', ('message', ))


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
