import logging, math, os, socket, struct, sys, time, threading, traceback
from cStringIO import StringIO
import uuid
import datetime
from collections import namedtuple
from types import NoneType
import futures

class MMDEncodeError(Exception):
    pass

class MMDDecodeError(Exception):
    pass

class MMDUnsupported(Exception):
    pass

class _MMDEncodable(object):
    """Adds an encode() method to classes that impliment an
       encode_into() method"""
    __slots__ = ()
    def encode(self):
        bs = bytearray()
        self.encode_into(bs)
        return bs

def _resolve_body(body, kwargs):
    assert not ((body is not None) and (len(kwargs) > 0)), \
        "Must pass one and only one of body or **kwargs (not both) " \
        "as channel message body (%s)"
    if len(kwargs) > 0:
        return kwargs
    return body

class MMDErrorCode(object):
    __slots__ = ('code','name')

    def __init__(self, code, name):
        self.code = code
        self.name = name

    def __repr__(self):
        return 'MMDErrorCode(%d, "%s")' % (self.code, self.name)

errors = [
    MMDErrorCode(0, "UNKNOWN"),
    MMDErrorCode(1, "SERVICE_NOT_FOUND"),
    MMDErrorCode(2, "IMPROPER_RESPONSE_TYPE"),
    MMDErrorCode(3, "BROKER_CONNECTION_CLOSED"),
    MMDErrorCode(4, "SERVICE_ERROR"),
    MMDErrorCode(5, "UNEXPECTED_REMOTE_CHANNEL_CLOSE"),
    MMDErrorCode(6, "INVALID_REQUEST"),
    MMDErrorCode(7, "AUTHENTICATION_ERROR"),
    MMDErrorCode(8, "CHANNEL_ADMIN_CLOSED"),
    MMDErrorCode(9, "INVALID_CHANNEL"),
]

this_mod = sys.modules[__name__]
for error in errors:
    setattr(this_mod, error.name, error)

error_code_to_MMDErrorCode = dict((error.code, error) for error in errors)

class MMDError(Exception, _MMDEncodable):
    def __init__(self, code, msg):
        if type(code) is int:
            code = error_code_to_MMDErrorCode[code]
        self.code = code
        self.msg = msg

    def __repr__(self):
        return "MMDError(code=%s, msg=%s)" % (repr(self.code), repr(self.msg))

    __str__ = __repr__

    @staticmethod
    def decode(bs):
        return MMDError(code=decode_int(bs), msg=decode(bs))

    @staticmethod
    def decode_fast(bs):
        return MMDError(code=decode(bs), msg=decode(bs))

    def encode_into(self, bs):
        if codec_version == "1.0":
            bs.append("E")
            encode_int(self.code.code, bs)
        elif codec_version == "1.1":
            bs.append("e")
            encode_fast_int(self.code.code, bs)
        else:
            raise MMDEncodeError("Unsupported codec version: %s" %
                                 codec_version)
        encode_into(self.msg, bs)

class Security(object):

    @staticmethod
    def decode_key(key):
        assert key[1] == ":"
        if key[0] == "S":
            return Stock(str(key[2:]))
        elif key[0] == "F":
            return Future(str(key[2:]))
        elif key[0] == "O":
            parts = str(key[2:]).split(":")
            yy = int(parts[1][:2])
            mm = int(parts[1][2:4])
            dd = int(parts[1][4:])
            yyyy = datetime.datetime.now().year // 100 * 100 + yy
            return Option(parts[0], yyyy, mm, dd,
                          float(parts[2]) / 1000, parts[3])
        else:
            raise MMDDecodeError("Unknown SecurityKey type: '%s'" % key)

    @staticmethod
    def decode_id(bs):
        typ_sym_sz = ord(bs.read(1))
        typ = typ_sym_sz >> 5
        sym_sz = typ_sym_sz & 0xf
        if typ == 0:
            sym = Stock(bs.read(sym_sz))
            bs.seek(16 - 1 - sym_sz, os.SEEK_CUR)
            return sym
        elif typ == 1:
            cp = "C" if ((typ_sym_sz >> 4) & 0x1) == 0 else "P"
            month_day = ord(bs.read(1))
            day_year = ord(bs.read(1))
            year = 1970 + (day_year & 0x7f)
            month = month_day >> 4
            day = (month_day & 0xf) << 1 | day_year >> 7
            security = bs.read(sym_sz)
            strike = struct.unpack("!I", bs.read(4))[0] / 1000.0
            bs.seek(16 - 3 - sym_sz - 4, os.SEEK_CUR)
            return Option(security, year, month, day, strike, cp)
        elif typ == 2:
            fut = Future(bs.read(sym_sz))
            bs.seek(16 - 1 - sym_sz, os.SEEK_CUR)
            return fut
        else:
            raise MMDDecodeError("Unknown SecurityId type: %d" % typ)

class Stock(Security):
    __slots__ = ('symbol',)
    def __init__(self, symbol):
        self.symbol = symbol

    def __repr__(self):
        return "Stock(symbol='%s')" % self.symbol

    def __str__(self):
        return "S:" + self.symbol

    def encode_into(self, bs):
        bs.append("$")
        # should be ORed with 0<<5, but this has no effect so we leave
        # it out for performance reasons
        bs.append(len(self.symbol))
        bs.extend(self.symbol)
        bs.extend("\0" * (16 - 1 - len(self.symbol)))

class Option(Security):
    __slots__ = ('security', 'year', 'month', 'day', 'strike', 'call_put')
    def __init__(self, security, year, month, day, strike, call_put):
        self.security = security
        self.year = year
        self.month = month
        self.day = day
        self.strike = strike
        self.call_put = call_put

    def __repr__(self):
        return ("Option(security='%s', year=%d, month=%d, day=%d, "
                "strike=%.3f, call_put='%s')" %
                (self.security, self.year, self.month, self.day,
                 self.strike, self.call_put))

    def __str__(self):
        return ("O:%s:%.4d%.2d%.2d:%.2f:%s" %
                (self.security, self.year, self.month, self.day,
                 self.strike, self.call_put))

    def shifted_strike(self):
        return int(math.ceil(self.strike * 1000.0))

    def encode_into(self, bs):
        bs.append("$")
        # 32 comes from 1<<5 where 1 is for option
        bs.append(32 |
                  (0 if self.call_put in ('C', 'c') else 1) << 4 |
                  len(self.security))
        bs.append((self.month << 4) | (self.day >> 1))
        bs.append(((self.day & 0x01) << 7) |
                  (self.year - 1970) & 0x7f)
        bs.extend(self.security)
        bs.extend(struct.pack("!I", self.shifted_strike()))
        bs.extend("\0" * (16 - 3 - len(self.security) - 4))

class Future(Security):
    __slots__ = ('symbol',)
    def __init__(self, symbol):
        self.symbol = symbol

    def __repr__(self):
        return "Future(symbol='%s')" % self.symbol

    def __str__(self):
        return "F:%s" % self.symbol

    def encode_into(self, bs):
        bs.append("$")
        # 64 comes from 2<<5 where 2 is for future
        bs.append(64 | len(self.symbol))
        bs.extend(self.symbol)
        bs.extend("\0" * (16 - 1 - len(self.symbol)))

class _MMDReplyable(object):
    """Adds methods such as .reply() and .close() to channel messages"""
    __slots__ = ('_con',)
    def send(self, body=None, **kwargs):
        """Send a ChannelMessage (or ChannelClose if this is a call channel)"""
        if isinstance(self, MMDChannelCreate) and self.chan_type == "call":
            mk_msg = MMDChannelClose
        else:
            mk_msg = MMDChannelMessage
        self._con.send_msg(mk_msg(chan_id = self.chan_id,
                                  body = _resolve_body(body, kwargs)))

    reply = send
    __call__ = send

    def close(self, body=None, **kwargs):
        """Close the associated channel."""
        self._con.send_msg(MMDChannelClose(chan_id = self.chan_id,
                                           body = _resolve_body(body, kwargs)))

class _SlotsRepr(object):
    def __repr__(self):
        slots = ["%s=%s" % (s, repr(getattr(self, s))) for s in self.__slots__]
        return "%s(%s)" % (self.__class__.__name__, ", ".join(slots))

class MMDChannelCreate(_MMDReplyable, _MMDEncodable, _SlotsRepr):
    __slots__ = ('chan_id', 'chan_type', 'service', 'body',
                 'auth_id', 'timeout')

    def __init__(self, service, chan_type, body=None, chan_id=None,
                auth_id=None, timeout=0, **kwargs):
        self.service=service
        self.body=_resolve_body(body, kwargs)
        self.chan_type=chan_type
        self.chan_id = chan_id or uuid.uuid1()
        self.auth_id = auth_id or uuid.UUID(int=0)
        self.timeout = timeout

    @staticmethod
    def decode(bs):
        return MMDChannelCreate(chan_id = decode_uuid(bs),
                                chan_type =
                                {"C": "call",
                                 "S": "subscribe"}[bs.read(1)],
                                service=decode_str(bs),
                                timeout=decode_uint(bs),
                                auth_id=decode_uuid(bs),
                                body=decode(bs))

    @staticmethod
    def decode_fast(bs):
        return MMDChannelCreate(chan_id = decode_uuid(bs),
                                chan_type =
                                {"C": "call",
                                 "S": "subscribe"}[bs.read(1)],
                                service=bs.read(ord(bs.read(1))),
                                timeout=struct.unpack("!H", bs.read(2)),
                                auth_id=decode_uuid(bs),
                                body=decode(bs))

    def encode_into(self, bs):
        if codec_version == "1.0":
            bs.append("C")
            bs.extend(self.chan_id.bytes)
            bs.append({"call": "C", "subscribe": "S"}[self.chan_type])
            encode_str(self.service, bs)
            encode_uint(self.timeout, bs)
            bs.extend(self.auth_id.bytes)
            encode_into(self.body, bs)
        elif codec_version == "1.1":
            bs.append("c")
            bs.extend(self.chan_id.bytes)
            bs.append({"call": "C", "subscribe": "S"}[self.chan_type])
            bs.append(len(self.service))
            bs.extend(self.service)
            bs.extend(struct.pack("!H", self.timeout))
            bs.extend(self.auth_id.bytes)
            encode_into(self.body, bs)
        else:
            raise MMDEncodeError("Unsupported codec version: %s" %
                                 codec_version)


class MMDChannelMessage(_MMDEncodable, _MMDReplyable, _SlotsRepr):
    __slots__ = ('chan_id', 'body')

    def __init__(self, chan_id, body):
        self.chan_id = chan_id
        self.body = body

    @staticmethod
    def decode(bs):
        return MMDChannelMessage(chan_id = decode_uuid(bs), body = decode(bs))

    def encode_into(self, bs):
        bs.append('M')
        bs.extend(self.chan_id.bytes)
        encode_into(self.body, bs)

class MMDChannelClose(_MMDEncodable, _MMDReplyable, _SlotsRepr):
    __slots__ = ('chan_id', 'body')

    def __init__(self, chan_id, body):
        self.chan_id = chan_id
        self.body = body

    @staticmethod
    def decode(bs):
        return MMDChannelClose(chan_id = decode_uuid(bs), body = decode(bs))

    def encode_into(self, bs):
        bs.append('X')
        bs.extend(self.chan_id.bytes)
        encode_into(self.body, bs)

def decode_uint(bs):
    v = 0
    b = ord(bs.read(1))
    shift = 0
    while b & 0x80 != 0:
        v |= ((b & 0x7f) << shift)
        shift += 7
        b = ord(bs.read(1))
    v |= ((b & 0x7f) << shift)
    return v

def decode_int(bs):
    v = decode_uint(bs)
    return (v >> 1) ^ -(v & 1)

u_szs = {1: "!B", 2: "!H", 4: "!I", 8: "!Q"}
def decode_int_u(bs, n):
    return struct.unpack(u_szs[n], bs.read(n))[0]

s_szs = {1: "!b", 2: "!h", 4: "!i", 8: "!q"}
def decode_int_s(bs, n):
    return struct.unpack(s_szs[n], bs.read(n))[0]

def decode_int_u1(bs): return decode_int_u(bs, 1)
def decode_int_u2(bs): return decode_int_u(bs, 2)
def decode_int_u4(bs): return decode_int_u(bs, 4)
def decode_int_u8(bs): return decode_int_u(bs, 8)

def decode_int_s1(bs): return decode_int_s(bs, 1)
def decode_int_s2(bs): return decode_int_s(bs, 2)
def decode_int_s4(bs): return decode_int_s(bs, 4)
def decode_int_s8(bs): return decode_int_s(bs, 8)

def decode_double(bs):
    return struct.unpack("!d", bs.read(8))[0]

def decode_float(bs):
    return struct.unpack("!f", bs.read(4))[0]

def decode_uuid(bs):
    return uuid.UUID(bytes=(bs.read(16)))

def decode_str(bs):
    return bs.read(decode_uint(bs))

def decode_fast_str(bs):
    return bs.read(decode(bs))

def decode_byte(bs):
    return bs.read(1)

def decode_bytes(bs):
    return bytearray(bs.read(decode_uint(bs)))

def decode_fast_bytes(bs):
    return bytearray(bs.read(decode(bs)))

us_per_s = int(1e6)
def decode_datetime(bs):
    ts_us = decode_int(bs)
    dt = datetime.datetime.fromtimestamp(ts_us // us_per_s)
    return dt.replace(microsecond=ts_us % us_per_s)

def decode_fast_datetime(bs):
    ts_us = decode_int_s(bs, 8)
    dt = datetime.datetime.fromtimestamp(ts_us // us_per_s)
    return dt.replace(microsecond=ts_us % us_per_s)

def decode_map(bs):
    l = decode_uint(bs)
    d = {}
    for n in range(l):
        k = decode(bs)
        d[k] = decode(bs)
    return d

def decode_fast_map(bs):
    d = {}
    for n in range(decode(bs)):
        k = decode(bs)
        d[k] = decode(bs)
    return d

def decode_array(bs):
    r = []
    for n in range(decode_uint(bs)):
        r.append(decode(bs))
    return r

def decode_fast_array(bs):
    sz = decode(bs)
    r = []
    for n in range(sz):
        r.append(decode(bs))
    return r

def decode_channel_close(bs):
    return {"type": "close",
            "chan_id": decode_uuid(bs),
            "body": decode(bs)}

mmd_decoders = {
    "\x00": lambda bs: 0,
    "\x01": decode_int_s1,
    "\x02": decode_int_s2,
    "\x04": decode_int_s4,
    "\x08": decode_int_s8,
    "\x10": lambda bs: 0,
    "\x11": decode_int_u1,
    "\x12": decode_int_u2,
    "\x14": decode_int_u4,
    "\x18": decode_int_u8,
    "L": decode_int,
    "l": decode_uint,
    "I": decode_int,
    "i": decode_uint,
    "D": decode_double,
    "d": decode_float,
    "T": lambda bs: True,
    "F": lambda bs: False,
    "N": lambda bs: None,
    "S": decode_str,
    "B": decode_byte,
    "b": decode_bytes,
    "U": decode_uuid,
    "#": decode_datetime,
    "m": decode_map,
    "A": decode_array,
    "$": Security.decode_id,
    "E": MMDError.decode,
    "C": MMDChannelCreate.decode,
    "c": MMDChannelCreate.decode_fast,
    "X": MMDChannelClose.decode,
    "M": MMDChannelMessage.decode,
    "r": decode_fast_map,
    "a": decode_fast_array,
    "s": decode_fast_str,
    "e": MMDError.decode_fast,
    "q": decode_fast_bytes,
    "z": decode_fast_datetime,
    }

def decode(bs):
    typ = bs.read(1)
    if typ in mmd_decoders:
        return mmd_decoders[typ](bs)
    else:
        raise MMDDecodeError("Don't know how to decode type '%s' (%d): %s" %
                             (repr(typ), ord(typ), repr(bs)))

def encode_uint(v, bs):
    while v > 0x7f:
        bs.append(0x80 | (v & 0x7f))
        v >>= 7
    bs.append(v)

def encode_int(v, bs):
    return encode_uint(v * 2 if v >= 0 else -v * 2 - 1, bs)

def encode_fast_int(v, bs):
    if v == 0:
        bs.append(0)
        return

    if -128 <= v < 128:
        bs.append(0x01)
        fmt = "!b"
    elif -32768 <= v < 32768:
        bs.append(0x02)
        fmt = "!h"
    elif -2147483648 <= v < 2147483648:
        bs.append(0x04)
        fmt = "!i"
    elif -9223372036854775808 <= v < 9223372036854775808:
        bs.append(0x08)
        fmt = "!q"
    else:
        raise MMDEncodeError("Integer value too large to encode: %s" % v)
    bs.extend(struct.pack(fmt, v))

def encode_double(v, bs):
    return bs.extend(struct.pack("!d", v))

def encode_float(v, bs):
    return bs.extend(struct.pack("!f", v))

def encode_str(v, bs):
    encode_uint(len(v), bs)
    bs.extend(v)

def encode_fast_str(v, bs):
    encode_fast_int(len(v), bs)
    bs.extend(v)

def encode_bytes(v, bs):
    encode_uint(len(v), bs)
    bs.extend(v)

def encode_fast_bytes(v, bs):
    encode_fast_int(len(v), bs)
    bs.extend(v)

def encode_bool(v, bs):
    bs.append("T" if v else "F")

def encode_datetime(dt, bs):
    ts = int(time.mktime(dt.timetuple())) * us_per_s + dt.microsecond
    encode_int(ts, bs)

def encode_fast_datetime(dt, bs):
    ts = int(time.mktime(dt.timetuple())) * us_per_s + dt.microsecond
    bs.extend(struct.pack("!q", ts))

def encode_uuid(uuid, bs):
    bs.extend(uuid.bytes)

def encode_fast_array(a, bs):
    encode_fast_int(len(a), bs)
    for e in a:
        encode_into(e, bs)

def date_to_dt(d):
    return datetime.datetime.combine(d, datetime.time())

mmd_code_and_encoders = {
    "1.0": {
        int: ("L", encode_int),
        long: ("L", encode_int),
        float: ("D", encode_double),
        bool: ("", encode_bool),
        NoneType: ("N", None),
        str: ("S", encode_str),
        unicode: ("S", lambda u, bs: encode_str(str(u), bs)),
        uuid.UUID: ("U", encode_uuid),
        datetime.datetime: ("#", encode_datetime),
        datetime.date: ("#", lambda d, bs: encode_datetime(date_to_dt(d), bs)),
        bytearray: ("b", encode_bytes),
        },
    "1.1": {
        int: ("", encode_fast_int),
        long: ("", encode_fast_int),
        float: ("D", encode_double),
        bool: ("", encode_bool),
        NoneType: ("N", None),
        str: ("s", encode_fast_str),
        unicode: ("s", lambda u, bs: encode_fast_str(str(u), bs)),
        uuid.UUID: ("U", encode_uuid),
        datetime.datetime: ("z", encode_fast_datetime),
        datetime.date: ("#", lambda d, bs: encode_datetime(date_to_dt(d), bs)),
        bytearray: ("q", encode_fast_bytes),
        list: ("a", encode_fast_array),
        },
}

codec_version = "1.1"

def encode_into(v, bs):
    if type(v) in mmd_code_and_encoders[codec_version]:
        code, encoder = mmd_code_and_encoders[codec_version][type(v)]
        bs.extend(code)
        if encoder:
            encoder(v, bs)
    elif hasattr(v, "encode_into"):
        v.encode_into(bs)
    elif hasattr(v, "iteritems"):
        if codec_version == "1.0":
            bs.append("m")
            encode_uint(len(v), bs)
        elif codec_version == "1.1":
            bs.append("r")
            encode_fast_int(len(v), bs)
        else:
            raise MMDEncodeError("Unsupported codec version: %s" %
                                 codec_version)
        for vk, vv in v.iteritems():
            encode_into(vk, bs)
            encode_into(vv, bs)
    elif hasattr(v, "__iter__"):
        lbs = bytearray()
        n = -1
        for n, e in enumerate(v):
            encode_into(e, lbs)

        if codec_version == "1.0":
            bs.append("A")
            encode_uint(n + 1, bs)
        elif codec_version == "1.1":
            bs.append("a")
            encode_fast_int(n + 1, bs)
        else:
            raise MMDEncodeError("Unsupported codec version: %s" %
                                 codec_version)
        bs.extend(lbs)
    else:
        raise MMDEncodeError(
            "Don't know how to encode value: type: %s, repr: %s" %
            (type(v), repr(v)))

def encode(v):
    bs = bytearray()
    encode_into(v, bs)
    return bs

_MMDChannel = namedtuple('_MMDChannel', ['handler', 'create_msg'])

class MMDConnection(object):
    """MMDConnection(host="localhost", post=9999, \
thread_class=threading.Thread)

Abstracts the connection to mmd. This class spawns 2 reader threads and
is written to allow multiple channels to be in-flight at the same
time. One reader thread handles service requests, while the other thread
handles client requests.

Example: calling echo2:
  >>> c = pymmd.connect()
  >>> c.echo2("Hello, World!")

Example: subscribing to services.
  >>> c = pymmd.connect()
  >>> class ServicesHandler:
  ...     def handle_message(self, msg):
  ...         print "Got update from services: %s" % msg.body
  ... sh = ServicesHandler()
  c.services.subscribe(handler=sh)
"""
    def __init__(self, host="localhost", port=9999,
                 thread_class=threading.Thread,
                 inst=""):
        if len(inst) > 0:
            inst = "." + inst
        self._s = MMDRealConnection(host, port, thread_class,
                                    inst="services"+inst)
        self._c = MMDRealConnection(host, port, thread_class,
                                    inst="clients"+inst)

    def call(self, service, body=None,
             timeout=0, auth_id=None, handler=None, **kwargs):
        return self._c.call(service, body, timeout, auth_id, handler, **kwargs)

    def subscribe(self, handler, service, body, timeout=0, auth_id=None):
        return self._c.subscribe(handler, service, body, timeout, auth_id)

    def close(self):
        self._s.close()
        self._c.close()

    def register(self, service, handler):
        return self._s.register(service, handler)

    def unregister(self, service):
        return self._s.unregister(service)

    def send_msg(self, msg):
        return self._c.send_msg(msg)

    # 'listen' is deprecated, please use 'register' instead
    listen = register

    def __getattr__(self, name):
        return MMDRemoteService(service=self._s, client=self._c, path=[name])

    def __getitem__(self, key):
        return MMDRemoteService(service=self._s, client=self._c, path=[key])

class MMDRealConnection(object):
    """MMDRealConnection(host="localhost", post=9999, \
thread_class=threading.Thread)

Abstracts the connection to mmd. This class spawns a reader thread and
is written to allow multiple channels to be in-flight at the same
time.

Do not use this class directly. Use MMDConnection instead, unless you can
guarantee that services live remotely from clients or vice versa.

Example: calling echo2:
  >>> c = pymmd.connect()
  >>> c.echo2("Hello, World!")

Example: subscribing to services.
  >>> c = pymmd.connect()
  >>> class ServicesHandler:
  ...     def handle_message(self, msg):
  ...         print "Got update from services: %s" % msg.body
  ... sh = ServicesHandler()
  c.services.subscribe(handler=sh)
"""
    def __init__(self, host="localhost", port=9999,
                 thread_class=threading.Thread, inst=""):
        self._host = host
        self._port = port
        self._chans = {}
        self._chans_lock = threading.Lock()
        self._svcs = {}
        self._svcs_lock = threading.Lock()
        self._inst = inst
        self._connect()
        self._recv_thread = thread_class(target=self._recv_loop)

        # this thread shouldn't hold-up python exit
        self._recv_thread.daemon = True

        self._recv_thread.start()

    def _connect(self):
        self._s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        self._s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._s.connect((self._host, self._port))
        wire_version = [int(v) for v in codec_version.split(".")]
        bs = bytearray(wire_version) + "pymmd." + self._inst
        self._s.send(struct.pack("!I", len(bs)))
        self._s.send(bs)

    def send_msg(self, m):
        if isinstance(m, MMDChannelClose):
            with self._chans_lock:
                del self._chans[m.chan_id]
        bs = bytearray(4)
        m.encode_into(bs)
        bs[0:4] = struct.pack("!I", len(bs) - 4)
        self._s.send(bs)

    def _recv_len(self, l):
        bs = bytearray()
        while len(bs) < l:
            rbs = self._s.recv(l - len(bs))
            assert len(rbs) > 0
            bs.extend(rbs)
        return bs

    def _recv_msg(self):
        lbs = self._recv_len(4)
        l = struct.unpack("!I", str(lbs))[0]
        bs = self._recv_len(l)
        m = decode(StringIO(bs))
        m._con = self
        return m

    def _recv_loop(self):
        while True:
            m = self._recv_msg()
            with self._chans_lock:
                chan = self._chans.get(m.chan_id, None)
                if type(m) is MMDChannelClose and chan is not None:
                    del self._chans[m.chan_id]
            if chan is not None:
                chan.handler(m)
                continue

            if type(m) is MMDChannelCreate:
                with self._svcs_lock:
                    svc_handler = self._svcs.get(m.service, None)
                if svc_handler is not None:
                    with self._chans_lock:
                        self._chans[m.chan_id] = \
                            _MMDChannel(handler=svc_handler, create_msg=None)
                    svc_handler(m)
                    continue

            logging.warn("MMD: Got unhandled msg: %s" % repr(m))

    def call(self, service, body=None,
             timeout=0, auth_id=None, handler=None, **kwargs):
        """Call (as opposed to Subscribe) a service.

If no handler is given the call is synchronous (for the calling thread),
otherwise the handler should be a function/method that will be called
when the result comes back.

It might produce cleaner code to use 'c.myservice(myargs)' rather then
'c.call("myservice", myargs)'"""
        h = handler
        if handler is None:
            f = futures.Future()
            h = f.set

        if auth_id is None:
            auth_id = uuid.UUID(int=0)

        cc = MMDChannelCreate(service=service, body=body, timeout=timeout,
                              auth_id=auth_id, chan_type="call", **kwargs)
        with self._chans_lock:
            self._chans[cc.chan_id] = _MMDChannel(handler=h, create_msg=cc)

        self.send_msg(cc)
        if handler is None:
            r = f()
            if isinstance(r, Exception):
                raise r
            return r

    def subscribe(self, handler, service, body, timeout=0, auth_id=None):
        """Create a subscription channel.

Handler should be an object that as the following (callback) methods:

    def set_channel(self, ch):
        '''Called with the channel object which can be used to send
           messages (ch.send(msg) or ch.reply(msg) or ch(msg)) or
           to close the channel (ch.close(msg))'''

    def handle_message(self, msg):
        '''Called whenever the other side sends a message'''

    def handle_close(self, msg):
        '''Called whenever the other side closes the channel'''

It might produce cleaner code to use 'c.myservice.subscribe(myargs)'
rather then 'c.subscribe("myservice", myargs)'"""
        if auth_id is None:
            auth_id = uuid.UUID(int=0)

        cc = MMDChannelCreate(service=service, body=body, timeout=timeout,
                              auth_id=auth_id, chan_type="subscribe")
        with self._chans_lock:
            self._chans[cc.chan_id] = \
                _MMDChannel(handler=handler, create_msg=cc)

        if hasattr(handler, "set_channel"):
            handler.set_channel(cc)
        self.send_msg(cc)
        cc._con = self
        return cc

    def close(self):
        """Close connection to MMD.

Note this will destroy all open channels."""
        self._s.close()

    def register(self, service, handler):
        """Register as a service.

Handler should be a function/method that will be called for each
channel create or message or close object. Quite likely you want
to the handler to be a subclass of MMDService which takes care of
a bunch of stuff for you."""
        with self._svcs_lock:
            self._svcs[service] = handler
        self.serviceregistry({"action": "register", "name": service})

    def unregister(self, service):
        """Unregister a service."""
        with self._svcs_lock:
            del self._svcs[service]
        self.serviceregistry({"action": "unregister", "name": service})

    def __getattr__(self, name):
        return MMDRemoteService(service=self, client=self, path=[name])

    def __getitem__(self, key):
        return MMDRemoteService(service=self, client=self, path=[key])

connect = MMDConnection

class MMDRemoteService(object):
    __slots__ = ("_s", "_c", "_path")

    def __init__(self, service, client, path=None):
        self._s = service
        self._c = client
        self._path = path if path else []

    @property
    def _service(self):
        return ".".join(self._path)

    def __getattr__(self, name):
        return MMDRemoteService(service=self._s, client=self._c,
                                path=self._path + [name])

    def __getitem__(self, name):
        return MMDRemoteService(service=self._s, client=self._c,
                                path=self._path + [name])

    def __call__(self, body=None, handler=None,
                 auth_id=None, timeout=0, **kwargs):
        return self._c.call(service=self._service,
                            body=_resolve_body(body, kwargs),
                            handler=handler,
                            auth_id=auth_id,
                            timeout=timeout)

    def subscribe(self, handler, body=None, auth_id=None, timeout=0, **kwargs):
        return self._c.subscribe(handler, service=self._service,
                                 body=_resolve_body(body, kwargs),
                                 auth_id=auth_id,
                                 timeout=timeout)

    def register(self, handler):
        return self._s.register(service=self._service, handler=handler)

    def unregister(self):
        return self._s.unregister(service=self._service)

    # 'listen' is deprecated, please use 'register' instead
    listen = register

class MMDService(object):
    """A base class for service handlers.

This base class makes it easier for you to write services. You can
define the following methods:

    def handle_call(self, msg):
        '''This will be called for every CreateChannel calls that you're
service receives. The return value from this function is sent back as the
call response.'''

    def handle_subscribe(self, msg):
        '''This will be called for every CreateChannel subscribe that
you're service receives.'''

    def handle_message(self, msg):
        '''This will be called for channel message you receive'''

    def handle_close(self, msg):
        '''This will be called whenever one of your channels is closed'''

If you fail to create one of these methods and a message is sent that
would have needed one of these, the offending channel is closed with
an error message indicating that you're service doesn't support such.

There is also an exception handler in place so that if your handle_*()
method raises, the exception will be logged as well as channel being
closed and the exception being encoded and sent with the channel close
message."""
    def __init__(self):
        pass

    def __call__(self, msg):
        try:
            if type(msg) is MMDChannelCreate:
                if msg.chan_type == "call":
                    msg.reply(self.handle_call(msg))
                elif msg.chan_type == "subscribe":
                    self.handle_subscribe(msg)
                else:
                    assert False, ("Unknown create type: %s, msg:" %
                                   (msg.chan_type, repr(msg)))
            elif type(msg) is MMDChannelMessage:
                self.handle_message(msg)
            elif type(msg) is MMDChannelClose:
                self.handle_close(msg)
            else:
                assert False, ("Unknown channel msg type: %s, msg: %s" %
                               (type(msg), repr(msg)))
        except MMDError, e:
            msg.close(e)
        except Exception, e:
            classname = "%s.%s" % (e.__class__.__module__, e.__class__.__name__)
            logging.error("Exception (%s(%s)) while handling mmd message. "
                          "Channel closed. mmd_msg: %s, stack: %s" %
                          (classname, e.args, msg, traceback.format_exc()))
            msg.close(MMDError(4, {"type": classname,
                                   "args": e.args,
                                   "stack": traceback.format_exc()}))

    def handle_call(self, msg):
        raise MMDUnsupported("This service (%s) doesn't support "
                             "channel calls." % self.__class__.__name__)

    def handle_subscribe(self, msg):
        raise  MMDUnsupported("This service (%s) doesn't support "
                              "channel subscribe." % self.__class__.__name__)

    def handle_message(self, msg):
        raise  MMDUnsupported("This service (%s) doesn't support "
                              "channel message." % self.__class__.__name__)

    def handle_close(self, msg):
        raise  MMDUnsupported("This service (%s) doesn't support "
                              "channel close." % self.__class__.__name__)
