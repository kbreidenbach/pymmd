import errno, logging, socket, struct, time, threading, traceback, uuid
from datetime import datetime
from collections import namedtuple
from types import NoneType
import pymongo
from futures import Future

wire_version = (1,0)

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
    assert (body is not None) ^ (len(kwargs) > 0), \
        "Must pass one and only one of body or **kwargs (not both) " \
        "as channel message body (%s)"
    if len(kwargs) > 0:
        return kwargs
    return body

class MMDError(Exception, _MMDEncodable):
    def __init__(self, code, msg):
        self.code = code
        self.msg = msg
        Exception.__init__(self, "code: %s, msg: %s" % (code, msg))

    @staticmethod
    def decode(bs):
        return MMDError(code=decode_int(bs), msg=decode(bs))

    def encode_into(self, bs):
        bs.append("E")
        encode_int(self.code, bs)
        encode_into(self.msg, bs)

class _MMDReplyable(object):
    """Adds methods such as .reply() and .close() to channel messages"""
    __slots__ = ('_con',)
    def send(self, body=None, **kwargs):
        if isinstance(self, MMDChannelCreate) and self.chan_type == "call":
            mk_msg = MMDChannelClose
        else:
            mk_msg = MMDChannelMessage
        self._con.send_msg(mk_msg(chan_id = self.chan_id,
                                  body = _resolve_body(body, kwargs)))

    reply = send

    def close(self, body=None, **kwargs):
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
        self.auth_id = auth_id or uuid.uuid1()
        self.timeout = timeout

    @staticmethod
    def decode(bs):
        return MMDChannelCreate(chan_id = decode_uuid(bs),
                                chan_type =
                                {"C": "call", "S": "subscribe"}[chr(bs.pop(0))],
                                service=decode_str(bs),
                                timeout=decode_uint(bs),
                                auth_id=decode_uuid(bs),
                                body=decode(bs))

    def encode_into(self, bs):
        bs.append("C")
        bs.extend(self.chan_id.bytes)
        bs.append({"call": "C", "subcribe": "S"}[self.chan_type])
        encode_str(self.service, bs)
        encode_uint(self.timeout, bs)
        bs.extend(self.auth_id.bytes)
        encode_into(self.body, bs)

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
    b = bs.pop(0)
    shift = 0
    while b & 0x80 != 0:
        v |= ((b & 0x7f) << shift)
        shift += 7
        b = bs.pop(0)
    v |= ((b & 0x7f) << shift)
    return v

def decode_int(bs):
    v = decode_uint(bs)
    return (v >> 1) ^ -(v & 1)

def decode_uuid(bs):
    u = uuid.UUID(bytes=str(bs[:16]))
    del bs[:16]
    return u

def decode_str(bs):
    l = decode_uint(bs)
    r = bs[:l]
    del bs[:l]
    return str(r)

def decode_bytes(bs):
    l = decode_uint(bs)
    r = bs[:l]
    del bs[:l]
    return r

us_per_s = int(1e6)
def decode_datetime(bs):
    ts_us = decode_int(bs)
    dt = datetime.fromtimestamp(ts_us // us_per_s)
    return dt.replace(microsecond=ts_us % us_per_s)

def decode_map(bs):
    l = decode_uint(bs)
    d = {}
    for n in range(l):
        k = decode(bs)
        d[k] = decode(bs)
    return d

def decode_array(bs):
    r = []
    for n in range(decode_uint(bs)):
        r.append(decode(bs))
    return r

def decode_channel_close(bs):
    return {"type": "close",
            "chan_id": decode_uuid(bs),
            "body": decode(bs)}

mmd_decoders = {
    "L": decode_int,
    "l": decode_uint,
    "I": decode_int,
    "i": decode_uint,
    "T": lambda bs: True,
    "F": lambda bs: False,
    "N": lambda bs: None,
    "S": decode_str,
    "b": decode_bytes,
    "U": decode_uuid,
    "#": decode_datetime,
    "m": decode_map,
    "A": decode_array,
    "E": MMDError.decode,
    "C": MMDChannelCreate.decode,
    "X": MMDChannelClose.decode,
    "M": MMDChannelMessage.decode,
    }

def decode(bs):
    typ = chr(bs.pop(0))
    if typ in mmd_decoders:
        return mmd_decoders[typ](bs)
    else:
        raise MMDDecodeError("Don't know how to decode type '%s' (%d)" %
                             (repr(typ), ord(typ)))

def encode_uint(v, bs):
    while v > 0x7f:
        bs.append(0x80 | (v & 0x7f))
        v >>= 7
    bs.append(v)

def encode_int(v, bs):
    return encode_uint(v * 2 if v >= 0 else -v * 2 - 1, bs)

def encode_str(v, bs):
    encode_uint(len(v), bs)
    bs.extend(v)

def encode_bytes(v, bs):
    encode_uint(len(v), bs)
    bs.extend(v)

def encode_bool(v, bs):
    bs.append("T" if v else "F")

def encode_datetime(dt, bs):
    ts = int(time.mktime(dt.timetuple())) * us_per_s + dt.microsecond
    encode_int(ts, bs)

def encode_uuid(uuid, bs):
    bs.extend(uuid.bytes)

mmd_code_and_encoders = {
    int: ("L", encode_int),
    long: ("L", encode_int),
    bool: ("", encode_bool),
    NoneType: ("N", None),
    str: ("S", encode_str),
    uuid.UUID: ("U", encode_uuid),
    datetime: ("#", encode_datetime),
    bytearray: ("b", encode_bytes),
}

def encode_into(v, bs):
    if type(v) in mmd_code_and_encoders:
        code, encoder = mmd_code_and_encoders[type(v)]
        bs.extend(code)
        if encoder:
            encoder(v, bs)
    elif hasattr(v, "encode_into"):
        v.encode_into(bs)
    elif hasattr(v, "iteritems"):
        bs.append("m")
        encode_uint(len(v), bs)
        for vk, vv in v.iteritems():
            encode_into(vk, bs)
            encode_into(vv, bs)
    elif hasattr(v, "__iter__"):
        bs.append("A")
        lbs = bytearray()
        for n, e in enumerate(v):
            encode_into(e, lbs)
        encode_uint(n + 1, bs)
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
    def __init__(self, host="localhost", port=9999,
                 thread_class=threading.Thread):
        self._host = host
        self._port = port
        self._chans = {}
        self._chans_lock = threading.Lock()
        self._svcs = {}
        self._svcs_lock = threading.Lock()
        self._connect()
        self._recv_thread = thread_class(target=self._recv_loop)

        # this thread shouldn't hold-up python exit
        self._recv_thread.daemon = True

        self._recv_thread.start()

    def _connect(self):
        self._s = socket.create_connection((self._host, self._port))
        self._s.send(struct.pack("!I", len(wire_version)))
        self._s.send(bytearray(wire_version))

    def send_msg(self, m):
        if isinstance(m, MMDChannelClose):
            with self._chans_lock:
                del self._chans[m.chan_id]
        bs = m.encode()
        self._s.send(struct.pack("!I", len(bs)))
        self._s.send(bs)

    def _recv_msg(self):
        lbs = self._s.recv(4)
        l = struct.unpack("!I", lbs)[0]
        try:
            bs = bytearray(l)
            self._s.recv_into(bs, l)
        except TypeError: # support for python 2.6
            bs = bytearray(self._s.recv(l))
        m = decode(bs)
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

    def call(self, service, body=None, timeout=0, auth_id=None, **kwargs):
        f = Future()

        if auth_id is None:
            auth_id = uuid.uuid1()

        cc = MMDChannelCreate(service=service, body=body, timeout=timeout,
                              auth_id=auth_id, chan_type="call", **kwargs)
        with self._chans_lock:
            self._chans[cc.chan_id] = \
                _MMDChannel(handler=f.set, create_msg=cc)

        self.send_msg(cc)
        r = f()
        if isinstance(r, Exception):
            raise r
        return r

    def subscribe(self, handler, service, body, timeout=0, auth_id=None):
        f = Future()

        if auth_id is None:
            auth_id = uuid.uuid1()

        cc = MMDChannelCreate(service=service, body=body, timeout=timeout,
                              auth_id=auth_id, chan_type="subcribe")
        with self._chans_lock:
            self._chans[cc.chan_id] = \
                _MMDChannel(handler=handler, create_msg=cc)

        self.send_msg(cc)
        cc._con = self
        return cc

    def close(self):
        self._s.close()

    def listen(self, service, handler):
        with self._svcs_lock:
            self._svcs[service] = handler
        self.serviceregistry(service)

    def __getattr__(self, name):
        return MMDRemoteService(mmd=self, path=[name])

    def __getitem__(self, key):
        return MMDRemoteService(mmd=self, path=[key])

connect = MMDConnection

class MMDRemoteService(object):
    __slots__ = ("_mmd", "_path")

    def __init__(self, mmd, path=None):
        self._mmd = mmd
        self._path = path if path else []

    @property
    def _service(self):
        return ".".join(self._path)

    def __getattr__(self, name):
        return MMDRemoteService(mmd=self._mmd, path=self._path + [name])

    def __getitem__(self, name):
        return MMDRemoteService(mmd=self._mmd, path=self._path + [name])

    def __call__(self, body=None, auth_id=None, timeout=0, **kwargs):
        return self._mmd.call(service=self._service,
                              body=_resolve_body(body, kwargs),
                              auth_id=auth_id,
                              timeout=timeout)

    def subscribe(self, handler, body=None, auth_id=None, timeout=0, **kwargs):
        return self._mmd.subscribe(handler, service=self._service,
                                   body=_resolve_body(body, kwargs),
                                   auth_id=auth_id,
                                   timeout=timeout)

class MMDService(object):
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
        except Exception, e:
            classname = "%s.%s" % (e.__class__.__module__, e.__class__.__name__)
            msg.close(MMDError(0, {"type": classname,
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
