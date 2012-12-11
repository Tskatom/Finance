#!/usr/bin/env python

import sys
import __builtin__
import zmq
import zmq.ssh
import json
import logging
import hashlib
import os
import os.path
import codecs
import datetime

log = logging.getLogger(__name__)
conf = {}

def init(args=None):
    # init logger
    # load/get the config
    # eventually this needs a search path for the config
    # should be env(QFU_CONFIG);./queue.conf;/etc/embers/queue.conf;tcp://localhost:3473
    # use 3473 as the global control channel
    global conf

    cf = None
    if args and vars(args).get('queue_conf', None):
        log.debug('trying queue config %s', vars(args)['queue_conf'])
        cf = vars(args)['queue_conf']

    if not (cf and os.path.exists(cf)):
        log.debug('trying queue config %s', os.path.join(os.getcwd(), 'queue.conf'))
        cf = os.path.join(os.getcwd(), 'queue.conf')
        
    if not (cf and os.path.exists(cf)):
        log.debug('trying queue config %s', os.path.join(os.environ.get('HOME', '.'), 'queue.conf'))
        cf = os.path.join(os.environ.get('HOME', '.'), 'queue.conf')
        
    if not (cf and os.path.exists(cf)):
        log.debug('trying queue config %s', os.path.join(os.path.dirname(sys.argv[0]), 'queue.conf'))
        cf = os.path.join(os.path.dirname(sys.argv[0]), 'queue.conf')
        
    if not (cf and os.path.exists(cf)):
        log.warn('Could not find queue.conf, bailing out.')
        return

    try:
        with __builtin__.open(cf, 'r') as f:
            conf = json.load(f)
    except Exception as e:
        log.exception("Could not find or load config file %s", (cf,))
    
    log.debug('loaded config=%s from "%s"', conf, cf)

class JsonMarshal(object):
    def __init__(self, encoding='utf8'):
        # raises an error if you get a bogus encoding
        codecs.lookup(encoding) 
        self.encoding = encoding

    def encode(self, obj):
        return json.dumps(obj, encoding=self.encoding)

    def decode(self, data):
        return json.loads(data, encoding=self.encoding)

    def send(self, socket, data):
        socket.send_unicode(data, encoding=self.encoding)

    def recv(self, socket):
        return socket.recv_unicode(encoding=self.encoding)

class UnicodeMarshal(JsonMarshal):
    def __init__(self, **kw):
        super(JsonMarshal, self).__init__(**kw)

    def encode(self, obj):
        return unicode(obj)

    def decode(self, data):
        # exception if this is not decodeable (str, stream etc.)
        return unicode(data)

    # send and recv are handled in JsonMarshall

class RawMarshal(object):
    def encode(self, obj):
        return obj

    def decode(self, obj):
        return obj

    def send(self, socket, data):
        socket.send(data)

    def recv(self, socket):
        return socket.recv()

class StreamCaptureProbe(object):
    def __init__(self, encoding='utf8', stream=sys.stdout):
        self._s = codecs.getwriter(encoding)(stream)
        self._s.flush() # make sure its good

    def __call__(self, action, message):
        if action == Queue.SENT:
            self._s.write(message)
            self._s.write('\n')
            self._s.flush()

class QueueStatsProbe(object):
    def __init__(self, interval_min=5):
        self.interval = datetime.timedelta(minutes=interval_min)
        self.start = datetime.datetime.now()
        self.sent_bytes = 0
        self.sent_msg = 0
        self.recv_bytes = 0
        self.recv_msg = 0

    def __call__(self, action, message):
        if action == Queue.SENT:
            self.sent_bytes += len(message)
            self.sent_msg += 1

        if action == Queue.RECEIVED:
            self.recv_bytes += len(message)
            self.recv_msg += 1

        # TODO - if delta past period report the stats

class Queue(object):
    # constants
    SENT = 1
    RECEIVED = 2

    def __init__(self, addr, qtype, 
                 context=None, 
                 marshal=JsonMarshal(), 
                 hwm=10000, 
                 ssh_key=None, 
                 ssh_conn=None,
                 use_paramiko=True):
        '''
        addr - ZMQ-style URL
        qtype - ZMQ queue type constant (e.g. zmq.PUB)
        context - ZMQ context (one is created if None)
        marshal - a marshaller for the messages (encode, decode, recv and send)
        hwm - ZMQ High Water Mark (default 10000 messages)
        ssh_key - optional SSH key file for SSH tunnel connections
        ssh_conn - optional SSH connection string (user@hostname) for SSH tunnel connections
        use_paramiko - use the Paramiko SSH library if True.
        '''
        # cope with lists
        if isinstance(addr, basestring):
            self._addr = [addr]
        else:
            self._addr = addr 

        self._qtype = qtype
        self._context = context or zmq.Context()
        self._err = None
        self._socket = None
        self._marshal = marshal
        self._hwm = hwm
        self._ssh_key = ssh_key
        self._ssh_conn = ssh_conn
        self._use_paramiko = use_paramiko
        self._probes = [] # probes for tracing events
        # invariants
        assert self._marshal, "No marshaller defined."
        assert self._addr, "No endpoint address defined."

    def open(self):
        if self._socket and not self._socket.closed:
            self.close()

        self._socket = self._context.socket(self._qtype)
        self._socket.setsockopt(zmq.HWM, self._hwm)
        
        # TODO add ssh connections
        for a in self._addr:
            if self._qtype in (zmq.PUB, zmq.REP):
                try:
                    self._socket.bind(a)
                    log.debug('bind: %s HWM=%d' % (a, self._hwm))
                except:
                    log.exception('Bind failed %s.' % a)
                    raise
            elif self._qtype in (zmq.SUB, zmq.REQ):
                try:
                    if self._ssh_key and self._ssh_conn:
                        zmq.ssh.tunnel_connection(self._socket, a, server=self._ssh_conn, keyfile=self._ssh_key, paramiko=self._use_paramiko)
                    else:
                        self._socket.connect(a)
                    log.debug('connect: %s HWM=%d' % (a, self._hwm))
                except:
                    log.exception('Connect failed %s.' % a)
                    raise

        if self._qtype == zmq.SUB:
            self._socket.setsockopt(zmq.SUBSCRIBE, '')

    def check_socket(self):
        if not self._socket:
            raise Exception('Socket not created.')

        if self._socket.closed:
            raise Exception('Socket is closed.')

        if self._err:
            raise self._err

    def close(self):
        if self._socket and not self._socket.closed:
            self._socket.close()
            self._socket = None

    ## basic file semantics
    def read(self):
        self.check_socket()
        msg = self._marshal.recv(self._socket)
        result = self._marshal.decode(msg)
        self.notify(Queue.RECEIVED, msg)
        return result

    def write(self, obj):
        self.check_socket()
        msg = self._marshal.encode(obj)
        self._marshal.send(self._socket, msg)
        self.notify(Queue.SENT, msg)
    
    ## be an iterator
    ## http://docs.python.org/library/stdtypes.html#iterator-types
    def __iter__(self):
        return self 

    def next(self):
        try:
            self.check_socket()
        except Exception:
            raise StopIteration

        return self.read()

    ## support contextmanager
    ## see http://docs.python.org/library/stdtypes.html#context-manager-types
    ## with queue.open(...) as q: ...
    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex_val, ex_trace):
        self.close()
        # tell any open control channels we are exiting
        return False

    ## probes for tracing messages
    ## this is how you can do dumps of messages as they are read/written
    ## and stuff like collecting metrics on messages
    def add_probe(self, probe):
        assert hasattr(probe, '__call__'), "Object must be callable."
        self._probes.append(probe)

    def notify(self, action, msg):
        for p in self._probes:
            try:
                p(action, msg)
            except:
                log.exception('Failed to notify probe.')

def open(name, mode='r', marshal=JsonMarshal(), capture=None):
    '''
    Open a queue with file-like semantics. E.g.:
    q = open('sample-1', 'w') - publish
    q = open('sample-1', 'r') - subscribe
    options:
    name - a queue name, either a full ZMQ-style URL or a name found in queue.conf
    mode - the queue open more. One of r (SUB), w (PUB), r+ (REP), w+ (REQ).
    marshal - class to use to marshal messages, default JsonMarshal
    capture - capture and log messages as they are sent. Can be True, or a stream, or a Capture instance.
    '''

    # this is somewhat goofy, but once you have 
    # a metaphor you might as well run it into the ground
    mode_map = {
        'r'  : zmq.SUB, # read only
        'w'  : zmq.PUB, # write only
        'r+' : zmq.REP, # read, then write
        'w+' : zmq.REQ  # write, then read
        }
    typ = mode_map.get(mode, None)
    if not typ:
        raise Exception('Mode %s is not a valid mode. Use one of r, w, r+ or w+.' % (mode))

    if isinstance(name, basestring):
        name = [name]

    addr = []
    for n in name: 
        a = n # assume full queue name, e.g. tcp://localhost:1234
        info = conf.get(n, {})
        port = info.get('port', None)
        host = info.get('host', None)
        if port:
            # bind sockets should always use * as the hostname
            if typ in (zmq.PUB, zmq.REP): 
                a = 'tcp://*:%d' % (port)
            elif host: 
                # config specifies
                a = 'tcp://%s:%d' % (host, port)
            else: 
                # connect defaults to localhost
                a = 'tcp://localhost:%d' % (port)
            
        addr.append(a)


    result = Queue(addr, typ, marshal=marshal)
    result.open()
    if capture:
        result.add_probe(StreamCaptureProbe())

    return result

#initialize the module
#init()

if __name__ == '__main__':
    pass
