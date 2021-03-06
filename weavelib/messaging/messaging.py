import json
import logging
import socket
from threading import Lock, Event, Thread
from uuid import uuid4
try:
    from Queue import Queue
except ImportError:
    from queue import Queue

import weavelib
from weavelib.exceptions import ProtocolError, WeaveException, ObjectClosed


logger = logging.getLogger(__name__)


def exception_to_message(ex):
    msg = Message("result")
    msg.headers["RES"] = ex.err_msg()
    if ex.extra is not None:
        msg.headers["ERRMSG"] = str(ex.extra)
    return msg


def parse_message(lines):
    required_fields = {"OP"}
    fields = {}
    for line in lines:
        line_parts = line.split(" ", 1)
        if len(line_parts) != 2:
            raise ProtocolError("Bad message line.")
        fields[line_parts[0]] = line_parts[1]

    if required_fields - set(fields.keys()):
        raise ProtocolError("Required fields missing.")

    if "MSG" in fields:
        try:
            obj = json.loads(fields["MSG"])
        except json.decoder.JSONDecodeError:
            raise ProtocolError("Bad JSON.")
        task = obj
        del fields["MSG"]
    else:
        task = None
    msg = Message(fields.pop("OP"), task)
    msg.headers = fields
    return msg


def serialize_message(msg):
    msg_lines = [
        "OP " + msg.op,
    ]

    for key, value in msg.headers.items():
        msg_lines.append(key + " " + str(value))

    if msg.task is not None:
        msg_lines.append("MSG " + json.dumps(msg.task))
    msg_lines.append("")  # Last newline before blank line.
    return "\n".join(msg_lines)


def read_message(conn):
    # Reading group of lines
    lines = []
    while True:
        line = conn.readline()
        stripped_line = line.strip()
        if not line:
            # If we have read a line at least, raise InvalidMessageStructure,
            # else IOError because mostly the socket was closed.
            if lines:
                raise ProtocolError("Invalid message.")
            else:
                raise IOError
        if not stripped_line:
            break
        lines.append(stripped_line.decode("UTF-8"))
    return parse_message(lines)


def write_message(conn, msg):
    conn.write((serialize_message(msg) + "\n").encode())
    conn.flush()


def raise_message_exception(err, extra):
    known_exceptions = weavelib.exceptions
    objects = [getattr(known_exceptions, x) for x in dir(known_exceptions)]
    exceptions = [x for x in objects if isinstance(x, type)]
    known_exceptions = {x for x in exceptions if issubclass(x, WeaveException)}
    responses = {c().err_msg(): c for c in known_exceptions}
    responses["OK"] = None

    ex = responses.get(err, WeaveException)
    if ex:
        raise ex(extra)


def ensure_ok_message(msg):
    if msg.op != "result" or "RES" not in msg.headers:
        raise ProtocolError("Bad response.")
    raise_message_exception(msg.headers["RES"], msg.headers.get("ERRMSG"))


class Message(object):
    def __init__(self, op, msg=None):
        self.op = op
        self.headers = {}
        self.json = msg

    @property
    def operation(self):
        return self.op

    @property
    def task(self):
        return self.json


class MessageWaiter(object):
    CONNECTION_CLOSED = object()

    def __init__(self):
        self.queue = Queue()

    @property
    def message(self):
        item = self.queue.get()
        if item is self.CONNECTION_CLOSED:
            raise IOError("Connection closed.")
        return item

    @message.setter
    def message(self, msg):
        self.queue.put(msg)

    def close(self):
        self.message = self.CONNECTION_CLOSED


class WeaveConnection(object):
    PORT = 11023
    READ_BUF_SIZE = -1
    WRITE_BUF_SIZE = 10240

    def __init__(self, host="localhost", port=PORT, auto_discover=True):
        self.default_host = host
        self.default_port = port
        self.auto_discover = auto_discover
        self.sock = None
        self.rfile = None
        self.wfile = None
        self.readers_lock = Lock()
        self.readers = {}
        self.reader_thread = Thread(target=self.read_loop)
        self.send_lock = Lock()
        self.active = False

    @staticmethod
    def local():
        return WeaveConnection(auto_discover=False)

    @staticmethod
    def discover():
        result = discover_message_server()
        if not result:
            raise WeaveException("Unable to discover server.")

        host, port = result
        return WeaveConnection(host, port, auto_discover=False)

    def connect(self):
        self.sock = self.socket_connect()
        self.rfile = self.sock.makefile('rb', self.READ_BUF_SIZE)
        self.wfile = self.sock.makefile('wb', self.WRITE_BUF_SIZE)
        self.active = True
        self.reader_thread.start()

    def socket_connect(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((self.default_host, self.default_port))
            return sock
        except IOError:
            sock.close()

        if not self.auto_discover:
            raise WeaveException("Unable to connect to the Server.")

        discovery_result = discover_message_server()
        if discovery_result is None:
            raise WeaveException("Unable to connect to Server.")

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(discovery_result)
            return sock
        except IOError:
            sock.close()
            raise WeaveException("Unable to connect to Server.")

    def write_message(self, msg, session_id):
        msg.headers["SESS"] = session_id
        with self.readers_lock:
            waiter = self.readers.get(session_id)
            if waiter is None:
                waiter = MessageWaiter()
                self.readers[session_id] = waiter

        self.send_internal(msg)
        response = waiter.message
        ensure_ok_message(response)
        return response

    def read_message(self, msg, session_id):
        msg.headers["SESS"] = session_id
        with self.readers_lock:
            waiter = self.readers.get(session_id)
            if waiter is None:
                waiter = MessageWaiter()
                self.readers[session_id] = waiter

        self.send_internal(msg)
        response = waiter.message

        if response.op == "inform":
            return response
        elif response.op == "result":
            ensure_ok_message(response)
            return response
        else:
            raise ProtocolError("Bad Response")

    def send_internal(self, msg):
        with self.send_lock:
            write_message(self.wfile, msg)

    def read_loop(self):
        while self.active:
            try:
                msg = read_message(self.rfile)
            except IOError:
                logger.error("Connection closed. Stopping reading.")
                self.close()
                break
            session_id = msg.headers.get("SESS")
            waiter = self.readers.get(session_id)
            if waiter is None:
                logger.warning("Dropping message to: %s. No waiter found.",
                               serialize_message(msg))
                continue

            waiter.message = msg

    def interrupt_session(self, session_id):
        with self.readers_lock:
            waiter = self.readers.get(session_id)
            if waiter is None:
                return
            waiter.close()

    def close(self):
        self.active = False
        for waiter in self.readers.values():
            waiter.close()

        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass

        for item in (self.rfile, self.wfile, self.sock):
            try:
                item.close()
            except Exception:
                pass


class Sender(object):
    def __init__(self, conn, channel, **kwargs):
        self.channel = channel
        self.extra_headers = {x.upper(): y for x, y in kwargs.items()}
        self.conn = conn
        self.session_id = "sender-session-" + str(uuid4())

    def start(self):
        pass

    def send(self, obj, headers=None):
        if isinstance(obj, Message):
            msg = obj
        else:
            msg = Message("push", obj)

        msg.headers.update(self.extra_headers)
        if headers:
            msg.headers.update(headers)

        msg.headers["C"] = self.channel
        return self.conn.write_message(msg, self.session_id)

    def close(self):
        pass


class Receiver(object):
    def __init__(self, conn, channel, **kwargs):
        self.channel = channel
        self.conn = conn
        self.extra_headers = {x.upper(): y for x, y in kwargs.items()}
        self.session_id = "receiver-session-" + str(uuid4())
        self.active = False

    def start(self):
        pass

    def receive(self):
        response = self.conn.read_message(self.prepare_receive_message(),
                                          self.session_id)
        self.preprocess(response)
        return response

    def prepare_receive_message(self):
        pop_msg = Message("pop")
        pop_msg.headers["SESS"] = self.session_id
        pop_msg.headers["C"] = self.channel
        pop_msg.headers.update(self.extra_headers)
        return pop_msg

    def run(self):
        self.active = True
        while self.active:
            try:
                msg = self.receive()
                self.on_message(msg.task, msg.headers)
            except IOError:
                if not self.active:
                    return
                raise
            except ObjectClosed:
                logger.error("Channel closed: " + self.channel)
                self.stop()
                break

    def stop(self):
        self.active = False
        self.conn.interrupt_session(self.session_id)

    def preprocess(self, msg):
        if "AUTH" in msg.headers:
            try:
                msg.headers["AUTH"] = json.loads(msg.headers["AUTH"])
            except:
                pass

    def on_message(self, msg, headers):
        pass


def discover_message_server():
    IP, PORT = "<broadcast>", 23034
    client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client.bind(('', 0))
    client.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    client.sendto("QUERY".encode('UTF-8'), (IP, PORT))

    client.settimeout(10)
    try:
        data, addr = client.recvfrom(1024)
    except socket.timeout:
        return None

    try:
        obj = json.loads(data.decode())
        return obj["host"], obj["port"]
    except (KeyError, ValueError):
        return None
