import json
import logging
import socket
from threading import Lock

import weavelib.exceptions
from weavelib.exceptions import WeaveException


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
            raise weavelib.exceptions.ProtocolError("Bad message line.")
        fields[line_parts[0]] = line_parts[1]

    if required_fields - set(fields.keys()):
        raise weavelib.exceptions.ProtocolError("Required fields missing.");

    if "MSG" in fields:
        try:
            obj = json.loads(fields["MSG"])
        except json.decoder.JSONDecodeError:
            raise weavelib.exceptions.ProtocolError("Bad JSON.")
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
                raise weavelib.exceptions.ProtocolError("Invalid message.")
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
    objects = [getattr(weavelib.exceptions, x)
                  for x in dir(weavelib.exceptions)]
    exceptions = [x for x in objects if isinstance(x, type)]
    known_exceptions = {x for x in exceptions if issubclass(x, WeaveException)}
    responses = {c().err_msg(): c for c in known_exceptions}
    responses["OK"] = None

    ex = responses.get(err, WeaveException)
    if ex:
        raise ex(extra)


def ensure_ok_message(msg):
    if msg.op != "result" or "RES" not in msg.headers:
        raise weavelib.exceptions.ProtocolError("Bad response.")
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


class Sender(object):
    PORT = 11023
    READ_BUF_SIZE = -1
    WRITE_BUF_SIZE = 10240

    def __init__(self, queue, host="localhost", **kwargs):
        self.queue = queue
        self.extra_headers = {x.upper(): y for x, y in kwargs.items()}
        self.host = host
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.send_lock = Lock()

    def start(self):
        self.sock.connect((self.host, self.PORT))
        self.rfile = self.sock.makefile('rb', self.READ_BUF_SIZE)
        self.wfile = self.sock.makefile('wb', self.WRITE_BUF_SIZE)

    def send(self, obj, headers=None):
        if isinstance(obj, Message):
            msg = obj
        else:
            msg = Message("enqueue", obj)

        msg.headers.update(self.extra_headers)
        if headers:
            msg.headers.update(headers)

        msg.headers["Q"] = self.queue

        with self.send_lock:
            write_message(self.wfile, msg)
            msg = read_message(self.rfile)
            ensure_ok_message(msg)

    def close(self):
        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()


class Receiver(object):
    PORT = 11023
    READ_BUF_SIZE = -1
    WRITE_BUF_SIZE = 10240

    def __init__(self, queue, host="localhost", **kwargs):
        self.queue = queue
        self.extra_headers = {x.upper(): y for x, y in kwargs.items()}
        self.host = host
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.active = False

    def start(self):
        self.sock.connect((self.host, self.PORT))
        self.sock.settimeout(None)

        self.rfile = self.sock.makefile('rb', self.READ_BUF_SIZE)
        self.wfile = self.sock.makefile('wb', self.WRITE_BUF_SIZE)

    def run(self):
        self.active = True

        while self.active:
            try:
                msg = self.receive()
            except weavelib.exceptions.ObjectClosed:
                logger.error("Queue closed: " + self.queue)
                self.stop()
                break
            except IOError:
                if self.active:
                    logger.exception("Encountered error. Stopping receiver.")
                break
            if msg.op == "inform":
                self.on_message(msg.task, msg.headers)
            elif msg.op == "result":
                ensure_ok_message(msg)
            else:
                logger.warning("Dropping message without data.")
                continue

            # TODO: ACK the server.

    def receive(self):
        dequeue_msg = Message("dequeue")
        dequeue_msg.headers.update(self.extra_headers)
        dequeue_msg.headers["Q"] = self.queue
        write_message(self.wfile, dequeue_msg)
        msg = read_message(self.rfile)
        if msg.op == "inform":
            self.preprocess(msg)
            return msg
        if "RES" not in msg.headers:
            raise weavelib.exceptions.ProtocolError("Bad response.")
        raise_message_exception(msg.headers["RES"], msg.headers.get("ERRMSG"))

    def stop(self):
        self.active = False
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass

        for item in (self.rfile, self.wfile, self.sock):
            try:
                item.close()
            except Exception:
                pass

    def preprocess(self, msg):
        if "AUTH" in msg.headers:
            try:
                msg.headers["AUTH"] = json.loads(msg.headers["AUTH"])
            except:
                pass

    def on_message(self, msg, headers):
        pass


class SyncMessenger(object):
    PORT = 11023
    READ_BUF_SIZE = -1
    WRITE_BUF_SIZE = 10240

    def __init__(self, queue, host="localhost", **kwargs):
        self.queue = queue
        self.extra_headers = {x.upper(): y for x, y in kwargs.items()}
        self.host = host
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.active = False

    def start(self):
        self.sock.connect((self.host, self.PORT))
        self.rfile = self.sock.makefile('rb', self.READ_BUF_SIZE)
        self.wfile = self.sock.makefile('wb', self.WRITE_BUF_SIZE)

    def send(self, obj):
        msg = Message("enqueue", obj)
        msg.headers.update(self.extra_headers)
        msg.headers["Q"] = self.queue

        write_message(self.wfile, msg)
        msg = read_message(self.rfile)
        ensure_ok_message(msg)
        return Receiver.receive(self)

    def preprocess(self, msg):
        return Receiver.preprocess(self, msg)

    def stop(self):
        Receiver.stop(self)


class Creator(object):
    PORT = 11023
    READ_BUF_SIZE = -1
    WRITE_BUF_SIZE = 10240

    def __init__(self, host="localhost", **kwargs):
        self.host = host
        self.extra_headers = {x.upper(): y for x, y in kwargs.items()}
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.active = False

    def start(self):
        self.sock.connect((self.host, self.PORT))
        self.rfile = self.sock.makefile('rb', self.READ_BUF_SIZE)
        self.wfile = self.sock.makefile('wb', self.WRITE_BUF_SIZE)

    def create(self, queue_info, headers=None):
        msg = Message("create", queue_info)
        msg.headers.update(self.extra_headers)
        if headers is not None:
            msg.headers = headers

        write_message(self.wfile, msg)
        msg = read_message(self.rfile)
        ensure_ok_message(msg)

        return msg.headers["Q"]

    def close(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass

        for item in (self.rfile, self.wfile, self.sock):
            try:
                item.close()
            except Exception:
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
