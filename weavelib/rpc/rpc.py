import inspect
import logging
from concurrent.futures import ThreadPoolExecutor
from threading import Thread, RLock, Event
from uuid import uuid4

from jsonschema import Draft4Validator

from weavelib.messaging import Sender, Receiver, Creator
from weavelib.messaging.messaging import raise_message_exception
from weavelib.exceptions import WeaveException
from weavelib.services import MessagingEnabled
from .api import API, ArgParameter, KeywordParameter


logger = logging.getLogger(__name__)


def api_group_schema(apis):
    return {
        "anyOf": [x.schema for x in apis]
    }


class RemoteAPIError(RuntimeError):
    """Raised to indicate exception thrown by remote API."""


class ClientAPI(API):
    def __init__(self, name, desc, params, handler):
        super(ClientAPI, self).__init__(name, desc, params)
        self.handler = handler

    def __call__(self, *args, _block=False, _callback=None, **kwargs):
        obj = self.validate_call(*args, **kwargs)
        return self.handler(obj, block=_block, callback=_callback)

    @staticmethod
    def from_info(info, handler):
        api = ClientAPI(info["name"], info["description"], [], handler)
        api.args = [ArgParameter.from_info(x) for x in info.get("args", [])]
        api.kwargs = [KeywordParameter.from_info(x) for x in
                      info.get("kwargs", {}).values()]
        return api


class ServerAPI(API):
    def __init__(self, name, desc, params, handler):
        super(ServerAPI, self).__init__(name, desc, params)
        self.handler = handler

    def __call__(self, *args, **kwargs):
        self.validate_call(*args, **kwargs)
        return self.handler(*args, **kwargs)


class RPC(object):
    def __init__(self, name, description, apis):
        self.name = name
        self.description = description
        self.apis = {x.name: x for x in apis}

    def __getitem__(self, name):
        return self.apis[name]

    @property
    def request_schema(self):
        return {
            "type": "object",
            "properties": {
                "invocation": api_group_schema(self.apis.values()),
            }
        }

    @property
    def response_schema(self):
        return {"type": "object"}


class RPCReceiver(Receiver):
    def __init__(self, conn, component, queue, host="localhost", **kwargs):
        super(RPCReceiver, self).__init__(conn, queue, host=host, **kwargs)
        self.component = component

    def on_message(self, msg, headers):
        self.component.on_rpc_message(msg, headers)


class RPCServer(RPC):
    MAX_RPC_WORKERS = 5

    def __init__(self, name, description, apis, service):
        if not isinstance(service, MessagingEnabled):
            raise BadArguments("Service is not messaging enabled.")

        super(RPCServer, self).__init__(name, description, apis)
        self.service = service
        self.executor = ThreadPoolExecutor(self.MAX_RPC_WORKERS)
        self.sender = None
        self.receiver = None
        self.receiver_thread = None
        self.cookie = None

    def register_rpc(self):
        apis = {name: api.info for name, api in self.apis.items()}
        return self.service.rpc_client["register_rpc"](
            self.name, self.description, apis, _block=True)

    def start(self):
        conn = self.service.get_connection()
        auth_token = self.service.get_auth_token()
        rpc_info = self.register_rpc()
        self.sender = Sender(conn, rpc_info["response_queue"], auth=auth_token)
        self.receiver = RPCReceiver(conn, self, rpc_info["request_queue"],
                                    auth=auth_token)

        self.sender.start()
        self.receiver.start()

        self.receiver_thread = Thread(target=self.receiver.run)
        self.receiver_thread.start()

    def stop(self):
        # TODO: Delete the queue, too.
        self.receiver.stop()
        self.receiver_thread.join()

        self.executor.shutdown()

    def on_rpc_message(self, rpc_obj, headers):
        def make_done_callback(request_id, cmd, cookie):
            def callback(future):
                try:
                    self.sender.send({
                        "id": request_id,
                        "command": cmd,
                        "result": future.result()
                    }, headers={"COOKIE": cookie})
                except WeaveException as e:
                    logger.warning("WeaveException was raised by API: %s", e)
                    self.sender.send({
                        "id": request_id,
                        "command": cmd,
                        "error_name": e.err_msg(),
                        "error": e.extra
                    }, headers={"COOKIE": cookie})
                except Exception as e:
                    logger.exception("Internal API raised exception." + str(e))
                    self.sender.send({
                        "id": request_id,
                        "command": cmd,
                        "error": "Internal API Error."
                    }, headers={"COOKIE": cookie})

            return callback

        def execute_api_internal(rpc_obj, headers, api, *args, **kwargs):
            # Keep func name in sync one in get_rpc_caller(..)
            return api(*args, **kwargs)

        obj = rpc_obj["invocation"]
        cookie = rpc_obj["response_cookie"]
        request_id = obj["id"]
        cmd = obj["command"]
        try:
            api = self[cmd]
        except KeyError:
            self.sender.send({
                "id": request_id,
                "result": False,
                "error": "API not found."
            })
            return

        args = obj.get("args", [])
        kwargs = obj.get("kwargs", {})
        future = self.executor.submit(execute_api_internal, rpc_obj, headers,
                                      api, *args, **kwargs)
        future.add_done_callback(make_done_callback(request_id, cmd, cookie))

    @property
    def info_message(self):
        return {
            "name": self.name,
            "description": self.description,
            "apis": {name: api.info for name, api in self.apis.items()},
            "request_queue": self.receiver.queue,
            "response_queue": self.sender.queue
        }


class RPCClient(RPC):
    def __init__(self, conn, rpc_info, token=None):
        self.token = token
        name = rpc_info["name"]
        description = rpc_info["description"]
        apis = [self.get_api_call(x) for x in rpc_info["apis"].values()]
        super(RPCClient, self).__init__(name, description, apis)

        self.client_cookie = "rpc-client-cookie-" + str(uuid4())
        self.sender = Sender(conn, rpc_info["request_queue"], auth=self.token)
        self.receiver = RPCReceiver(conn, self, rpc_info["response_queue"],
                                    cookie=self.client_cookie)
        self.receiver_thread = Thread(target=self.receiver.run)

        self.callbacks = {}
        self.callbacks_lock = RLock()

    def start(self):
        self.sender.start()
        self.receiver.start()
        self.receiver_thread.start()

    def stop(self):
        self.sender.close()
        self.receiver.stop()
        self.receiver_thread.join()

    def get_api_call(self, obj):
        def make_blocking_callback(event, response_arr):
            def callback(obj):
                response_arr.append(obj)
                event.set()
            return callback

        def on_invoke(obj, block, callback):
            msg_id = obj["id"]

            if block:
                res_arr = []
                event = Event()
                callback = make_blocking_callback(event, res_arr)

            if callback:
                with self.callbacks_lock:
                    self.callbacks[msg_id] = callback

            self.sender.send({
                "invocation": obj,
                "response_cookie": self.client_cookie
            }, headers={"AUTH": self.token})
            if not block:
                return

            event.wait()

            if "result" in res_arr[0]:
                return res_arr[0]["result"]
            elif "error_name" in res_arr[0]:
                raise_message_exception(res_arr[0]["error_name"],
                                        res_arr[0].get("error"))
            else:
                raise RemoteAPIError(res_arr[0].get("error"))

        return ClientAPI.from_info(obj, on_invoke)

    def on_rpc_message(self, msg, headers):
        with self.callbacks_lock:
            callback = self.callbacks.pop(msg["id"])

        if not callback:
            return

        callback(msg)


def get_rpc_caller():
    for frame, _, _, func, _, _ in inspect.stack(context=0):
        if func == 'execute_api_internal':
            if "headers" not in frame.f_locals:
                continue

            return frame.f_locals["headers"].get("AUTH")


def find_rpc(service, app_id, rpc_name):
    token = service.get_auth_token()
    conn = service.get_connection()

    rpc_info = {
        "name": "",
        "description": "",
        "apis": {
            "rpc_info": {
                "name": "rpc_info",
                "description": "",
                "args": [
                    {
                        "name": "app_id",
                        "description": "",
                        "schema": {"type": "string"}
                    },
                    {
                        "name": "rpc_name",
                        "description": "",
                        "schema": {"type": "string"}
                    },
                ]
            }
        },
        "request_queue": "_system/registry/request",
        "response_queue": "_system/registry/response",
    }

    client = RPCClient(conn, rpc_info, token)
    client.start()

    res = client["rpc_info"](app_id, rpc_name, _block=True)
    client.stop()

    return res
