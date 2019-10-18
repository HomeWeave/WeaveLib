import time
import random
from threading import Event
from concurrent.futures import ThreadPoolExecutor

import pytest

from weavelib.exceptions import BadArguments
from weavelib.messaging import WeaveConnection
from weavelib.rpc import OneOf
from weavelib.rpc import RPCClient, RPCServer, ServerAPI, get_rpc_caller
from weavelib.rpc import ArgParameter, KeywordParameter, RemoteAPIError
from weavelib.rpc import find_rpc, extract_rpc_payload
from weavelib.services import BaseService, MessagingEnabled

from test_utils import MessagingService, DummyEnvService


MESSAGING_PLUGIN_URL = "https://github.com/HomeWeave/WeaveServer.git"


class DummyService(MessagingEnabled, BaseService):
    def __init__(self, conn, token):
        super(DummyService, self).__init__(auth_token=token, conn=conn)
        apis = [
            ServerAPI("api1", "desc1", [
                ArgParameter("p1", "d1", str),
                ArgParameter("p2", "d2", int),
                KeywordParameter("k3", "d3", bool)
            ], self.api1),
            ServerAPI("api2", "desc2", [], self.api2),
            ServerAPI("api3", "desc3", [], self.api3),
            ServerAPI("callback", "c", [
                ArgParameter("param", "d", self.get_param)
            ], self.callback_api),
            ServerAPI("change_param", "c", [
                ArgParameter("param", "d", str)
            ], self.change_param),
            ServerAPI("exception", "desc2", [], self.exception)
        ]
        self.rpc_server = RPCServer("name", "desc", apis, self)
        self.paused = False
        self.available_params = ["1", "2"]

    def api1(self, p1, p2, k3):
        if type(p1) != str or type(p2) != int or type(k3) != bool:
            return "BAD"
        if self.paused:
            while self.paused:
                sleep_time = random.randrange(500, 1500)/1000.0
                time.sleep(sleep_time)
        return "{}{}{}".format(p1, p2, k3)

    def api2(self):
        return "API2"

    def api3(self):
        def execute_api_internal():
            return get_rpc_caller()
        return execute_api_internal()

    def callback_api(self, param):
        return param in self.available_params

    def get_param(self):
        return OneOf(*self.available_params)

    def change_param(self, value):
        self.available_params = value.split(",")
        self.rpc_server.update_rpc()

    def exception(self):
        raise RuntimeError("dummy")

    def get_service_queue_name(self, path):
        return "/" + path

    def on_service_start(self):
        self.rpc_server.start()

    def on_service_stop(self):
        self.rpc_server.stop()


class TestRPC(object):
    @classmethod
    def setup_class(cls):
        cls.messaging_service = MessagingService()
        cls.messaging_service.service_start()
        cls.messaging_service.wait_for_start(15)

        cls.conn = WeaveConnection.local()
        cls.conn.connect()

        cls.env_service = DummyEnvService(cls.messaging_service.test_token,
                                          cls.conn)

        rpc_info = find_rpc(cls.env_service, MESSAGING_PLUGIN_URL,
                            "app_manager")
        appmgr_client = RPCClient(cls.env_service.get_connection(), rpc_info,
                                  cls.env_service.get_auth_token())
        appmgr_client.start()

        # Register the DummyService used in the test cases.
        cls.test_token = appmgr_client["register_plugin"]("x", "y", _block=True)

        appmgr_client.stop()

    @classmethod
    def teardown_class(cls):
        cls.conn.close()
        cls.messaging_service.service_stop()

    def setup_method(self):
        self.service = DummyService(self.conn, self.test_token)
        self.service.service_start()

    def teardown_method(self):
        self.service.service_stop()

    def test_server_function_invoke(self):
        info = self.service.rpc_server.info_message
        client = RPCClient(self.conn, info, self.test_token)
        client.start()

        res = client["api1"]("hello", 5, k3=False, _block=True)
        assert res == "hello5False"

        client.stop()

    def test_with_different_client(self):
        info = self.service.rpc_server.info_message
        client = RPCClient(self.conn, info, self.test_token)
        client.start()

        res = client["api1"]("hello", 5, k3=False, _block=True)
        assert res == "hello5False"

        client.stop()

        client = RPCClient(self.conn, info, self.test_token)
        client.start()

        res = client["api1"]("hello", 5, k3=False, _block=True)
        assert res == "hello5False"

        client.stop()

    def test_several_functions_invoke(self):
        info = self.service.rpc_server.info_message

        self.service.paused = True

        client = RPCClient(self.conn, info, self.test_token)
        client.start()
        api1 = client["api1"]
        api2 = client["api2"]

        res = []

        with ThreadPoolExecutor(max_workers=100) as exc:
            for i in range(20):
                future = exc.submit(api1, "iter", i, k3=i % 2 == 0, _block=True)
                expected = "iter{}{}".format(i, i % 2 == 0)
                res.append((future, expected))

                future = exc.submit(api2, _block=True)
                res.append((future, "API2"))

            time.sleep(5)
            self.service.paused = False

            for future, expected in res:
                assert future.result() == expected
            exc.shutdown()
        client.stop()

    def test_callback_rpc_invoke(self):
        info = self.service.rpc_server.info_message
        client = RPCClient(self.conn, info, self.test_token)
        client.start()

        event = Event()
        result = []

        def callback(res):
            result.append(extract_rpc_payload(res))
            event.set()

        client["api1"]("hello", 5, k3=False, _callback=callback)

        event.wait()

        assert result[0] == "hello5False"

        client.stop()

    def test_rpc_caller(self):
        info = self.service.rpc_server.info_message
        client = RPCClient(self.conn, info, self.test_token)
        client.start()

        res = client["api3"](_block=True)
        expected = {
            "app_name": "x",
            "app_url": "y",
            "app_type": "plugin"
        }
        assert res == expected

        client.stop()

    def test_api_with_exception(self):
        info = self.service.rpc_server.info_message
        client = RPCClient(self.conn, info, self.test_token)
        client.start()

        with pytest.raises(RemoteAPIError):
            client["exception"](_block=True)

        client["exception"]()  # Exception is not visible.

        client.stop()

    def test_api_with_exception_callback(self):
        info = self.service.rpc_server.info_message
        client = RPCClient(self.conn, info, self.test_token)
        client.start()


        event = Event()
        result = []

        def callback(res):
            result.append(res)
            event.set()

        client["exception"](_callback=callback)

        event.wait()

        with pytest.raises(RemoteAPIError):
            extract_rpc_payload(result[0])

        client.stop()

    def test_api_with_dynamic_schema(self):
        info = self.service.rpc_server.info_message
        client = RPCClient(self.conn, info, self.test_token)
        client.start()

        assert client["callback"]("1", _block=True)
        client["change_param"]("hi,there", _block=True)

        client.stop()

        info = self.service.rpc_server.info_message
        client = RPCClient(self.conn, info, self.test_token)
        client.start()
        with pytest.raises(BadArguments):
            client["callback"]("1", _block=True)
        client.stop()

        client = RPCClient(self.conn, info, self.test_token)
        client.start()
        assert client["callback"]("hi", _block=True)
        assert client["callback"]("there", _block=True)
        client.stop()
