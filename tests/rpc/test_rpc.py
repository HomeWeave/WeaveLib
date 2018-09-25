import time
import random
from threading import Event
from concurrent.futures import ThreadPoolExecutor

import pytest
from weaveserver.core.services import ServiceManager

from weavelib.messaging import WeaveConnection
from weavelib.rpc import RPCClient, RPCServer, ServerAPI, get_rpc_caller
from weavelib.rpc import ArgParameter, KeywordParameter, RemoteAPIError
from weavelib.services import BaseService


from weaveserver.core.logger import configure_logging

configure_logging()


AUTH = {
    "auth1": {
        "type": "SYSTEM",
        "appid": "appmgr"
    },
    "auth2": {
        "package": "p",
        "appid": "appid2"
    }
}


class DummyService(BaseService):
    def __init__(self, conn, token):
        apis = [
            ServerAPI("api1", "desc1", [
                ArgParameter("p1", "d1", str),
                ArgParameter("p2", "d2", int),
                KeywordParameter("k3", "d3", bool)
            ], self.api1),
            ServerAPI("api2", "desc2", [], self.api2),
            ServerAPI("api3", "desc3", [], self.api3),
            ServerAPI("exception", "desc2", [], self.exception)
        ]
        self.rpc_server = RPCServer("name", "desc", apis, self, conn)
        self.paused = False
        super(DummyService, self).__init__(token)

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
        cls.service_manager = ServiceManager()
        cls.service_manager.apps.update(AUTH)
        cls.service_manager.start_services(["core"])

        cls.conn = WeaveConnection()
        cls.conn.connect()

    @classmethod
    def teardown_class(cls):
        cls.service_manager.stop()
        cls.service_manager.wait()

    def setup_method(self):
        self.service = DummyService(self.conn, "auth2")
        self.service.service_start()

    def teardown_method(self):
        self.service.service_stop()

    def test_server_function_invoke(self):
        info = self.service.rpc_server.info_message
        client = RPCClient(self.conn, info)
        client.start()

        res = client["api1"]("hello", 5, k3=False, _block=True)
        assert res == "hello5False"

        client.stop()

    def test_with_different_client(self):
        info = self.service.rpc_server.info_message
        client = RPCClient(self.conn, info)
        client.start()

        res = client["api1"]("hello", 5, k3=False, _block=True)
        assert res == "hello5False"

        client.stop()

        client = RPCClient(self.conn, info)
        client.start()

        res = client["api1"]("hello", 5, k3=False, _block=True)
        assert res == "hello5False"

        client.stop()

    def test_several_functions_invoke(self):
        info = self.service.rpc_server.info_message

        self.service.paused = True

        client = RPCClient(self.conn, info)
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
        client = RPCClient(self.conn, info)
        client.start()

        event = Event()
        result = []

        def callback(res):
            result.append(res)
            event.set()

        client["api1"]("hello", 5, k3=False, _callback=callback)

        event.wait()

        assert result[0]["result"] == "hello5False"

        client.stop()

    def test_rpc_caller(self):
        info = self.service.rpc_server.info_message
        client = RPCClient(self.conn, info, token="auth2")
        client.start()

        res = client["api3"](_block=True)
        assert res == AUTH["auth2"]

        client.stop()

    def test_api_with_exception(self):
        info = self.service.rpc_server.info_message
        client = RPCClient(self.conn, info)
        client.start()

        with pytest.raises(RemoteAPIError):
            client["exception"](_block=True)

        client["exception"]()  # Exception is not visible.

        client.stop()
