from threading import Event, Thread

import pytest

import weavelib.netutils as netutils
from weavelib.exceptions import AuthenticationFailed, ProtocolError
from weavelib.messaging import discover_message_server, Sender
from weavelib.messaging import Receiver, SyncMessenger, Creator

from weaveserver.services.discovery import DiscoveryService
from weaveserver.services.discovery.service import DiscoveryServer
from weaveserver.core.services import ServiceManager


CONFIG = {
    "redis_config": {
        "USE_FAKE_REDIS": True
    },
    "queues": {
        "custom_queues": [
        ]
    }
}


AUTH = {
    "auth1": {
        "type": "SYSTEM",
        "appid": "appmgr"
    },
    "auth2": {
        "appid": "appid2"
    }
}


class TestDiscoverMessageServer(object):
    @classmethod
    def setup_class(cls):
        DiscoveryServer.ACTIVE_POLL_TIME = 1

    def test_no_discovery_server(self):
        assert discover_message_server() is None

    def test_discovery_server_bad_json(self):
        service = DiscoveryService("token", None)
        service.server.process = lambda x, y: "sdf;lghkd;flkh".encode("UTF-8")
        event = Event()
        service.notify_start = event.set
        Thread(target=service.on_service_start).start()
        event.wait()

        assert discover_message_server() is None
        service.on_service_stop()

    def test_discovery_server_unknown_json(self):
        service = DiscoveryService("token", None)
        service.server.process = lambda x, y: '{"vld": "json"}'.encode("UTF-8")
        event = Event()
        service.notify_start = event.set
        Thread(target=service.on_service_start).start()
        event.wait()

        assert discover_message_server() is None
        service.on_service_stop()

    def test_valid_discovery_server(self):
        service = DiscoveryService("token", None)
        event = Event()
        service.notify_start = event.set
        Thread(target=service.on_service_start).start()
        event.wait()

        ip_addresses = [x["addr"] for x in netutils.iter_ipv4_addresses()]
        try:
            assert discover_message_server()[0] in ip_addresses
        finally:
            service.on_service_stop()


class TestSyncMessenger(object):
    @classmethod
    def setup_class(cls):
        cls.service_manager = ServiceManager()
        cls.service_manager.apps = AUTH
        cls.service_manager.start_services(["core"])

        cls.start_echo_receiver("/dummy")

    @classmethod
    def teardown_class(cls):
        cls.service_manager.stop()

    @classmethod
    def start_echo_receiver(cls, queue):
        creator = Creator(auth="auth1")
        creator.start()
        creator.create({
            "queue_name": queue,
            "request_schema": {"type": "object"}
        })

        sender = Sender(queue)
        sender.start()

        def reply(msg, headers):
            sender.send(msg)

        receiver = Receiver(queue)
        receiver.on_message = reply
        receiver.start()

    def test_send_sync(self):
        obj = {"test": "test-messsage", "arr": [1, 2, 3]}

        sync = SyncMessenger("/dummy")
        sync.start()

        assert obj == sync.send(obj).task

        sync.stop()


class TestCreator(object):
    @classmethod
    def setup_class(cls):
        cls.service_manager = ServiceManager()
        cls.service_manager.apps = AUTH
        cls.service_manager.start_services(["core"])

    @classmethod
    def teardown_class(cls):
        cls.service_manager.stop()

    def test_create_without_auth(self):
        creator = Creator()
        creator.start()
        with pytest.raises(ProtocolError):
            creator.create({"queue_name": "/test"})

    def test_create_bad_auth(self):
        creator = Creator(auth="bad-auth")
        creator.start()
        with pytest.raises(AuthenticationFailed):
            creator.create({"queue_name": "/test"})
