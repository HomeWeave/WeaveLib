from threading import Event, Thread

import pytest

import weavelib.netutils as netutils
from weavelib.exceptions import AuthenticationFailed, ProtocolError
from weavelib.messaging import discover_message_server
from weavelib.messaging import WeaveConnection

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
