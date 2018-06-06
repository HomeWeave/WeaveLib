import eventlet

eventlet.monkey_patch()

import time

import requests
from weaveserver.core.services import ServiceManager

from weavelib.services import BaseService
from weavelib.http import AppHTTPServer
from weavelib.http.apphttp import path_from_service


AUTH = {
    "auth1": {
        "type": "SYSTEM",
        "appid": "appmgr"
    },
    "auth2": {
        "appid": "appid2",
        "package": "p"
    }
}


class DummyService(BaseService):
    def __init__(self, token):
        super(DummyService, self).__init__(token)
        self.http = AppHTTPServer(self)

    def on_service_start(self):
        super(DummyService, self).on_service_start()
        self.http.start()

    def on_service_stop(self):
        self.http.stop()
        super(DummyService, self).on_service_stop()


class TestAppHTTPServer(object):
    @classmethod
    def setup_class(cls):
        cls.service_manager = ServiceManager()
        cls.service_manager.apps.update(AUTH)
        cls.service_manager.start_services(["messaging", "appmanager"])

        cls.service = DummyService("auth2")
        cls.service.service_start()

    @classmethod
    def teardown_class(cls):
        cls.service.service_stop()
        cls.service_manager.stop()

    def test_register_folder(self):
        base_url = self.service.http.register_folder("static")
        assert base_url == "/apps/appid2"

        url = "http://localhost:5000" + base_url
        assert requests.get(url + "/_status-card.json").json() == {}

        assert requests.get(url + "/test.txt").text == "test\n"

        assert requests.get(url + "/folder/test.md").text == "hello world\n"

    def test_folder_watcher(self):
        path = path_from_service("static2/temp.txt", self.service)

        base_url = self.service.http.register_folder("static2", watch=True)
        assert base_url == "/apps/appid2"

        url = "http://localhost:5000" + base_url
        assert requests.get(url + "/temp.txt").text == "temp\n"

        path = path_from_service("static2/temp.txt", self.service)
        with open(path, "w") as f:
            f.write("new-value")

        time.sleep(5)

        assert requests.get(url + "/temp.txt").text == "new-value"

        with open(path, "w") as f:
            f.write("temp\n")
