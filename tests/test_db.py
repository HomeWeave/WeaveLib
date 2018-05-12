import os

import pytest
from weaveserver.core.services import ServiceManager

from weavelib.services import BaseService
from weavelib.db import AppDBConnection
from weaveserver.core.logger import configure_logging

configure_logging()


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
        self.db = AppDBConnection(self)

    def on_service_start(self):
        super(DummyService, self).on_service_start()
        self.db.start()

    def on_service_stop(self):
        self.db.stop()
        super(DummyService, self).on_service_stop()


class TestAppDBConnection(object):
    @classmethod
    def setup_class(cls):
        os.environ["DB_PATH"] = ":memory:"
        cls.service_manager = ServiceManager()
        cls.service_manager.apps.update(AUTH)
        cls.service_manager.start_services(["messaging", "appmanager",
                                            "simpledb"])

    @classmethod
    def teardown_class(cls):
        cls.service_manager.stop()
        del os.environ["DB_PATH"]

    def setup_method(self):
        self.service = DummyService("auth2")
        self.service.service_start()

    def teardown_method(self):
        self.service.service_stop()

    def test_object_save(self):
        self.service.db["test"] = "value"
        assert self.service.db["test"] == "value"

    def test_no_object(self):
        with pytest.raises(KeyError):
            self.service.db["invalid"]
