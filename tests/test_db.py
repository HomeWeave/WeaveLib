import os
from tempfile import NamedTemporaryFile

import pytest
from weaveserver.core.services import ServiceManager

from weavelib.db import AppDBConnection
from weavelib.messaging import WeaveConnection
from weavelib.services import BaseService
from weaveserver.core.logger import configure_logging

configure_logging()


AUTH = {
    "auth1": {
        "appid": "auth1",
        "package": "p"
    }
}


class DummyService(BaseService):
    def __init__(self, conn, token):
        super(DummyService, self).__init__(token)
        self.db = AppDBConnection(conn, self)

    def on_service_start(self):
        super(DummyService, self).on_service_start()
        self.db.start()

    def on_service_stop(self):
        self.db.stop()
        super(DummyService, self).on_service_stop()


class TestAppDBConnection(object):
    @classmethod
    def setup_class(cls):
        os.environ["DB_PATH"] = NamedTemporaryFile(delete=False).name
        cls.service_manager = ServiceManager()
        cls.service_manager.apps.update(AUTH)
        cls.service_manager.start_services(["core", "simpledb"])

        cls.conn = WeaveConnection.local()
        cls.conn.connect()

        cls.service = DummyService(cls.conn, "auth1")
        cls.service.service_start()

    @classmethod
    def teardown_class(cls):
        cls.service.service_stop()
        cls.conn.close()

        cls.service_manager.stop()
        os.unlink(os.environ["DB_PATH"])
        del os.environ["DB_PATH"]

    def test_object_save(self):
        self.service.db["test"] = "value"
        assert self.service.db["test"] == "value"

    def test_no_object(self):
        with pytest.raises(KeyError):
            self.service.db["invalid"]
