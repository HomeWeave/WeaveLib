import re

import requests

from weavehttp.service import WeaveHTTPService

from weavelib.messaging import WeaveConnection
from weavelib.rpc import find_rpc, RPCClient
from weavelib.services import BackgroundThreadServiceStart, MessagingEnabled
from weavelib.services import BaseService
from weavelib.plugins.http import HTTPResourceRegistrationHelper

from test_utils import MessagingService, DummyEnvService


MESSAGING_PLUGIN_URL = "https://github.com/HomeWeave/WeaveServer.git"
WEAVE_HTTP_URL = "https://github.com/HomeWeave/WeaveHTTP.git"


class ThreadedWeaveHTTPService(BackgroundThreadServiceStart, WeaveHTTPService):
    pass


class DummyService(MessagingEnabled, BaseService):
    def __init__(self, conn, token):
        super(DummyService, self).__init__(auth_token=token, conn=conn)
        self.http_helper = HTTPResourceRegistrationHelper(self)

    def on_service_start(self, *args, **kwargs):
        self.http_helper.start()

    def on_service_stop(self):
        self.http_helper.stop()


class TestHTTPResourceRegistrationHelper(object):
    HTTP_URL = "http://localhost:15000/static/"

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

        http_token = appmgr_client["register_plugin"]("http", WEAVE_HTTP_URL,
                                                      _block=True)
        dummy_token = appmgr_client["register_plugin"]("x", "y", _block=True)

        cls.http = ThreadedWeaveHTTPService(auth_token=http_token,
                                            plugin_dir="x", venv_dir="y",
                                            conn=cls.conn, started_token="")
        cls.http.service_start()
        cls.http.wait_for_start(15)

        cls.dummy_service = DummyService(cls.conn, dummy_token)
        cls.dummy_service.service_start()
        cls.dummy_service.wait_for_start(15)


    @classmethod
    def teardown_class(cls):
        cls.http.service_stop()
        cls.dummy_service.service_stop()
        cls.conn.close()
        cls.messaging_service.service_stop()

    def test_register_content(self):
        url = self.dummy_service.http_helper.register_content(b"test", "/test")

        assert re.match("apps/[^/]+/test", url)
        assert requests.get(self.HTTP_URL + url).text == "test"

        self.dummy_service.http_helper.unregister_url("/test")

    def test_register_directory(self, tmpdir):
        dirs = [tmpdir.mkdir("dir-" + str(x)) for x in range(2)]
        files = ["file-" + str(x) for x in range(3)]
        for directory in dirs:
            for filename in files:
                directory.join(filename).write(str(files.index(filename)))

        url = self.dummy_service.http_helper.register_directory(str(tmpdir),
                                                                "/dir")
        base_url = self.HTTP_URL + url
        for directory in dirs:
            for filename in files:
                cur = base_url + "/" + directory.basename + "/" + filename
                assert requests.get(cur).text == str(files.index(filename))
