import json
import os
import sys


def load_ui(path, service):
    module = sys.modules[service.__class__.__module__]
    service_dir = os.path.dirname(module.__file__)
    with open(os.path.join(service_dir, path)) as inp:
        return json.load(inp)


class AppHTTPServer(object):
    def __init__(self, service):
        self.service = service

    def start(self):
        pass

    def add_url(self, path, block=True):
        obj = load_ui(path, self.service)
        return self.service.rpc_client["register_view"](obj, _block=block)
