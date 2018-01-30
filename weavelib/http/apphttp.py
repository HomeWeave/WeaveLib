class AppHTTPServer(object):
    def __init__(self, service, root_obj):
        self.service = service
        self.root_obj = root_obj

    def start(self):
        pass

    def add_url(self, obj, block=True):
        return self.service.rpc_client["register_app_view"](obj, _block=block)
