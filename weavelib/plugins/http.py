from base64 import b64encode
from threading import Event, Lock

from weavelib.rpc import RPCClient, find_rpc
from weavelib.exceptions import WeaveException


WEAVE_HTTP_URL = "https://github.com/HomeWeave/WeaveHTTP.git"


class HTTPResourceRegistrationHelper(object):
    def __init__(self, service):
        self.service = service
        self.rpc_client = None
        self.watchers = []

    def start(self):
        rpc_info = find_rpc(self.service, WEAVE_HTTP_URL, "static_files")
        self.rpc_client = RPCClient(self.service.get_connection(), rpc_info,
                                    self.service.get_auth_token())
        self.rpc_client.start()

    def stop(self):
        self.rpc_client.stop()

        for watcher in self.watchers:
            watcher.stop()

        for watcher in self.watchers:
            watcher.join()

    def register_content(self, content, rel_http_url, block=True,
                         callback=None):
        param = b64encode(content).decode('ascii')
        return self.rpc_client["register"](rel_http_url, param, _block=block,
                                           _callback=callback)

    def register_file(self, local_path, relative_http_url, block=True,
                      callback=None):
        with open(local_path, "rb") as inp:
            content = inp.read()
        return self.register_content(content, relative_http_url, block=block,
                                     callback=callback)

    def register_directory(self, local_path, relative_http_url):
        files = []
        for cur_folder, _, files in os.walk(local_path):
            for filename in files:
                cur_file = os.path.join(cur_folder, filename)
                files.append((cur_file, os.path.relpath(cur_file, local_path)))

        events = {}
        responses = {}
        response_lock = Lock()

        def make_callback(rel_path, event):
            def callback(response):
                with response_lock:
                    responses[rel_path] = response
                event.set()
            return callback

        for abs_path, rel_path in files:
            cur_rel_http_url = os.path.join(relative_http_url, rel_path)
            event = Event()
            events[rel_path] = event
            callback = make_callback(rel_path, event)
            self.register_file(abs_path, cur_rel_http_url, _callback=callback)

        base_rel_url = None
        for rel_path, event in events.items():
            event.wait()
            with response_lock:
                rel_url = extract_rpc_payload(responses[rel_path])

            if base_rel_url is None and rel_url.endswith(rel_path):
                base_rel_url = rel_url[:-len(rel_path)]



        return base_rel_url

    def unregister_url(self, url, block=True, callback=None):
        return self.rpc_client["unregister"](url, _block=block,
                                             _callback=callback)
