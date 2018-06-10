import base64
import inspect
import mimetypes
import os
from threading import Thread, Event


mimetypes.init()


def path_from_service(path, service):
    class_file = inspect.getfile(service.__class__)
    service_dir = os.path.dirname(class_file)
    return os.path.join(service_dir, path)


def encode_content(path, service):
    with open(path_from_service(path, service), 'rb') as inp:
        encoded = base64.b64encode(inp.read())
        if isinstance(encoded, str):
            return encoded
        return encoded.decode('ascii')


def register_url(service, cur_file, path, mime=None, block=True):
    rel_path = os.path.relpath(cur_file, path)
    obj = encode_content(cur_file, service)

    mime = mime or (mimetypes.guess_type(cur_file) or [None])[0]
    if not mime:
        mime = "application/octet-stream"

    url = service.rpc_client["register_view"](rel_path, obj, mime, _block=block)

    if url and url.endswith(rel_path):
        return url[:-len(rel_path)], rel_path


def walk_folder(path, callback):
    for cur_folder, _, files in os.walk(path):
        for filename in files:
            cur_file = os.path.join(cur_folder, filename)
            callback(cur_file)



class FileWatcher(Thread):
    """ Super dumb way to inform AppManager to update the content of files."""

    POLL_SECS = 5

    def __init__(self, base_path, service):
        super(FileWatcher, self).__init__()
        self.base_path = base_path
        self.service = service
        self.stop_event = Event()

    def run(self):
        while True:
            walk_folder(self.base_path, self.process_path)
            if self.stop_event.wait(timeout=self.POLL_SECS):
                break

    def process_path(self, path):
        register_url(self.service, path, self.base_path)

    def stop(self):
        self.stop_event.set()
        self.join()


class AppHTTPServer(object):
    def __init__(self, service):
        self.service = service
        self.watchers = []

    def start(self):
        pass

    def stop(self):
        for watcher in self.watchers:
            watcher.stop()

    def register_folder(self, path, watch=False):
        path = path_from_service(path, self.service)
        base_url = [None]

        def process_file(file_path):
            prefix_url, _ = register_url(self.service, file_path, path)
            if base_url[0] is None:
                base_url[0] = prefix_url

        walk_folder(path, process_file)

        if watch:
            self.watchers.append(FileWatcher(path, self.service))
            self.watchers[-1].start()

        return base_url[0].rstrip("/")
