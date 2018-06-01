import base64
import inspect
import mimetypes
import os

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileModifiedEvent


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

    mime = mime or mimetypes.guess_type(cur_file)[0]
    if not mime:
        raise ValueError("Can't guess mime type. Please specify.")

    url = service.rpc_client["register_view"](rel_path, obj, mime, _block=block)

    if url and url.endswith(rel_path):
        return url[:-len(rel_path)], rel_path


class FileSystemUpdater(FileSystemEventHandler):
    def __init__(self, callback):
        self.callback = callback

    def on_modified(self, event):
        if isinstance(event, FileModifiedEvent):
            self.callback(event.src_path)


class FileWatcher(Observer):
    def __init__(self, base_path, service):
        super(FileWatcher, self).__init__()
        self.base_path = base_path

        def callback(path):
            register_url(service, path, base_path)

        self.updater = FileSystemUpdater(callback)

    def start(self):
        self.schedule(self.updater, path=self.base_path, recursive=True)
        super(FileWatcher, self).start()

    def stop(self):
        super(FileWatcher, self).stop()
        super(FileWatcher, self).join()


class AppHTTPServer(object):
    def __init__(self, service):
        self.service = service
        self.watchers = []

    def start(self):
        pass

    def stop(self):
        for watcher in self.watchers:
            watcher.stop()
        for watcher in self.watchers:
            watcher.join()

    def register_folder(self, path, watch=False):
        path = path_from_service(path, self.service)
        base_url = None
        for cur_folder, _, files in os.walk(path):
            for filename in files:
                cur_file = os.path.join(cur_folder, filename)
                prefix_url, rel_url = register_url(self.service, cur_file, path)

                if base_url is None:
                    base_url = prefix_url
        if watch:
            self.watchers.append(FileWatcher(path, self.service))
            self.watchers[-1].start()

        return base_url.rstrip("/")
