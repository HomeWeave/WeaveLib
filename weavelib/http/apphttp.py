import base64
import errno
import inspect
import mimetypes
import os
import select
import sys
from threading import Thread

from pyinotify import Notifier, ProcessEvent, WatchManager, IN_MODIFY

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


class FileChangeNotifier(Notifier):
    # From: https://ondergetekende.nl/using-pyinotify-with-eventlet.html.
    def __init__(self, *args, **kwargs):
        super(FileChangeNotifier, self).__init__(*args, **kwargs)

        # We won't be using the pollobj
        self._pollobj.unregister(self._fd)
        self._pollobj = None
        self._thread = Thread(target=self.loop,
                              kwargs={"callback": self.is_active})

    def start(self):
        self._thread.start()

    def stop(self):
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
        if self._thread.is_alive():
            self._thread.join()

    def is_active(self):
        return self._fd is not None

    def check_events(self, timeout=None):
        while True:
            try:
                read_fs, _, _ = select.select([self._fd], [], [])
                break
            except select.error as err:
                error_no = err[0] if sys.version_info[0] < 3 else err.errno
                if error_no == errno.EINTR:
                    break
                else:
                    raise

        return bool(read_fs)


class FileSystemUpdater(ProcessEvent):
    def __init__(self, relative_path, service, *args, **kwargs):
        self.service = service
        self.relative_path = relative_path
        super(FileSystemUpdater, self).__init__(*args, **kwargs)

    def process_IN_MODIFY(self, event):
        register_url(self.service, event.pathname, self.relative_path)


WATCH_MANAGER = WatchManager()


class AppHTTPServer(object):
    def __init__(self, service):
        self.service = service
        self.watcher_notifiers = []

    def start(self):
        pass

    def stop(self):
        for notifier in self.watcher_notifiers:
            notifier.stop()

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
            WATCH_MANAGER.add_watch(path, mask=IN_MODIFY, rec=True)
            updater = FileSystemUpdater(path, self)
            notifier = FileChangeNotifier(WATCH_MANAGER, updater)
            self.watcher_notifiers.append(notifier)
            self.watcher_notifiers[-1].start()

        return base_url.rstrip("/")
