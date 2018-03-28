import base64
import inspect
import mimetypes
import os


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


class AppHTTPServer(object):
    def __init__(self, service):
        self.service = service

    def start(self):
        pass

    def add_url(self, url, path, mime=None, block=True):
        obj = encode_content(path, self.service)

        mime = mime or mimetypes.guess_type(path)[0]
        if not mime:
            raise ValueError("Can't guess mime type. Please specify.")

        return self.service.rpc_client["register_view"](url, obj, mime,
                                                        _block=block)

    def register_folder(self, path):
        VIEW_MIME = "application/vnd.weaveview+json"

        path = path_from_service(path, self.service)
        base_url = None
        for cur_folder, _, files in os.walk(path):
            for filename in files:
                cur_file = os.path.join(cur_folder, filename)
                url = os.path.relpath(cur_file, path)
                if url == "index.json":
                    http_url = self.add_url(url, cur_file, mime=VIEW_MIME)
                else:
                    http_url = self.add_url(url, cur_file)

                if base_url is None and http_url and http_url.endswith(url):
                    base_url = http_url[:-len(url)]

        return base_url.rstrip("/")
