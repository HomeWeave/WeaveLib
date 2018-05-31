from uuid import uuid4


class Module(object):
    def __init__(self, name, deps, meta, package_path):
        self.id = "app-id-" + str(uuid4())
        self.name = name
        self.deps = deps
        self.meta = meta
        self.package_path = package_path
        self.system = False

    def get_config(self):
        return self.meta.get("config", [])

    def json(self):
        # TODO: Remove this and use the entire object for storing the map.
        return {
            "type": "SYSTEM" if self.system else "PLUGIN",
            "appid": self.id,
            "package": self.package_path.replace("/", ".")
        }
