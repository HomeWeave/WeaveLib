class Module(object):
    def __init__(self, name, deps, meta, package_path):
        self.name = name
        self.deps = deps
        self.meta = meta
        self.package_path = package_path

    def get_config(self):
        return self.meta.get("config", [])
