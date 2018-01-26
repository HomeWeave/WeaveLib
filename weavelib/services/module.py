class Module(object):
    def __init__(self, name, deps, meta):
        self.name = name
        self.deps = deps
        self.meta = meta

    def get_config(self):
        return self.meta.get("config", [])
