from app.core.config_loader import get_config


class Module(object):
    def __init__(self, name, deps, meta):
        self.name = name
        self.deps = deps
        self.meta = meta

    def get_config(self):
        get_config(self.meta.get("config", []))
