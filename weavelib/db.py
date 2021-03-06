from weavelib.exceptions import ObjectNotFound
from weavelib.rpc import RPCClient


class AppDBConnection(object):
    def __init__(self, conn, service):
        self.conn = conn
        self.service = service
        self.db_rpc = None

    def start(self):
        rpc = self.service.rpc_client["rpc_info"]
        rpc_info = rpc("weaveserver.services.simpledb", "object_store",
                       _block=True)
        self.db_rpc = RPCClient(self.conn, rpc_info, self.service.token)
        self.db_rpc.start()

    def stop(self):
        self.db_rpc.stop()

    def __getitem__(self, key):
        try:
            return self.db_rpc["query"](key, _block=True)
        except ObjectNotFound:
            raise KeyError(key)

    def __setitem__(self, key, value):
        self.db_rpc["insert"](key, value, _block=True)
