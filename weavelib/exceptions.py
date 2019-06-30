class WeaveException(Exception):
    def __init__(self, extra=None):
        self.extra = extra

    def err_msg(self):
        return self.__class__.__name__


class ObjectNotFound(WeaveException):
    pass


class ObjectAlreadyExists(WeaveException):
    pass


class ObjectClosed(WeaveException):
    pass


class BadOperation(WeaveException):
    pass


class BadArguments(WeaveException):
    pass


class InternalError(WeaveException):
    pass


class ProtocolError(WeaveException):
    pass


class AuthenticationFailed(WeaveException):
    pass


class Unauthorized(WeaveException):
    pass


class SchemaValidationFailed(WeaveException):
    pass


class PluginLoadError(WeaveException):
    pass


class TimedOut(WeaveException):
    pass
