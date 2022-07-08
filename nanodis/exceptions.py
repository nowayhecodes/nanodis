class ClientQuit(Exception):
    pass


class Shutdown(Exception):
    pass


class ServerError(Exception):
    pass


class ServerDisconnect(ServerError):
    pass


class ServerInternalError(ServerError):
    pass
