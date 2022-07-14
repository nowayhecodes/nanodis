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


class CmdError(Exception):
    def __init__(self, message: str):
        self.message = message
        super(CmdError, self).__init__()
