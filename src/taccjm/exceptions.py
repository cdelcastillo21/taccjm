"""
TACCJM Exceptions

"""

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"


class SSHCommandError(Exception):
    """
    Exception for commands run on systems

    Attributes
    ----------
    system : str
        TACC System on which command was executed.
    user : str
        User that is executing command.
    command : str
        Command that threw the error.
    rc : str
        Return code.
    stdout : str
        Output from stdout.
    stderr : str
        Output from stderr.
    message : str
        Explanation of the error.
    """

    def __init__(self, system, user, command_config, message="Non-zero return code."):
        self.system = system
        self.user = user
        self.command_config = command_config
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        msg = f"{self.message}"
        msg += f"\n{self.user}@{self.system}$ {self.command_config['cmd']}"
        msg += f"\nrc     : {self.command_config['rc']}"
        msg += f"\nstdout : {self.command_config['stdout']}"
        msg += f"\nstderr : {self.command_config['stderr']}"
        return msg


class TACCClientErrror(Exception):
    """
    Custom exception for errors when using TACC resources.

    Attributes
    ----------
    system : str
        TACC System on which command was executed.
    user : str
        User that is executing command.
    command : str
        Command that threw the error.
    rc : str
        Return code.
    stdout : str
        Output from stdout.
    stderr : str
        Output from stderr.
    message : str
        Explanation of the error.
    """

    def __init__(self, system, user, command_config, message="Non-zero return code."):
        self.system = system
        self.user = user
        self.command_config = command_config
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        msg = f"{self.message}"
        msg += f"\n{self.user}@{self.system}$ {self.command_config['cmd']}"
        msg += f"\nrc     : {self.command_config['rc']}"
        msg += f"\nstdout : {self.command_config['stdout']}"
        msg += f"\nstderr : {self.command_config['stderr']}"
        return msg


class TJMCommandError(Exception):
    """
    Custom exception to wrap around executions of any commands sent to TACC
    resource. This exception gets thrown first by the _execute_command method
    of the TACCJobManager class, upon any command executing returning a
    non-zero return code. The idea is to handle this exception gracefully and
    throw a more specific error in other methods.

    Attributes
    ----------
    system : str
        TACC System on which command was executed.
    user : str
        User that is executing command.
    command : str
        Command that threw the error.
    rc : str
        Return code.
    stdout : str
        Output from stdout.
    stderr : str
        Output from stderr.
    message : str
        Explanation of the error.
    """

    def __init__(
        self, system, user, command, rc, stderr, stdout, message="Non-zero return code."
    ):
        self.system = system
        self.user = user
        self.command = command
        self.rc = rc
        self.stderr = stderr.strip("\n")
        self.stdout = stdout.strip("\n")
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        msg = f"{self.message}"
        msg += f"\n{self.user}@{self.system}$ {self.command}"
        msg += f"\nrc     : {self.rc}"
        msg += f"\nstdout : {self.stdout}"
        msg += f"\nstderr : {self.stderr}"
        return msg


class TACCJMError(Exception):
    """
    Custom TACCJM exception for errors encountered when interacting with
    commands sent to TACCJM server via HTTP endpoints.

    Attributes
    ----------
    jm_id : str
        TACC Job Manager which command was sent to.
    user : str
        User that sent API call.
    res : requests.models.Response
        Response object containing info on API call that failed.
    message : str
        Str message explaining error.
    """

    def __init__(self, res, message: str = "API Error"):
        self.res = res
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        # Get response object
        res = self.res

        try:
            m = "\n".join([f"{k} : {v}" for k, v in res.json()["errors"].items()])
        except Exception as e:
            m = str(res) + "Unable to parse json errors. Raw content:"
            m += res.content.decode("utf-8")
            # m += f"HTTP Request: {self.get_http_str()}"

        return m

    def get_http_str(self):
        # Format HTTP request
        m = f"{self.message} - {res.status_code} {res.reason}:\n"
        m += "\n-----------START-----------\n"
        m += f"{res.request.method} {res.request.url}\r\n"
        m += "\r\n".join("{}: {}".format(k, v) for k, v in res.request.headers.items())
        if res.request.body is not None:
            # Fix body to remove psw if exists, don't want in logs
            body = [s.split("=") for s in res.request.body.split("&")]
            body = [x if x[0] != "psw" else (x[0], "") for x in body]
            body = "&".join(["=".join(x) for x in body])
            m += body

        return m
