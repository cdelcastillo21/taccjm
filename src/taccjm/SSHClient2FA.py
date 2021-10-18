"""
SSHClient2FA Class


Note:


References:

"""

import pdb                      # Debug
import socket                   # This method requires that we create our own socket
import getpass                  # Allows for secure prompting and collection of the user password
import logging                  # Used to setup the Paramiko log file
import paramiko                 # Provides SSH functionality


# Better way to do this?
global user, pw, mfa, user_p, pw_p, mfa_p


logger = logging.getLogger(__name__)


def inter_handler(title, instructions, prompt_list):
    """inter_handler: the callback for paramiko.transport.auth_interactive

    The prototype for this function is defined by Paramiko, so all of the
    arguments need to be there, even though we don't use 'title' or
    'instructions'.

    The function is expected to return a tuple of data containing the
    responses to the provided prompts. Experimental results suggests that
    there will be one call of this function per prompt, but the mechanism
    allows for multiple prompts to be sent at once, so it's best to assume
    that that can happen.

    Since tuples can't really be built on the fly, the responses are
    collected in a list which is then converted to a tuple when it's time
    to return a value.

    Experiments suggest that the username prompt never happens. This makes
    sense, but the Username prompt is included here just in case.
    """
    global user, pw, mfa, user_p, pw_p, mfa_p
    resp = []  #Initialize the response container

    # Make pairs of (response, prompt)
    prompts = [(user, user_p), (pw, pw_p), (mfa, mfa_p)]

    #Walk the list of prompts that the server sent that we need to answer
    for pr in prompt_list:
        # str() - amke sure dealing with a string rather than a unicode string
        # strip() - get rid of any padding spaces sent by the server

        # Match prompts with responses
        for p in prompts:
            if str(pr[0]).strip() == p[1]:
                resp.append(p[0])

    return tuple(resp)  #Convert the response list to a tuple and return it


class SSHClient2FA(paramiko.SSHClient):
    """Paramiko SSH Client with 2-Factor Authentication

    This class inherits from the usual paramiko SSHClient class, but overwrites
    the connect method to allow for connections where 2-Factor authenticaion
    may be required. It does this by creating  its own custom inter_handler
    method, a callback method for paramiko.transport.auth_interactive that
    defines what prompts to expect and how to respond to them when creating an
    ssh connection.
    """

    def __init__(self, user_prompt="Username:",
            psw_prompt="Password:", mfa_prompt=None):
        """
        Create a new SSHClient.
        """
        super().__init__()

        global user_p, pw_p, mfa_p
        user_p = user_prompt
        pw_p = psw_prompt
        mfa_p = mfa_prompt


    def connect(
        self,
        hostname,
        uid=None,
        pswd=None,
        mfa_pswd=None
    ):
        """
        Connect to an SSH server and authenticate to it.
        """
        global user, pw, mfa, user_p, pw_p, mfa_p

        #Get the username, password, and MFA token code from the user
        user = uid if uid is not None else input(user_p)
        pw = pswd if pswd is not None else getpass.getpass(pw_p)
        mfa = None if mfa_p is None else \
                mfa_pswd if mfa_pswd is not None else input(mfa_p)

        #Create a socket and connect it to port 22 on the host
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((hostname, 22))

        t = self._transport = paramiko.Transport(sock)

        #Tell Paramiko that the Transport is going to be used as a client
        t.start_client(timeout=10)

        #Begin authentication; note that the username and callback are passed
        t.auth_interactive(user, inter_handler)

        return user

