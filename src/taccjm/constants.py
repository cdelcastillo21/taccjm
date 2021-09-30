"""
TACCJM Constants

"""

import os

# Location on disk where to find taccjm source code
TACCJM_SOURCE = os.path.dirname(__file__)

# Dir where to store taccjm logs and other files
TACCJM_DIR = os.path.join(os.path.expanduser("~"),'.taccjm')

# Default host and port to start taccjm servers on
TACCJM_PORT = 8221
TACCJM_HOST = 'localhost'

def make_taccjm_dir():
    """
    Make TACCJM Directories

    Make local directory, at ~/.taccjm, to store taccjm files, if it needs to
    be created.

    Parameters
    ----------

    Returns
    -------

    """
    if not os.path.exists(TACCJM_DIR):
        os.makedirs(TACCJM_DIR)

