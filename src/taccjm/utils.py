"""
TACCJobManager Utility Function


"""

import os                       # OS system utility functions
import json                     # For reading/writing dictionary<->json
import errno                    # For error messages
import configparser             # For reading configs
from jinja2 import Template     # For templating input json files

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

def update_dic_keys(d, **kwargs):
    """
    Utility to update a dictionary, with only updating singular parameters in
    sub-dictionaries. Used when updating and configuring/templating job configs.

    Parameters
    ----------
    d : dict
        Dictioanry to update
    **kwargs : dict, optional
        All extra keyword arguments will be interpreted as items to override in
        d.

    Returns
    -------
    err_code : int
        An SFTP error code int like SFTP_OK (0).

    """
    # Treat kwargs as override parameters
    for key, value in kwargs.items():
        old_val = d.get(key)
        if type(old_val) is dict:
            d[key].update(value)
        else:
            d[key] = value

    return d


def load_templated_json_file(path, config_path, **kwargs):
    """
    Loads a local json config found at path and templates it using jinja with
    the values found in config ini file found at config_path . For example, if
    json file contains `{{ a.b }}`, and `config={'a':{'b':1}}`, then `1` would
    be substituted in (note nesting). All extra keyword arguments will be
    interpreted as job config overrides.

    Parameters
    ----------
    path : str
        Local path to json file.
    config : dict
        Dictionary with values to substitute in for jinja templates
        in json file.

    Returns
    -------
    json_config : dict
        json config from file templated appropriately.

    Raises
    ------
    FileNotFoundError
        if json or config file do not exist.

    """
    # Check if it exists - If it doesn't config parser won't error
    if not os.path.exists(config_path):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT),
                config_path)

    # Read project config file
    config_parse = configparser.ConfigParser()
    config_parse.read(config_path)
    config = config_parse._sections

    with open(path) as file_:
        json_config = json.loads(Template(file_.read()).render(config))

    # Treat kwargs as override parameters
    json_config = update_dic_keys(json_config, **kwargs)

    # Return json_config
    return json_config

