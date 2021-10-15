"""
TACCJobManager Utility Function


"""

import os                       # OS system utility functions
import json                     # For reading/writing dictionary<->json
import errno                    # For error messages
import configparser             # For reading configs
from jinja2 import Template     # For templating input json files
from typing import Tuple        # For type hinting
from taccjm.constants import *  # Library constants

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

def update_dic_keys(d:dict, **kwargs) -> dict:
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


def load_templated_json_file(path:str, config_path:str, **kwargs) -> dict:
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
    config_path : str
        Path to config file (.ini)
    **kwargs : dict, optional
        Any extra keyword arguments will be interpreted as items to override in
        from the loaded json config file.

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


def create_template_app(name:str,
        dest_dir:str='.',
        app_template:dict=APP_TEMPLATE,
        script:str=APP_SCRIPT_TEMPLATE,
        **kwargs) -> Tuple[dict, dict]:
    """
    Create files for a templated HPC application at given directory.

    Parameters
    ----------
    dir : str, default='.'
        Directory to put application.
    app_template : dict, default=taccjm.constants.APP_TEMPLATE
        Dictionary containing application template.
    script : dict, default=taccjm.constants.APP_SCRIPT_TEMPLATE
        Dictionary containing application template.
    **kwargs : dict, optional
        Any keyword arguments will be interpreted as an override to the template
        paremters.

    Returns
    -------
    configs : Tuple[dict, dict]
        Tuple of app and job configs as loaded from application created.

    Raises
    -------
    FileExistsError
        If application with given name already exists in local directory.
    """
    # Update app template dictionary with passed in arguments
    app_template.update(kwargs)

    # Create application directory - Fails if already exists
    app_dir = os.path.join(dest_dir, name)
    os.mkdir(app_dir)

    # Create application assets directory
    assets_dir = os.path.join(app_dir, 'assets')
    os.mkdir(assets_dir)

    # Write project config file
    config_path = os.path.join(app_dir, 'project.ini')
    with open(config_path, 'w') as f:
        f.write(CONFIG_TEMPLATE)

    # Write app config json file
    app_config_path = os.path.join(app_dir, 'app.json')
    with open(app_config_path, 'w') as f:
        f.write(json.dump(app_template))

    # Write job config json file
    job_config_path = os.path.join(job_dir, 'job.json')
    with open(job_config_path, 'w') as f:
        f.write(json.dump(JOB_TEMPLATE))

    # Write entry point script
    with open(os.path.join(assets_dir, 'run.sh'), 'w') as f:
        f.write(json.dump(script))

    # Load in app and job config from templates as they were created
    app_config = load_templated_json_file(app_config_path, config_path)
    job_config = load_templated_json_file(job_config_path, config_path)

    return (app_config, job_config)


