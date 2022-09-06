"""
TACCJobManager Utility Function


"""

import os                       # OS system utility functions
import json                     # For reading/writing dictionary<->json
import errno                    # For error messages
import configparser             # For reading configs
from typing import Tuple        # For type hinting
from taccjm.constants import JOB_TEMPLATE, APP_TEMPLATE, APP_SCRIPT_TEMPLATE

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

def create_template_app(name:str,
        dest_dir:str='.',
        app_config:dict=APP_TEMPLATE,
        job_config:dict=JOB_TEMPLATE,
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
    app_config.update(kwargs)

    # Create application directory - Fails if already exists
    app_dir = os.path.join(dest_dir, name)
    os.mkdir(app_dir)

    # Create application assets directory
    assets_dir = os.path.join(app_dir, 'assets')
    os.mkdir(assets_dir)

    # Write app config json file
    app_config_path = os.path.join(app_dir, 'app.json')
    with open(app_config_path, 'w') as f:
        json.dump(app_config, f)

    # Write job config json file
    job_config_path = os.path.join(app_dir, 'job.json')
    with open(job_config_path, 'w') as f:
        json.dump(job_config, f)

    # Write entry point script
    with open(os.path.join(assets_dir, 'run.sh'), 'w') as f:
        f.write(script)

    return (app_config, job_config)


