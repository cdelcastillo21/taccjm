"""
TACCJobManager Utility Function


"""

import pdb
import os                       # OS system utility functions
import re
import json                     # For reading/writing dictionary<->json
import errno                    # For error messages
import configparser             # For reading configs
from typing import Tuple        # For type hinting
from taccjm.constants import JOB_TEMPLATE, APP_TEMPLATE, APP_SCRIPT_TEMPLATE
from prettytable import PrettyTable

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
    app_config['name'] = name
    job_config['app'] = name
    job_config['name'] = f'{name}-test-job'

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


def filter_res(res, fields, search=None, match=r".", filter_fun=None):
    """
    Print results

    Prints dictionary keys in list `fields` for each dictionary in res,
    filtering on the search column if specified with regular expression
    if desired.

    Parameters
    ----------
    res : List[dict]
        List of dictionaries containing response of an AgavePy call
    fields : List[string]
        List of strings containing names of fields to extract for each element.
    search : string, optional
        String containing column to perform string patter matching on to
        filter results.
    match : str, default='.'
        Regular expression to match strings in search column.
    output_file : str, optional
        Path to file to output result table to.

    """
    # Initialize Table
    x = PrettyTable(float_format="0.2")
    x.field_names = fields

    # Build table from results
    filtered_res = []
    for r in res:
        if filter_fun is not None:
            r = filter_fun(r)
        if search is not None:
            if re.search(match, r[search]) is not None:
                x.add_row([r[f] for f in fields])
                filtered_res.append(dict([(f, r[f]) for f in fields]))
        else:
            x.add_row([r[f] for f in fields])
            filtered_res.append(dict([(f, r[f]) for f in fields]))

    return str(x)

def format_app_dict(app):
    res = [{'attr':x, 'val': app[x]} for x in app.keys()]
    def _filter_fun(x):
        if x['attr'] in ['inputs', 'parameters', 'outputs']:
            if len(x['val']) > 0:
                x['val'] = filter_res(x['val'], ['name', 'desc'])
            else:
                x['val'] = ''
        return x
    str_res = filter_res(res, ['attr', 'val'], filter_fun=_filter_fun)
    return str_res

def format_job_dict(job):
    res = [{'attr':x, 'val': job[x]} for x in job.keys()]
    def _filter_fun(x):
        if x['attr'] in ['inputs', 'parameters']:
            val_list = [{'name': x[0], 'value':x[1]} for x in x['val'].items()]
            x['val'] = filter_res(val_list, ['name', 'value'])
        return x
    str_res = filter_res(res, ['attr', 'val'], filter_fun=_filter_fun)
    return str_res

