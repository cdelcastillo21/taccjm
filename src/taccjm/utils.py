"""
TACCJobManager Utility Function


"""

import concurrent.futures
import json  # For reading/writing dictionary<->json
import logging
import math
import os  # OS system utility functions
import pdb
import re
import stat
import sys
import tarfile  # For sending compressed directories
import time
from datetime import datetime, timedelta
from fnmatch import fnmatch  # For unix-style filename pattern matching
from pathlib import Path
from typing import Tuple  # For type hinting

import numpy as np
import pandas as pd
from prettytable import PrettyTable
from pythonjsonlogger import jsonlogger

from taccjm.constants import APP_SCRIPT_TEMPLATE, APP_TEMPLATE, JOB_TEMPLATE

DEFAULT_SCRIPTS_PATH = Path(__file__).parent / "scripts"

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"


def update_dic_keys(d: dict, **kwargs) -> dict:
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


def create_template_app(
    name: str,
    dest_dir: str = ".",
    app_config: dict = APP_TEMPLATE,
    job_config: dict = JOB_TEMPLATE,
    script: str = APP_SCRIPT_TEMPLATE,
    **kwargs,
) -> Tuple[dict, dict]:
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
    app_config["name"] = name
    job_config["app"] = name
    job_config["name"] = f"{name}-test-job"

    # Create application directory - Fails if already exists
    app_dir = os.path.join(dest_dir, name)
    os.mkdir(app_dir)

    # Create application assets directory
    assets_dir = os.path.join(app_dir, "assets")
    os.mkdir(assets_dir)

    # Write app config json file
    app_config_path = os.path.join(app_dir, "app.json")
    with open(app_config_path, "w") as f:
        json.dump(app_config, f)

    # Write job config json file
    job_config_path = os.path.join(app_dir, "job.json")
    with open(job_config_path, "w") as f:
        json.dump(job_config, f)

    # Write entry point script
    with open(os.path.join(assets_dir, "run.sh"), "w") as f:
        f.write(script)

    return (app_config, job_config)


# TODO: this function can most likely be merged with filter_res
def filter_files(
    files,
    attrs=["filename", "st_size"],
    hidden: bool = False,
    search: str = None,
    match: str = r".",
):
    """list_files utility function to filter results"""

    # Filter hidden files
    if not hidden:
        files = [f for f in files if not f["filename"].startswith(".")]

    # Filter attrs
    files = [{a: f[a] for a in attrs} for f in files]

    if search is not None:
        files = [f for f in files if re.search(match, f[search]) is not None]

    return files


def filter_res(res, fields, search=None, match=r".", filter_fun=None,
               max_n=int(1e6)):
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

        if len(filtered_res) > max_n:
            break

    return str(x)


def format_app_dict(app):
    res = [{"attr": x, "val": app[x]} for x in app.keys()]

    def _filter_fun(x):
        if x["attr"] in ["inputs", "parameters", "outputs"]:
            if len(x["val"]) > 0:
                x["val"] = filter_res(x["val"], ["name", "desc"])
            else:
                x["val"] = ""
        return x

    str_res = filter_res(res, ["attr", "val"], filter_fun=_filter_fun)
    return str_res


def format_job_dict(job):
    res = [{"attr": x, "val": job[x]} for x in job.keys()]

    def _filter_fun(x):
        if x["attr"] in ["inputs", "parameters"]:
            val_list = [{"name": x[0], "value": x[1]} for x in x["val"].items()]
            x["val"] = filter_res(val_list, ["name", "value"])
        return x

    str_res = filter_res(res, ["attr", "val"], filter_fun=_filter_fun)

    return str_res


def get_default_script(script_name, ret="path"):
    """
    Get a pre-configured TACC script to run from this repo
    """
    script_path = DEFAULT_SCRIPTS_PATH / script_name
    if not script_path.exists():
        raise ValueError(f"Script {script_name} not a default taccjm script")
    if ret == "path":
        return str(script_path.resolve())
    else:
        with open(str(script_path), "r") as fp:
            script_text = fp.read()
        return script_text


def init_logger(
    name,
    log_config: dict = {"output": sys.stdout, "fmt": "json", "level": logging.INFO},
):
    """
    Format a logger instance
    """
    def_config = {"output": sys.stdout, "fmt": "json", "level": logging.CRITICAL}
    if log_config is None:
        log_config = def_config
        def_config["output"] = None
    else:
        bad = [x for x in log_config.keys() if x not in def_config.keys()]
        if len(bad) > 0:
            raise ValueError(
                f"Invalid log config keys {bad}. Valid: {def_config.keys()}"
            )
        def_config.update(log_config)
        log_config = def_config

    logger = logging.getLogger(name)
    if logger.hasHandlers():
        return log_config, logger

    if isinstance(log_config["output"], str):
        logHandler = logging.FileHandler(log_config["output"])
    else:
        logHandler = logging.StreamHandler(log_config["output"])
    if log_config["fmt"] == "json":
        formatter = jsonlogger.JsonFormatter(
            "%(asctime)s %(name)s - %(levelname)s:%(message)s"
        )
    else:
        formatter = logging.Formatter(
            "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        )

    logHandler.setFormatter(formatter)
    logger.addHandler(logHandler)
    logger.setLevel(log_config["level"])
    logger.info(f"Logger {name} initialized", extra={"config": log_config})

    return log_config, logger


def read_log(log):
    """
    Read a the log currently being written to by log handler. Assumes json
    logging to a file handler.
    """
    return pd.read_json(log.handlers[0].baseFilename, lines=True)


def format_hours(hours):
    """
    Format an integer or float amount of hours into a string for slurm scripts.

    Example
    -------
    >>> from taccjm.utils import format_hours
    >>> print(format_hours(2.5))
    02:30:00
    """
    runtime = timedelta(hours=hours)
    return runtime.strftime("%H:%M:%S")


def hours_to_runtime_str(hours):
    """
    Convert an int/float amount of hours to a runtime string for SLURM scripts.
    """
    days = math.floor(hours / 24)
    hours = hours - 24 * days
    minutes = int(60 * (hours - math.floor(hours)))
    hours = int(hours)
    if days:
        return f"{days}-{hours:02}:{minutes:02}:00"
    return f"{hours:02}:{minutes:02}:00"


def validate_file_attrs(attrs):
    """list_files utility function to parse valid file attribute lists."""
    avail_attrs = [
        "filename",
        "st_atime",
        "st_gid",
        "st_mode",
        "st_mtime",
        "st_size",
        "st_uid",
        "ls_str",
    ]
    invalid_attrs = [a for a in attrs if a not in avail_attrs]
    if len(invalid_attrs) > 0:
        raise ValueError(f"Requested Invalid file attrs {invalid_attrs}")
    if "filename" not in attrs:
        attrs = ["filename"] + attrs

    return attrs


def get_ts(fmt="%Y%m%d %H:%M:%S"):
    """
    Standarize timestamps returned
    """
    return datetime.fromtimestamp(time.time()).strftime(fmt)


def tar_file(to_compress, tar_file, arc_name=None, file_filter="*"):
    """ """
    with tarfile.open(tar_file, "w:gz") as tar:

        def filter_fun(x):
            return x if fnmatch(x.name, file_filter) else None

        tar.add(to_compress, arcname=arc_name, filter=filter_fun)


def get_log_level_str(log_int):
    """ """
    loglevels = np.array([0, 10, 20, 30, 40, 50])
    max_log_level = np.max(loglevels[loglevels < log_int])

    loglevels = {
        "50": "critical",
        "40": "error",
        "30": "warning",
        "20": "info",
        "10": "debug",
        "0": "notset",
    }

    return loglevels[str(int(max_log_level))]


def get_log_level(loglevel):
    """ """
    loglevels = {
        "critical": logging.CRITICAL,
        "error": logging.ERROR,
        "warning": logging.WARNING,
        "info": logging.INFO,
        "debug": logging.DEBUG,
        "notset": logging.NOTSET,
    }

    if loglevel not in loglevels.keys():
        raise ValueError(f"Invalid loglevel:{loglevel}: {loglevels.keys()}")

    return loglevels[loglevel]


def parse_allocations_string(allocs_return):
    """
    Given a string return fro ma `/usr/local/etc/taccinfo` call on TACC systems,
    parse available allocations for a user.
    """
    # Parse allocation info
    allocations = set([x.strip() for x in allocs_return.split("\n")[2].split("|")])
    allocations.remove("")
    allocations = [x.split() for x in allocations]
    allocations = [
        {"name": x[0], "service_units": int(x[1]), "exp_date": x[2]}
        for x in allocations
    ]

    return allocations


def stat_file_or_folder(full_path):
    """ """
    try:
        stat_result = os.stat(full_path)
        return (full_path, stat_result)
    except Exception as e:
        return f"Error getting stats for {full_path}: {e}"


def stat_all_files_and_folders(
    path,
    f_attrs=["st_atime", "st_gid", "st_mode", "st_mtime", "st_size", "st_uid"],
    recurse=False,
):
    """
    Stats all files and folders at a path.
    """

    def _process(res):
        d = {"filename": res[0]}
        for x in f_attrs:
            d[x] = res[1].__getattribute__(x)
        return d

    result = stat_file_or_folder(str(Path(path).resolve()))
    if isinstance(result, str):
        raise ValueError(result)
    if not stat.S_ISDIR(result[1].st_mode) or not recurse:
        return [_process(result)]

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for dirpath, dirnames, filenames in os.walk(path):
            for name in filenames + dirnames:
                full_path = os.path.join(dirpath, name)
                future = executor.submit(stat_file_or_folder, full_path)
                futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if isinstance(result, tuple):
                results.append(_process(result))
            else:
                pass

    return results


def check_path(path: str):
    """
    Check path for funny business
    """
    if any(
        [
            path.startswith("../"),
            path.endswith("/.."),
            "/../" in path,
        ]
    ):
        return False
    else:
        return True
