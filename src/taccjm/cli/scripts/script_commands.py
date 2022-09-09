"""
TACCJM Scripts CLI
"""
import pdb
import re
from pathlib import Path

import click
import pandas as pd
from prettytable import PrettyTable

import taccjm.taccjm as tjm
from taccjm.exceptions import TACCJMError
from taccjm.utils import filter_res

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

@click.group(short_help="list/deploy/run")
@click.option("-j", "--jm_id", default=None,
              help="Job Manager to execute operation on. Defaults to first available.")
@click.pass_context
def scripts(ctx, jm_id):
    """
    TACC Job Manager Scripts

    CLI Entrypoint for commands related to scripts.
    """
    if jm_id is None:
        jms = tjm.list_jms()
        if len(jms) == 0:
            raise TACCJMError('No JM specified (--jm_id) and no JMs already initialized.')
        jm_id = tjm.list_jms()[0]['jm_id']
    ctx.ensure_object(dict)
    ctx.obj['jm_id'] = jm_id


@scripts.command(short_help="List deployed scripts.")
@click.option("-m", "--match", default=r".", show_default=True,
              help="Regular expression to search script names for.")
@click.pass_context
def list(ctx, match):
    """
    List scripts deployed on JM_ID
    """
    jm_id = ctx.obj['jm_id']
    res = tjm.list_scripts(jm_id)
    str_res = filter_res([{
        "name": x
    } for x in res], ["name"],
                          search="name",
                          match=match)
    click.echo(str_res)


@scripts.command(short_help="Deploy a local script.")
@click.argument("path")
@click.option("-r", "--rename", default=None,
              help="Name to give to deployed script. Defaults to script file name.")
@click.pass_context
def deploy(ctx, path, rename):
    """
    Deploy Script

    Deploy a local script onto a TACC system.
    """
    jm_id = ctx.obj['jm_id']
    if rename is None:
        script_name = str(Path(path).stem)
        res = tjm.deploy_script(jm_id, path)
    else:
        script_name = str(Path(rename).stem)
        res = tjm.deploy_script(jm_id, rename, local_file=path)
    res = tjm.list_scripts(jm_id)
    str_res = filter_res([{
        "name": x
    } for x in res], ["name"],
                          search="name",
                          match=script_name)
    click.echo(str_res)


@scripts.command(short_help="Run deployed script.")
@click.argument("script", type=str)
@click.argument("args", nargs=-1, default=None)
@click.pass_context
def run(ctx, script, args):
    """
    Run Deployed Script

    Run SCRIPT on JM_ID with passed in ARGS. SCRIPT must have been deployed
    already on JM_ID. Returns stdout. Take care with scripts that output a large
    amount of data. It is better practice for large output to be sent to a log
    file and for that to be downloaded seperately.
    """
    jm_id = ctx.obj['jm_id']
    res = tjm.run_script(jm_id, script, args=args)
    click.echo(res)

