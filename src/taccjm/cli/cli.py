"""
TACCJM CLI

CLI for using TACCJM Client.
"""
import pdb
import re
import os
from pathlib import Path

import click
from datetime import datetime

import taccjm.taccjm_client as tjm
from taccjm.utils import filter_res

from .files import file_commands as files_cli
from .apps import app_commands as apps_cli
from .jobs import job_commands as jobs_cli
from .scripts import script_commands as scripts_cli

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

TACC_PW = os.getenv('TACC_PW')

# Available return fields per command
_jm_fields = ["jm_id", "sys", "user", "apps_dir", "jobs_dir"]
_queue_fields = [
    "job_id",
    "job_name",
    "username",
    "state",
    "nodes",
    "remaining",
    "start_time",
]
_allocation_fields = ["name", "service_units", "exp_date"]

class NaturalOrderGroup(click.Group):
    """
    Class For custom ordering of commands in help output
    """
    def list_commands(self, ctx):
        return self.commands.keys()

@click.group(cls=NaturalOrderGroup)
@click.option("--server", type=(str, int), default=None,
              help="Host and port of location of TACC JM server. Advanced feature, use with care.")
def cli(server):
    if server is not None:
        host, port = server
        _ = tjm.set_host(host, port)

cli.add_command(apps_cli.apps)
cli.add_command(jobs_cli.jobs)
cli.add_command(files_cli.files)
cli.add_command(scripts_cli.scripts)

@cli.command(short_help="List active job managers")
@click.option("-s", "--search",
              type=click.Choice(_jm_fields, case_sensitive=False),
              default="jm_id",
              help="Column to search.",
              show_default=True)
@click.option("-m", "--match", default=r".", show_default=True,
              help="Regular expression to match.")
def list(search, match):
    """
    List TACC Job Managers

    List available Job Managers on server, along with info: jm_id, system, user,
    application directory, and jobs directory. Can filter results on any column
    by specifying the column in the `--search` option and the regular expression
    to match on in the `--match` option.
    """
    res = tjm.list_jms()
    str_res = filter_res(res, _jm_fields, search=search, match=match)
    click.echo(str_res)
@cli.command(short_help="Initialize a TACC connection.")
@click.argument("jm_id", )
@click.argument("system")
@click.argument("user")
@click.option("-p", "--password", envvar='TACC_PW', default=None,
              help='If not passed in and environment variable TACC_PW is not set, will be prompted.')
@click.option("-m", "--mfa", default=None, help='TACC 2fa Code')
@click.option("-r/-nr", "--restart/--no-restart", default=False, type=bool, help='Restart the job manager or not.')
def init(jm_id, system, user, password, mfa, restart):
    """
    Initialize TACC Job Manager

    Logs in USER via an ssh connection to SYSTEM and labels it with job manager
    id JM_ID. JM_ID must be unique and not present in a `taccjm list`.
    """
    res = tjm.init_jm(jm_id, system, user, password, mfa, restart)
    str_res = filter_res([res], _jm_fields)
    click.echo(str_res)


@cli.command(short_help="Show SLURM job queue on JM_ID for USER.")
@click.option("--jm_id", default=None,
              help="Job Manager to show queue for. If none specified, then defaults to first job manager from a `taccjm list` command.")
@click.option("--user", default=None, help="User to show job queue for. To show queue for all users, specify 'all' as the user")
@click.option(
    "--search",
    type=click.Choice(_queue_fields, case_sensitive=False),
    help="Column to search.",
    show_default=True,
    default="username",
)
@click.option("--match", default=r".", show_default=True,
              help="Regular expression to match.")
def queue(jm_id, user, search, match):
    """
    Show TACC SLURM Job Queue

    Returns the active SLURM Job queue on TACC system that JM_ID is connected
    to, with fields:\n
        - job_id : SLURM job id. NOT TACCJM Job ID.\n
        - job_name : SLURM job name.\n
        - username : User that launched job.\n
        - state : State of job.\n
        - nodes : Nodes request.\n
        - remaining : Time remaining in job if running.\n
        - start_time : Start time of job.

    Use `--search` and `--match` flags to filter results.
     """
    if jm_id is None:
        jms = tjm.list_jms()
        if len(jms) == 0:
            raise TACCJMError('No JM specified (--jm_id) and no JM')
        jm_id = tjm.list_jms()[0]['jm_id']
    res = tjm.get_queue(jm_id, user)
    str_res = filter_res(res, _queue_fields)
    click.echo(f'SLURM Queue for {user} on {jm_id}:')
    click.echo(str_res)


@cli.command()
@click.option("--jm_id", default=None)
@click.option(
    "--search",
    type=click.Choice(_allocation_fields, case_sensitive=False),
    help="Column to search.",
    show_default=True,
    default="name",
)
@click.option("--match", default=r".", show_default=True,
              help="Regular expression to match.")
def allocations(jm_id, search, match):
    """
    Get TACC Allocations

    Return TACC allocations on system connected to by JM_ID. For each
    allocation, returns remaining SUs.
    """
    if jm_id is None:
        jms = tjm.list_jms()
        if len(jms) == 0:
            raise TACCJMError('No JM specified (--jm_id) and no JM')
        jm_id = tjm.list_jms()[0]['jm_id']
    res = tjm.get_allocations(jm_id)
    str_res = filter_res(res, _allocation_fields, search, match)
    click.echo(f'Allocations for {jm_id}:')
    click.echo(str_res)

@cli.command(short_help="Locate TACCJM server")
@click.option("--start/--no-start", default=False)
@click.option("--kill/--no-kill", default=False)
def find_server(start, kill):
    """
    Find TACCJM Server

    Locate (host,port) where server is running. Can kill/start server as
    needed with appropriate flags. Note that will only show server for current
    (host, port) being used. To change (host, port) combo, use
    `taccjm --server hostname 1111 find-server` with appropriate host and port.
    """
    res = tjm.find_tjm_processes(start, kill)
    res = [{
        "type": x,
        "pid": res[x].pid,
    } for x in res.keys()]
        # "create_time": datetime.utcfromtimestamp(res[x]._create_time).strftime('%Y-%m-%d %H:%M:%S'),
    # str_res = filter_res(res, ["type", "pid", "create_time"])
    str_res = filter_res(res, ["type", "pid"])
    click.echo(str_res)


