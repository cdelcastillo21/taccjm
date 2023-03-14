"""
TACCJM CLI

CLI for using TACCJM Client.
"""
import os

import click

import taccjm.tacc_ssh_api as tacc_api
from taccjm.cli.files import file_commands as files_cli
from taccjm.cli.scripts import script_commands as scripts_cli
from taccjm.cli.jobs import job_commands as jobs_cli
from taccjm.cli.utils import _get_client, build_table
from taccjm.utils import filter_res

from rich.console import Console
from rich.traceback import install
install(suppress=[click])

# from .apps import app_commands as apps_cli
# from .jobs import job_commands as jobs_cli
# from .scripts import script_commands as scripts_cli

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

TACC_PW = os.getenv("TACC_PW")

# Available return fields per command
_conn_fields = [
    "id",
    "sys",
    "user",
    "start",
    "last_ts",
    "log_level",
    "log_file",
    "home_dir",
    "work_dir",
    "scratch_dir",
]

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
CONSOLE = Console()


class NaturalOrderGroup(click.Group):
    """
    Class For custom ordering of commands in help output
    """

    def list_commands(self, ctx):
        return self.commands.keys()


@click.group(cls=NaturalOrderGroup)
@click.option(
    "--server",
    type=(str, int),
    default=None,
    help="Host and port of location of TACC JM server. Advanced feature, use with care.",
)
def cli(server):
    if server is not None:
        host, port = server
        _ = tacc_api.set_host(host, port)


cli.add_command(files_cli.files)
cli.add_command(scripts_cli.scripts)
cli.add_command(jobs_cli.jobs)
# cli.add_command(apps_cli.apps)


@cli.command(short_help="List active ssh connections")
@click.option(
    "-c",
    "--cols",
    multiple=True,
    type=click.Choice(_conn_fields, case_sensitive=False),
    default=["id", "sys", "user", "last_ts", "work_dir"],
    help="Column to search.",
    show_default=True,
)
@click.option(
    "-s",
    "--search",
    type=click.Choice(_conn_fields, case_sensitive=False),
    default="id",
    help="Column to search.",
    show_default=True,
)
@click.option(
    "-m",
    "--match",
    default=r".",
    show_default=True,
    help="Regular expression to match.",
)
def list(cols, search, match):
    """
    List TACC SSH Connections

    List available connections on server, along with info:
        id: str
        sys: str
        user: str
        start: datetime
        last_ts: datetime
        log_level: str
        log_file: str
        home_dir: str
        work_dir: str
        scratch_dir: str
    Can filter results on any column by specifying the column in the `--search`
    option and the regular expression to match on in the `--match` option.
    """
    res = tacc_api.list_sessions()
    table = build_table(res, cols, search=search, match=match)
    CONSOLE.print(table)
    # click.echo(str_res)


@cli.command(short_help="Initialize a TACC SSH connection.")
@click.argument("system")
@click.argument("user")
@click.option(
    "--conn_id",
    default=None,
    help="TACC connection id to show queue for. If none specified, then "
    + "defaults to first connection from a `taccjm list` command.",
)
@click.option(
    "-p",
    "--password",
    envvar="TACC_PW",
    default=None,
    help="If not passed in and environment variable TACC_PW is not set, will be prompted.",
)
@click.option("-m", "--mfa", default=None, help="TACC 2fa Code")
@click.option(
    "-r/-nr",
    "--restart/--no-restart",
    default=False,
    type=bool,
    help="Restart the job manager or not.",
)
def init(system, user, conn_id, password, mfa, restart):
    """
    Initialize TACC SSH Connection

    Logs in USER via an ssh connection to SYSTEM and labels it with connection
    id conn_id. JM_ID must be unique and not present in a `taccjm list`.
    """
    connection_id = f"taccjm-{system}" if conn_id is None else conn_id
    res = tacc_api.init(connection_id, system, user, password, mfa, restart)
    str_res = filter_res([res], ["id", "sys", "user", "last_ts", "work_dir"])
    click.echo(str_res)


@cli.command(short_help="Remove TACC SSH connection.")
@click.argument("conn_id")
def rm(conn_id):
    """
    Stop TACC SSH Connection
    """
    client = tacc_api.stop_session(conn_id)
    click.echo(f'Stopped {conn_id}:')
    str_res = filter_res([client], ["id", "sys", "user", "last_ts"],
                         search=search, match=match)
    click.echo(str_res)


@cli.command(short_help="Show SLURM job queue on JM_ID for USER.")
@click.option(
    "-c",
    "--conn_id",
    default=None,
    help="TACC connection id to show queue for. If none specified, then "
    + "defaults to first connection from a `taccjm list` command.",
)
@click.option(
    "-u",
    "--user",
    default=None,
    help="User to show job queue for. Use `all` ti show queue for all users",
)
@click.option(
    "-s",
    "--search",
    type=click.Choice(_queue_fields, case_sensitive=False),
    help="Column to search.",
    show_default=True,
    default="username",
)
@click.option(
    "-m",
    "--match",
    default=r".",
    show_default=True,
    help="Regular expression to match.",
)
def showq(conn_id, user, search, match):
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
    client = _get_client(conn_id)
    res = client.showq(user=user)
    str_res = filter_res(res, _queue_fields)
    click.echo(f"SLURM Queue for {user} on {conn_id}:")
    click.echo(str_res)


@cli.command()
@click.option(
    "-c",
    "--conn_id",
    default=None,
    help="TACC connection id to get allocatiosn for. If none specified, then "
    + "defaults to first connection from a `taccjm list` command.",
)
@click.option(
    "--search",
    type=click.Choice(_allocation_fields, case_sensitive=False),
    help="Column to search.",
    show_default=True,
    default="name",
)
@click.option(
    "--match", default=r".", show_default=True, help="Regular expression to match."
)
def allocations(conn_id, search, match):
    """
    Get TACC Allocations

    Return TACC allocations on system connected to by conn_id. For each
    allocation, returns remaining SUs. if no conn_id specified, defaults to
    first SSH connection in `taccjm list`.
    """
    client = _get_client(conn_id)
    res = client.get_allocations()
    str_res = filter_res(res, _allocation_fields, search, match)
    click.echo(f"Allocations for {conn_id}:")
    click.echo(str_res)


@cli.command(short_help="Locate TACCJM server")
@click.option("-s/-ns", "--start/--no-start", is_flag=True, default=False)
@click.option("-k/-nk", "--kill/--no-kill", is_flag=True, default=False)
def find_server(start, kill):
    """
    Find TACCJM Server

    Locate (host,port) where server is running. Can kill/start server as
    needed with appropriate flags. Note that will only show server for current
    (host, port) being used. To change (host, port) combo, use
    `taccjm --server hostname 1111 find-server` with appropriate host and port.
    """
    res = tacc_api.find_server(start, kill)
    res = [
        {
            "type": x,
            "pid": res[x].pid,
        }
        for x in res.keys()
    ]
    # "create_time": datetime.utcfromtimestamp(res[x]._create_time).strftime('%Y-%m-%d %H:%M:%S'),
    # str_res = filter_res(res, ["type", "pid", "create_time"])
    str_res = filter_res(res, ["type", "pid"])
    click.echo(str_res)


@cli.command(short_help="Execute command on system via ssh")
@click.argument("command")
@click.option(
    "-c",
    "--conn_id",
    default=None,
    help="TACC connection id to run command on. If none specified, then "
    + "defaults to first connection from a `taccjm list` command.",
)
@click.option(
    "-w/-nw",
    "--wait/--no-wait",
    is_flag=True,
    default=True,
    help="Wait for command to execute",
)
@click.option(
    "-e/-ne",
    "--error/--no-error",
    is_flag=True,
    default=True,
    help="Throw error if encountered.",
)
@click.option(
    "-l/-nl",
    "--local/--no-local",
    is_flag=True,
    default=False,
    help="Run comand locally or remotely",
)
def exec(command, conn_id, wait, error, local):
    """
    Execute an arbitrary command via ssh.
    """
    client = _get_client(conn_id)
    res = client.exec(command, wait, error, local)
    str_command = command[:20] + "..." if len(command) > 20 else command
    if wait:
        click.echo(filter_res([res], ["id", "status", "rt"]))
        click.echo(f"{client.user}@{client.id}$ {str_command}" + f"\n{res['stdout']}")
    else:
        click.echo(filter_res([res], ["id", "status", "ts"]))
        click.echo(
            f"Use `taccjm process {res['id']} -w` to wait for command " + "to finish"
        )


@cli.command(short_help="Process a command that was executed via ssh")
@click.argument("command_id")
@click.option(
    "-c",
    "--conn_id",
    default=None,
    help="TACC connection id to run command on. If none specified, then "
    + "defaults to first connection from a `taccjm list` command.",
)
@click.option(
    "-w/-nw",
    "--wait/--no-wait",
    is_flag=True,
    default=True,
    help="Wait for command to execute",
    show_default=True,
)
@click.option(
    "-e/-ne",
    "--error/--no-error",
    is_flag=True,
    default=True,
    help="Throw error if encountered.",
    show_default=True,
)
@click.option(
    "-nbytes", default=0, show_default=True, help="Run comand locally or remotely"
)
@click.option(
    "-l/-nl",
    "--local/--no-local",
    is_flag=True,
    default=False,
    help="Run comand locally or remotely",
    show_default=True,
)
def process(command_id, conn_id, wait, error, nbytes, local):
    """
    Execute an arbitrary command via ssh.
    """
    max_cmnd_str_len = 50
    client = _get_client(conn_id)
    res = client.process(command_id, wait, error, nbytes, local)
    str_c = res["cmd"][:max_cmnd_str_len] + "..." if len(
            res["cmd"]) > max_cmnd_str_len else res["cmd"]
    if wait:
        click.echo(filter_res([res], ["id", "status", "rt"]))
        click.echo(f"{client.user}@{client.id}$ {str_c}" + f"\n{res['stdout']}")
    else:
        click.echo(filter_res([res], ["id", "status", "ts"]))
        click.echo(f"{client.user}@{client.id}$ {str_c}")
        click.echo(
            f"... use `taccjm process {res['id']} -w` to wait for "
            + "command to finish"
        )
