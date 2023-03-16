"""
TACCJM CLI

CLI for using TACCJM Client.
"""
import pdb
import os

from rich.console import Console
from rich.traceback import install
import rich_click as click

from taccjm.client.tacc_ssh_api import list_sessions, init as ssh_init, \
        stop as ssh_stop
from taccjm.cli.utils import build_table, _get_client, _session_field_fmts

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

install(suppress=[click])
CONSOLE = Console()


@click.group(short_help="list/init/rm")
@click.pass_context
def sessions(ctx):
    """
    TACC Job Manager Shell
    """
    ctx.ensure_object(dict)


@sessions.command(short_help="List active ssh sessions")
@click.pass_context
def list(ctx):
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
    rows = list_sessions()
    str_res = build_table(rows,
                          fields=ctx.obj['cols'],
                          search=ctx.obj['search'],
                          match=ctx.obj['match'],
                          fmts=_session_field_fmts)

    CONSOLE.print(str_res)


@sessions.command(short_help="Initialize a TACC SSH session.")
@click.argument("system")
@click.argument("user")
@click.option("-m", "--mfa", default=None, help="TACC 2fa Code")
@click.option(
    "-r/-nr",
    "--restart/--no-restart",
    default=False,
    type=bool,
    help="Restart the job manager or not.",
)
@click.option(
    "-s",
    "--session_id",
    default=None,
    help="Session ID to execute operation on. Defaults to first available.",
)
@click.pass_context
def init(ctx, system, user, mfa, restart=False, session_id=None):
    """
    Initialize TACC SSH Connection

    Logs in USER via an ssh connection to SYSTEM and labels it with connection
    id conn_id. JM_ID must be unique and not present in a `taccjm list`.
    """
    s_id = f"taccjm-{system}" if session_id is None else session_id
    row = ssh_init(s_id, system, user, mfa=mfa, restart=restart)
    str_res = build_table([row],
                          fields=ctx.obj['cols'],
                          search=ctx.obj['search'],
                          match=ctx.obj['match'],
                          fmts=_session_field_fmts)
    CONSOLE.print(str_res)


@sessions.command(short_help="Remove TACC SSH connection.")
@click.argument("session_id")
@click.pass_context
def rm(ctx, session_id):
    """
    Stop TACC SSH Connection
    """
    row = ssh_stop(session_id)
    str_res = build_table([row],
                          fields=ctx.obj['cols'],
                          search=ctx.obj['search'],
                          match=ctx.obj['match'],
                          fmts=_session_field_fmts)
    CONSOLE.print(str_res)



# @cli.command(short_help="Show SLURM job queue on JM_ID for USER.")
# @click.option(
#     "-c",
#     "--conn_id",
#     default=None,
#     help="TACC connection id to show queue for. If none specified, then "
#     + "defaults to first connection from a `taccjm list` command.",
# )
# @click.option(
#     "-u",
#     "--user",
#     default=None,
#     help="User to show job queue for. Use `all` ti show queue for all users",
# )
# @click.option(
#     "-s",
#     "--search",
#     type=click.Choice(_queue_fields, case_sensitive=False),
#     help="Column to search.",
#     show_default=True,
#     default="username",
# )
# @click.option(
#     "-m",
#     "--match",
#     default=r".",
#     show_default=True,
#     help="Regular expression to match.",
# )
# def showq(conn_id, user, search, match):
#     """
#     Show TACC SLURM Job Queue
# 
#     Returns the active SLURM Job queue on TACC system that JM_ID is connected
#     to, with fields:\n
#         - job_id : SLURM job id. NOT TACCJM Job ID.\n
#         - job_name : SLURM job name.\n
#         - username : User that launched job.\n
#         - state : State of job.\n
#         - nodes : Nodes request.\n
#         - remaining : Time remaining in job if running.\n
#         - start_time : Start time of job.
# 
#     Use `--search` and `--match` flags to filter results.
#     """
#     client = _get_client(conn_id)
#     res = client.showq(user=user)
#     str_res = build_table(res, _queue_fields)
#     click.echo(f"SLURM Queue for {user} on {conn_id}:")
#     click.echo(str_res)
# 
# 
# @cli.command()
# @click.option(
#     "-c",
#     "--conn_id",
#     default=None,
#     help="TACC connection id to get allocatiosn for. If none specified, then "
#     + "defaults to first connection from a `taccjm list` command.",
# )
# @click.option(
#     "--search",
#     type=click.Choice(_allocation_fields, case_sensitive=False),
#     help="Column to search.",
#     show_default=True,
#     default="name",
# )
# @click.option(
#     "--match", default=r".", show_default=True, help="Regular expression to match."
# )
# def allocations(conn_id, search, match):
#     """
#     Get TACC Allocations
# 
#     Return TACC allocations on system connected to by conn_id. For each
#     allocation, returns remaining SUs. if no conn_id specified, defaults to
#     first SSH connection in `taccjm list`.
#     """
#     client = _get_client(conn_id)
#     res = client.get_allocations()
#     str_res = build_table(res, _allocation_fields, search, match)
#     click.echo(f"Allocations for {conn_id}:")
#     click.echo(str_res)
