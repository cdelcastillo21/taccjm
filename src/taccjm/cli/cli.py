"""
TACCJM CLI

CLI for using TACCJM Client.
"""
import pdb
import os

from rich.console import Console
from rich.traceback import install
import rich_click as click
from rich_click.cli import patch

patch()

from taccjm.client.tacc_ssh_api import set_host
from taccjm.constants import TACC_SSH_HOST, TACC_SSH_PORT
from taccjm.cli.server import server_commands as server_cli
from taccjm.cli.sessions import session_commands as session_cli
from taccjm.cli.files import file_commands as files_cli
# from taccjm.cli.scripts import script_commands as scripts_cli
# from taccjm.cli.jobs import job_commands as jobs_cli
# from taccjm.cli.shell import shell_commands as shell_cli
# from taccjm.cli.utils import _get_client, build_table

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

install(suppress=[click], max_frames=10, show_locals=True)
CONSOLE = Console()

TACC_PW = os.getenv("TACC_PW")


@click.group(short_help="TACC Job Manager CLI")
@click.option(
    "--server",
    type=(str, int),
    default=(TACC_SSH_HOST, TACC_SSH_PORT),
    help="Host and port of location of TACC JM server to talk to.",
)
@click.option(
    "-c",
    "--cols",
    multiple=True,
    default=None,
    help="Output columns to display.",
    show_default=True,
)
@click.option(
    "-s",
    "--search",
    default=None,
    help="Column to filter results on.",
)
@click.option(
    "-m",
    "--match",
    default=r".",
    show_default=True,
    help="Regular expression to match on result column.",
)
@click.pass_context
def cli(ctx, server=(TACC_SSH_HOST, TACC_SSH_PORT),
        cols=None, search=None, match=r'.'):
    """
    TODO: Add explanation here
    """
    ctx.ensure_object(dict)
    if server[0] != TACC_SSH_HOST or server[1] != TACC_SSH_PORT:
        ctx.obj['host'], ctx.obj['port'] = set_host(server[0], server[1])
    ctx.obj["cols"] = cols
    ctx.obj["search"] = search
    ctx.obj["match"] = match


cli.add_command(server_cli.server)
cli.add_command(session_cli.sessions)
cli.add_command(files_cli.files)
# cli.add_command(scripts_cli.scripts)
# cli.add_command(jobs_cli.jobs)
# cli.add_command(shell_cli.shell)
