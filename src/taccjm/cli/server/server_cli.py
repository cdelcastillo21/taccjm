"""
TACCJM CLI

CLI for using TACCJM Client.
"""
import pdb
import time
from rich.console import Console
from rich.traceback import install
import rich_click as click
from taccjm.constants import TACC_SSH_HOST, TACC_SSH_PORT
from taccjm.cli.utils import build_table, _server_field_fmts
from taccjm.client.tacc_ssh_api import set_host, find_server

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

install(suppress=[click], max_frames=3)
CONSOLE = Console()


@click.group(short_help="find/log")
@click.pass_context
def server(ctx, cols=None, search=None, match=r'.'):
    """TACC Job Manager Server Operations

    Commands to manage the server that maintains TACC ssh clients.
    """
    ctx.ensure_object(dict)


@server.command(short_help="Locate TACCJM server")
@click.option("-s/-ns", "--start/--no-start",
              is_flag=True, default=False, show_default=True,
              help='If set, will start the server if it is not alive. If ' +
              'kill is set as well, the server will effectively be restarted.')
@click.option("-k/-nk", "--kill/--no-kill",
              is_flag=True, default=False, show_default=True,
              help='If set, will kill the server it is alive. This preceeds' +
              'starting the server.')
@click.pass_context
def find(ctx, kill, start):
    """
    Find Server

    Locate (host,port) where server is running. Can kill/start server as
    needed with appropriate flags. Note that will only show server for the
    default taccjm (host, port) being used. To change (host, port) combo, use
    `taccjm --server <host> <port> find-server` with appropriate host and port.
    """
    res = find_server(start, kill)
    rows = []
    for x in res.keys():
        ts = time.time()
        cmd = []
        if 'create_time' in dir(res[x]):
            ts = res[x].create_time()
        if 'info' in dir(res[x]):
            cmd = res[x].info['cmdline']
        rows.append({
            "name": 'Server' if x == 'server' else 'Heartbeat',
            "pid": res[x].pid,
            "started": ts,
            "cmd": cmd,
        })

    # Default set of columns for this operation
    cols = ["name", "pid", "started"]
    if ctx.obj['cols'] != ():
        cols = ctx.obj['cols']
    str_res = build_table(
            rows,
            fields=ctx.obj['cols'],
            fmts=_server_field_fmts,
            search=ctx.obj['search'], match=ctx.obj['match'])

    CONSOLE.print(str_res)
