"""
TACCJM Files CLI
"""
from pathlib import Path

import click

from taccjm.cli.utils import _file_field_names, _get_client, _get_files_str
from taccjm.utils import filter_res

from rich.console import Console
CONSOLE = Console()

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"


@click.group(short_help="list/peak/download/upload/read/write/rm/trash")
@click.option(
    "-c",
    "--conn_id",
    default=None,
    help="Connection to execute operation on. Defaults to first available.",
)
@click.option(
    "-j",
    "--job_id",
    type=str,
    default=None,
    help="If specified, remote paths is assumed to be relative to to job "
    + "directory with given job_id.",
)
@click.pass_context
def files(ctx, conn_id, job_id):
    """
    TACC Job Manager Files

    File operations. If no jm_id is specified, then paths are on system
    connected to by first job manager in a `taccjm list` operation. Remote
    paths, if not absolute, are releative to home directory on TACC system.
    If job_id is specified, then remote paths are relative to job directory.
    """
    client = _get_client(conn_id)
    ctx.ensure_object(dict)
    ctx.obj["job_id"] = job_id
    ctx.obj["client"] = client


@files.command(short_help="List files.")
@click.option("-p", "--path", type=str, default=".", help="Path to list files.")
@click.option(
    "--attrs",
    type=click.Choice(_file_field_names, case_sensitive=False),
    multiple=True,
    default=["name", "is_dir", "size_bytes", "modified_time"],
    help="File attributes to include in output.",
    show_default=True,
)
@click.option(
    "-r/-nr", "--recurse/--no-recurse", default=True, help="Get dir contents or not."
)
@click.option(
    "-h/-nh", "--hidden/--no-hidden", default=False, help="Include hidden output flag."
)
@click.option(
    "--search",
    type=click.Choice(_file_field_names, case_sensitive=False),
    default="name",
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
@click.pass_context
def list(ctx, path, attrs, recurse, hidden, search, match):
    """
    List Files

    List files in a given directory (defaults to home). Can search using
    regular expressions on any given output attribute.
    """
    client = ctx.obj["client"]
    table = _get_files_str(
        client,
        path,
        attrs=attrs,
        recurse=recurse,
        hidden=hidden,
        search=search,
        match=match,
        job_id=ctx.obj["job_id"],
    )
    table.title = ("[not italic magenta bold]:desktop_computer:" +
                   f"  {client.id}  :open_file_folder: {path}[/]")
    CONSOLE.print(table)


@files.command(short_help="Show head/tail of file")
@click.argument("path")
@click.option(
    "-h",
    "--head",
    type=int,
    default=10,
    help="If specified, number of lines from top of file to show.",
)
@click.option(
    "-t",
    "--tail",
    type=int,
    default=-1,
    help="If specified, number of lines from bottom of file to show.",
)
@click.pass_context
def peak(ctx, path, head, tail):
    """
    Peak File

    "Peak" at file via head/tail commands. Defaults to head command (first 10
    lines) if no options specified. Otherwise defaults to head number if head
    specified, and tail number otherwise (if specified). File is assumed to be
    text file. Output of operation is returned.
    """
    client = ctx.obj["client"]
    pre = None
    job_id = ctx.obj["job_id"]
    if job_id is not None:
        path = client.job_path(job_id, path)
    else:
        path = client.abspath(path)
    if head > 0:
        res = client.exec(f"head -n {head} {path}")
        pre = f"First {head}"
    if tail > 0:
        pre = f"Last {tail}" if pre is None else f"{pre} & Last {tail}"
        res = client.exec(f"tail -n {tail} {path}")
    click.echo(f"{pre} line(s) of {path} on {client.id} :")
    click.echo(res["stdout"])


@files.command(short_help="Send a local file or directory.")
@click.argument("local_path")
@click.argument("remote_path")
@click.option(
    "--file_filter",
    type=str,
    default="*",
    help="If LOCAL_PATH specifies a directory, glob string to filter files on.",
)
@click.pass_context
def upload(ctx, local_path, remote_path, file_filter):
    """
    Upload File or Directory

    Upload a local file or a directory at LOCAL_PATH to REMOTE_PATH on TACC
    system. Note that this is meant for relatively small (<100Mb) uploads. If a
    directory is specified, the contents are compressed first into a sinlge file
    before being sent, and then de-compressed on the target system. Note this
    means that if the upload fails for some reason, some cleanup may be required
    on the local/remote system (feature to check for/cleanup automatically is
    coming...).

    """
    client = ctx.obj["client"]
    job_id = ctx.obj["job_id"]
    client.upload(local_path, remote_path, job_id=job_id, file_filter=file_filter)
    click.echo(f"{local_path} uploaded to {remote_path} on {client.id}:")
    str_res = _get_files_str(
        client,
        remote_path,
        recurse=True,
        hidden=True,
        job_id=job_id,
    )
    click.echo(str_res)


@files.command(short_help="Get a remote file or directory")
@click.argument("remote_path")
@click.argument("local_path")
@click.option(
    "--file_filter",
    type=str,
    default="*",
    help="If REMOTE_PATH specifies a directory, glob string to filter files on.",
)
@click.pass_context
def download(ctx, remote_path, local_path, file_filter):
    """
    Download File or Directory

    Download a remote file or directory at REMOTE_PATH on TACC system to the
    specified LOCAL_PATH. Note that this is meant for relatively small (<100Mb)
    downloads. If a job_id was specified for this file operation, contents will
    always be downloaded into a folder with the name of the job_id. Furthermore
    REMOTE_PATH is assumed to be relative to a user's home directory, unless
    job_id is specified, in which case the path is assumed to be relative to the
    job directory. If a directory is specified, contents are compressed before
    downloaded, and then unpacked locally. Note this means that if download
    fails for some reason, some cleanup may be required (feature to check
    for/cleanup automatically coming...)
    """
    client = ctx.obj["client"]
    job_id = ctx.obj["job_id"]
    res = client.download(
        remote_path, local_path, job_id=job_id, file_filter=file_filter
    )
    click.echo(f"{remote_path} downloaded to {res} on {client.id}:")


@files.command(short_help="Send a remote file/directory to trash.")
@click.argument("remote_path")
@click.option(
    "-r/-nr",
    "--restore/--no-restore",
    is_flag=True,
    default=False,
    help="Set to true to restore a previously removed file.",
)
@click.option(
    "-w/-nw",
    "--wait/--no-wait",
    is_flag=True,
    default=True,
    help="Wait for remove command to finish or not",
)
@click.pass_context
def rm(ctx, remote_path, restore, wait):
    """
    Remove file/directory

    Remove file or directory at REMOTE_PATH by sending it to the trash directory
    managed by job manager. Note file can be restored with the restore command.
    """
    client = ctx.obj["client"]
    res = client.rm(remote_path, restore)
    action = "Removal" if not restore else "Restoring"
    msg = f"{action} of {remote_path} on {client.id} starting."
    if wait:
        msg += " Waiting for it to complete..."
        click.echo(msg)
        client.process(res["id"], wait=True)
        click.echo("Done!")
    else:
        msg += (
            " Performing in background. use `taccjm process [id] -w`"
            + " to wait for completion."
        )
        click.echo(msg)
        click.echo(filter_res([res], ["id", "status", "ts"]))


@files.command(short_help="Send a remote file/directory to trash.")
@click.pass_context
@click.option(
    "--attrs",
    type=click.Choice(_file_field_names, case_sensitive=False),
    multiple=True,
    default=["name", "is_dir", "size_bytes", "modified_time"],
    help="File attributes to include in output.",
    show_default=True,
)
@click.option(
    "--search",
    type=click.Choice(_file_field_names, case_sensitive=False),
    default="name",
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
def trash(ctx, attrs, search, match):
    """
    View Trash

    View contents of trash directory.
    """
    client = ctx.obj["client"]
    str_res = _get_files_str(
        client,
        client.trash_dir,
        attrs=attrs,
        recurse=True,
        hidden=False,
        search=search,
        match=match,
        trash_dir=True,
    )
    click.echo(f"Trash dir for {client.id}:")
    click.echo(str_res)


@files.command(short_help="Stream data directly to a remote file.")
@click.argument("remote_path", type=str)
@click.argument("data", type=str)
@click.pass_context
def write(ctx, data, remote_path):
    """
    Write Data

    Write string DATA directly to a file on a remote system. WARNING: Will
    overwrite existing file. REMOTE_PATH is assumed to be relative to a user's
    home directory, unless job_id is specified, in which case the path is
    assumed to be relative to the job directory.
    """
    client = ctx.obj["client"]
    job_id = ctx.obj["job_id"]
    client.write(data, remote_path, job_id)

    remote_path = (
        client.abspath(remote_path)
        if job_id is None
        else client.job_path(job_id, remote_path)
    )
    str_res = _get_files_str(
        client,
        str(Path(remote_path).parent),
        search="name",
        recurse=True,
        match=str(Path(remote_path).name),
    )
    click.echo(f"Succesfully wrote file {remote_path}")
    click.echo(str_res)


@files.command(short_help="Stream data directly from a remote file.")
@click.argument("remote_path", type=str)
@click.pass_context
def read(ctx, remote_path):
    """
    Read Data

    Read data directly from REMOTE_PATH to stdout. REMOTE_PATH is assumed to be
    relative to a user's home directory, unless job_id is specified, in which
    case the path is assumed to be relative to the job directory.
    """
    client = ctx.obj["client"]
    job_id = ctx.obj["job_id"]
    res = client.read(remote_path, job_id)
    click.echo(f"File {remote_path} contents:")
    click.echo(res)
