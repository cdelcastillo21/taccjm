"""
TACCJM Files CLI
"""
import pdb
import re
from stat import S_ISDIR
from pathlib import Path

import click
from prettytable import PrettyTable
from datetime import datetime

import taccjm.taccjm_client as tjm
from taccjm.utils import filter_res

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

# Available return fields per command, and their names in CLI table
_file_fields = [
    "filename",
    "st_atime",
    "st_gid",
    "st_mode",
    "st_mtime",
    "st_size",
    "st_uid",
]
_file_field_names = [
    "name",
    "is_dir",
    "size_bytes",
    "access_time",
    "modified_time",
    "uid",
    "gid",
]


def _get_files_str(
    jm_id,
    path,
    attrs=["name", "is_dir", "modified_time", "size_bytes"],
    hidden=False,
    search="name",
    match=r".",
    job_id=None,
):
    """
    Utility function for generating string for listing files in a directory.
    """
    if job_id is not None:
        res = tjm.list_job_files(jm_id, job_id, path, attrs=_file_fields, hidden=hidden)
    else:
        res = tjm.list_files(jm_id, path, attrs=_file_fields, hidden=hidden)

    def _filt_fun(x):
        o = {}
        o["name"] = x["filename"]
        o["is_dir"] = True if S_ISDIR(x["st_mode"]) else False
        o["size_bytes"] = x["st_size"]
        o["access_time"] = datetime.utcfromtimestamp(x["st_atime"]).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        o["modified_time"] = datetime.utcfromtimestamp(x["st_mtime"]).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        o["uid"] = x["st_uid"]
        o["gid"] = x["st_gid"]
        return o

    str_res = filter_res(res, attrs, search, match, filter_fun=_filt_fun)
    return str_res


@click.group(short_help="list/peak/download/upload/read/write/remove/restore")
@click.option(
    "-j",
    "--jm_id",
    default=None,
    help="Job Manager to execute operation on. Defaults to first available.",
)
@click.option(
    "-i",
    "--job_id",
    type=str,
    default=None,
    help="If specified, remote paths are taken to be relative to job directory with given id.",
)
@click.pass_context
def files(ctx, jm_id, job_id):
    """
    TACC Job Manager Files

    File operations. If no jm_id is specified, then paths are on system
    connected to by first job manager in a `taccjm list` operation. Remote
    paths, if not absolute, are releative to home directory on TACC system.
    If job_id is specified, then remote paths are relative to job directory.
    """
    if jm_id is None:
        jms = tjm.list_jms()
        if len(jms) == 0:
            raise TACCJMError(
                "No JM specified (--jm_id) and no JMs already initialized."
            )
        jm_id = tjm.list_jms()[0]["jm_id"]
    ctx.ensure_object(dict)
    ctx.obj["job_id"] = job_id
    ctx.obj["jm_id"] = jm_id


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
def list(ctx, path, attrs, hidden, search, match):
    """
    List Files

    List files in a given directory (defaults to home). Can search using
    regular expressions on any given output attribute.
    """
    jm_id = ctx.obj["jm_id"]
    str_res = _get_files_str(
        jm_id,
        path,
        attrs=attrs,
        hidden=hidden,
        search=search,
        match=match,
        job_id=ctx.obj["job_id"],
    )
    click.echo(f"Files on {jm_id} at {path} :")
    click.echo(str_res)


@files.command(short_help="Show head/tail of file")
@click.argument("path")
@click.option(
    "-h",
    "--head",
    type=int,
    default=-1,
    help="If specified, number of lines from top of file to show.",
)
@click.option(
    "-t",
    "--tail",
    type=int,
    default=-1,
    help="If specified, number of lines from bottom of file to show. Note: if head also specified, tail ignored.",
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
    jm_id = ctx.obj["jm_id"]
    job_id = ctx.obj["job_id"]
    if job_id is None:
        res = tjm.peak_file(jm_id, path, head, tail)
    else:
        res = tjm.peak_job_file(jm_id, job_id, path, head, tail)
    pre = f"Last {tail}" if tail > 0 else f"First {head if head > 0 else 10}"
    click.echo(f"{pre} line(s) of {path} on {jm_id} :")
    click.echo(res)


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
    jm_id = ctx.obj["jm_id"]
    job_id = ctx.obj["job_id"]
    if job_id is None:
        res = tjm.upload(jm_id, local_path, remote_path, file_filter)
        str_res = _get_files_str(
            jm_id, str(Path(remote_path).parent), match=str(Path(remote_path).name)
        )
    else:
        res = tjm.upload_job_file(jm_id, job_id, local_path, remote_path, file_filter)
        str_res = _get_files_str(
            jm_id, "", match=str(Path(remote_path).name), job_id=job_id
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
    jm_id = ctx.obj["jm_id"]
    job_id = ctx.obj["job_id"]
    if job_id is None:
        res = tjm.download(jm_id, remote_path, local_path, file_filter)
    else:
        res = tjm.download_job_file(jm_id, job_id, remote_path, local_path, file_filter)


@files.command(short_help="Send a remote file/directory to trash.")
@click.argument("remote_path")
@click.pass_context
def remove(ctx, remote_path):
    """
    Remove file/directory

    Remove file or directory at REMOTE_PATH by sending it to the trash directory
    managed by job manager. Note file can be restored with the restore command.
    """
    jm_id = ctx.obj["jm_id"]
    res = tjm.remove(jm_id, remote_path)
    str_res = _get_files_str(
        jm_id, str(Path(remote_path).parent), match=str(Path(remote_path).name)
    )
    click.echo(str_res)


@files.command(short_help="Restore a remote file/directory from trash.")
@click.argument("remote_path")
@click.pass_context
def restore(ctx, remote_path):
    """
    Restore file/directory

    Restore a file or directory at REMOTE_PATH by moving from the trash
    directory managed by job manager back to its original location.
    """
    jm_id = ctx.obj["jm_id"]
    res = tjm.restore(jm_id, remote_path)
    str_res = _get_files_str(
        jm_id, str(Path(remote_path).parent), match=str(Path(remote_path).name)
    )
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
    jm_id = ctx.obj["jm_id"]
    job_id = ctx.obj["job_id"]
    if job_id is None:
        res = tjm.write(jm_id, data, remote_path)
        str_res = _get_files_str(
            jm_id, str(Path(remote_path).parent), match=str(Path(remote_path).name)
        )
    else:
        res = tjm.write_job_file(jm_id, job_id, data, remote_path)
        str_res = _get_files_str(
            jm_id,
            str(Path(remote_path).parent),
            match=str(Path(remote_path).name),
            job_id=job_id,
        )
    click.echo(str_res)


@files.command(short_help="Stream data directly from a remote file.")
@click.argument("remote_path", type=str)
@click.option(
    "--data_type",
    type=click.Choice(["text", "json"]),
    default="text",
    help="Type of data assumed to be in remote file.",
    show_default=True,
)
@click.pass_context
def read(ctx, remote_path, data_type):
    """
    Read Data

    Read data directly from REMOTE_PATH to stdout. REMOTE_PATH is assumed to be
    relative to a user's home directory, unless job_id is specified, in which
    case the path is assumed to be relative to the job directory.
    """
    jm_id = ctx.obj["jm_id"]
    job_id = ctx.obj["job_id"]
    if job_id is None:
        res = tjm.read(jm_id, remote_path, data_type)
    else:
        res = tjm.read_job_file(jm_id, job_id, remote_path, data_type)
    click.echo(res)
