"""
TACCJM CLI Utils
"""
import re
from datetime import datetime
from posixpath import basename
from stat import S_ISDIR

from rich.table import Table

import taccjm.tacc_ssh_api as tacc_api
from taccjm.TACCClient import TACCClient

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


def _get_client(conn_id):
    """
    Get first available client if conn_id is None, else client with conn_id.
    """
    if conn_id is None:
        connections = tacc_api.list_sessions()
        if len(connections) == 0:
            raise ValueError("No connections found")
        conn_id = connections[0]["id"]
    client = TACCClient(conn_id=conn_id)
    return client


def _get_files_str(
    client,
    path,
    attrs=["name", "is_dir", "modified_time", "size_bytes"],
    recurse=False,
    hidden=False,
    search="name",
    match=r".",
    job_id=None,
    trash_dir=False,
    fnames=False,
    max_n=100,
):
    """
    Utility function for generating string for listing files in a directory.
    """
    res = client.list_files(
        path=path,
        recurse=recurse,
        hidden=hidden,
        match=match,
        job_id=job_id,
        local=False,
    )

    def _filt_fun(inp):
        """
        Format file dictioanry
        """
        out = {}
        out["name"] = inp["filename"]
        if trash_dir:
            out["name"] = basename(out["name"]).replace("___", "/")
        elif fnames:
            out["name"] = basename(out["name"])

        out["is_dir"] = S_ISDIR(inp["st_mode"])
        out["size_bytes"] = inp["st_size"]
        out["access_time"] = datetime.utcfromtimestamp(inp["st_atime"]).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        out["modified_time"] = datetime.utcfromtimestamp(inp["st_mtime"]).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        out["uid"] = inp["st_uid"]
        out["gid"] = inp["st_gid"]
        return out

    str_res = build_table(res, attrs, search, match, filter_fun=_filt_fun,
                          max_n=max_n)
    return str_res


def build_table(res, fields, search=None, match=r".", filter_fun=None,
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

    table = Table(show_header=True, header_style="bold magenta")
    for field in fields:
        table.add_column(field)

    # Build table from results
    filtered_res = []
    for r in res:
        if filter_fun is not None:
            r = filter_fun(r)
        row = [str(r[f]) for f in fields]
        if search is not None:
            if re.search(match, r[search]) is not None:
                table.add_row(*row)
                filtered_res.append(dict([(f, r[f]) for f in fields]))
        else:
            table.add_row(*row)
            filtered_res.append(dict([(f, r[f]) for f in fields]))

        if len(filtered_res) > max_n:
            break

    return table
