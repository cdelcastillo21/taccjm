"""
SLURM Task Queue Utilities

Utility functions for :class:`SLURMTaskQueue` and supporting classes.
"""
from prettytable import PrettyTable
import logging
import re

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

def expand_int_list(s):
    """Expands int lists with ranges."""
    r = []
    for i in s.split(","):
        if "-" not in i:
            r.append(int(i))
        else:
            l, h = map(int, i.split("-"))
            r += range(l, h + 1)
    return r


def compact_int_list(i, delim=","):
    """Compacts int lists with ranges."""
    if len(i) == 0:
        return ""
    elif len(i) == 1:
        return f"{i[0]}"
    for e in range(1, len(i)):
        if i[e] != i[0] + e:
            return f"{i[0]}-{i[e-1]}{delim}{compact_int_list(i[e:])}"
    return f"{i[0]}-{i[-1]}"

def filter_res(res, fields, search=None, match=r'.', print_res=False,
        output_file=None):
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
    x = PrettyTable(float_format='0.2')
    x.field_names = fields

    # Build table from results
    filtered_res = []
    for r in res:
        if search is not None:
            if re.search(match, r[search]) is not None:
                x.add_row([r[f] for f in fields])
                filtered_res.append(dict([(f, r[f]) for f in fields]))
        else:
            x.add_row([r[f] for f in fields])
            filtered_res.append(dict([(f, r[f]) for f in fields]))

    # Print Table
    if print_res:
        print(x)

    if output_file is not None:
        with open(output_file, 'w') as fp:
            fp.write(str(x))

    return filtered_res
