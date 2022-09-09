"""
TACCJM Apps CLI
"""
import pdb
import re
from pathlib import Path

import click
from prettytable import PrettyTable

import taccjm.taccjm_client as tjm
from taccjm.exceptions import TACCJMError
from taccjm.utils import create_template_app, filter_res, format_app_dict, format_job_dict

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

def _get_default():
    jms = tjm.list_jms()
    if len(jms) != 1:
        raise TACCJMError('More than one or no job managers intialized.')
    return jms[0]['jm_id']

@click.group(short_help="list/get/template/deploy")
@click.option("--jm_id", default=None,
              help="Job Manager to execute operation on. Defaults to first available.")
@click.pass_context
def apps(ctx, jm_id):
    """
    TACC Job Manager Apps

    TACCJM Application operations. If no jm_id is specified, then apps are on
    system connected to by first job manager in a `taccjm list` operation.
    """
    ctx.ensure_object(dict)
    ctx.obj['jm_id'] = jm_id

@apps.command(short_help="Create template application",
              context_settings=dict(ignore_unknown_options=True,
                                    allow_extra_args=True))
@click.argument("name", type=str)
@click.option("--dest_dir", type=str, default=".", show_default=True,
              help="Location to put application contents in.")
@click.pass_context
def template(ctx, name, dest_dir):
    """
    Create Application Template

    Creates an appplication template for application with name NAME in the
    current directory. Control destination with the --dest_dir parameter.
    """
    kwargs = dict([(ctx.args[i][2:], ctx.args[i+1]) for i in range(0, len(ctx.args), 2)])
    app, job = create_template_app(name, dest_dir, **kwargs)
    click.echo('Templated App:')
    click.echo(format_app_dict(app))
    click.echo('Example Job Config:')
    click.echo(format_job_dict(job))


@apps.command(short_help="Show deployed apps.")
@click.option("--match", default=r".", show_default=True,
              help="Regular expression to search job ids on.")
@click.pass_context
def list(ctx, match):
    """
    List Applications

    List deployed applications on given job manager. Can search applications
    using the --match option.

    """
    jm_id = ctx.obj['jm_id'] if ctx.obj['jm_id'] is not None else _get_default()
    res = tjm.list_apps(jm_id)
    str_res = filter_res([{
        "name": r
    } for r in res], ["name"],
                          search="name",
                          match=match)
    click.echo(str_res)


@apps.command(short_help="Show application config")
@click.argument("app_id", type=str)
@click.pass_context
def get(ctx, app_id):
    """
    Get Application Configuration

    Get configuration found in app.json file for application APP_ID deployed on
    given TACC Job Manager.
    """
    jm_id = ctx.obj['jm_id'] if ctx.obj['jm_id'] is not None else _get_default()
    res = tjm.list_apps(jm_id)
    app_config = tjm.get_app(jm_id, app_id)
    str_res = format_app_dict(app_config)
    click.echo(str_res)


@apps.command(short_help="Deploy a local application")
@click.argument("app_dir", type=str)
@click.option("--config_file", type=str, default="app.json", show_default=True,
              help="Path, relative to APP_DIR, of application config file.")
@click.option("-o/-no", "--overwrite/--no-overwrite", default=False,
              show_default=True,
              help="Whether to overwrite an existing application with same name if found.")
@click.pass_context
def deploy(ctx, app_dir, config_file, overwrite):
    """
    Deploy Application

    Deploy an application and its contents found in APP_DIR to a TACC system.
    Application contents are assumed to be within an 'assets' folder within
    APP_DIR, with the application config file to be found in an 'app.json' file
    within APP_DIR (by default).
    """
    jm_id = ctx.obj['jm_id'] if ctx.obj['jm_id'] is not None else _get_default()
    app_config = tjm.deploy_app(
        jm_id,
        app_config=None,
        local_app_dir=app_dir,
        app_config_file=config_file,
        overwrite=overwrite
    )
    str_res = format_app_dict(app_config)
    click.echo(f"Application succesfully deployed to {jm_id}:")
    click.echo(str_res)

