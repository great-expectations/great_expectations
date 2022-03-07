import click
from great_expectations.core.usage_statistics.util import send_usage_message
from great_expectations.cli.python_subprocess import execute_shell_command
from subprocess import run

@click.group()
@click.pass_context
def docker(ctx):
    """Container operations"""
    ctx.obj.data_context = ctx.obj.get_data_context_from_config_file()

    usage_stats_prefix = f"cli.store.{ctx.invoked_subcommand}"
    send_usage_message(
        data_context=ctx.obj.data_context,
        event=f"{usage_stats_prefix}.begin",
        success=True,
    )
    ctx.obj.usage_event_end = f"{usage_stats_prefix}.end"


@docker.command(name="run")
@click.pass_context
def docker_run(ctx):
    context = ctx.obj.data_context
    data_doc_port = 5055
    jupyter_server_port = 5050
    image = "ilriccio/great_expectations:dev_loop"
    root_dir = context.root_directory
    jupyter_port_1 = 5051
    jupyter_port_2 = 5052
    jupyter_port_3 = 5053
    jupyter_port_4 = 5054
    ports = [
        "-p", f"{jupyter_server_port}:5050",  # primary jupyter server
        "-p", f"{data_doc_port}:5055",  # data docs
        "-p", f"{jupyter_port_1}:5051",  # ports for opening up additional notebooks
        "-p", f"{jupyter_port_2}:5052",
        "-p", f"{jupyter_port_3}:5053",
        "-p", f"{jupyter_port_4}:5054"
    ]
    cmd = [
        "docker", "run", "-ti", "--rm", "--name", "great_expectations",
        *ports,
        "-v", f"{root_dir}:/ge/great_expectations",
        f"{image}"
    ]
    try:
        print(docker_welcome_msg)
        res = run(cmd)
    except Exception as e:
        print(e)


docker_welcome_msg = """

  ___              _     ___                  _        _   _
 / __|_ _ ___ __ _| |_  | __|_ ___ __  ___ __| |_ __ _| |_(_)___ _ _  ___
| (_ | '_/ -_) _` |  _| | _|\ \ / '_ \/ -_) _|  _/ _` |  _| / _ \ ' \(_-<
 \___|_| \___\__,_|\__| |___/_\_\ .__/\___\__|\__\__,_|\__|_\___/_||_/__/
                                |_|
             ~ Always know what to expect from your data ~
             
Welcome to the Great Expectations Dev-Loop container! Designed to get your
project up and running as quickly as possible, this container ships with
all Great Expectations dependencies pre-installed.

Access your project via a Jupyter server at port 5050 (random password generated below).
Access DataDocs at port 5055.
Access the CLI by obtaining the container ID ($ docker ps) and running:
    $ docker exec -it <container_id> bash

As a convenience, inside the container `great_expectations` is aliased to `ge`.
______________________________________________________________________________

"""