import docker
import logging
import click
from typing import List

import sys
import pathlib

PROJECT_ROOT = str(pathlib.Path(__file__).parent.parent.absolute())
try:
    sys.path.index(PROJECT_ROOT)
except ValueError:
    sys.path.append(PROJECT_ROOT)

import versioneer


logging.basicConfig(
    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
    level=logging.INFO
)

PYTHON_DOCKER_TAGS = [
    "3.6-buster",
    "3.7-buster",
]

DOCKER_REPOSITORY = "great-expectations"

client = docker.from_env()
python_docker_tags_opt = click.option("--python-docker-tags", "-p", multiple=True, default=PYTHON_DOCKER_TAGS, help="Build the image on top of these python images", show_default=True)
great_expectations_version_opt = click.option("--ge-version", "-g", default=versioneer.get_version(), show_default=True)


def mk_image_tag(python_docker_tag: str, ge_version: str):
    return f"great_expectations:python-{python_docker_tag}_ge-{ge_version}"


@click.group()
def cli():
    pass


@cli.command()
@python_docker_tags_opt
@great_expectations_version_opt
def build_images(python_docker_tags: List[str], ge_version: str):
    for python_docker_tag in python_docker_tags:
        tag = mk_image_tag(python_docker_tag, ge_version)

        logging.info("Building image with tag: " + tag)

        client.images.build(
            path="docker/",
            tag=tag,
            buildargs={
                "PYTHON_DOCKER_TAG": python_docker_tag
            }
        )


@cli.command()
@python_docker_tags_opt
@great_expectations_version_opt
@click.option("--repository", "-r", default=DOCKER_REPOSITORY, help="Target repository for the docker images")
def push_images(python_docker_tags: List[str], ge_version: str, repository: str):
    for python_docker_tag in python_docker_tags:
        tag = mk_image_tag(python_docker_tag, ge_version)

        logging.info(f"Pushing image {repository}/{tag}")


if __name__ == '__main__':
    cli()
