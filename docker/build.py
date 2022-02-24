# isort:block
import logging
import pathlib
import sys
from typing import List

import click

import docker
from docker import APIClient

PROJECT_ROOT = str(pathlib.Path(__file__).parent.parent.absolute())
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

import versioneer  # isort:skip


logging.basicConfig(
    format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)

PYTHON_DOCKER_TAGS = [
    "3.6-buster",
    "3.7-buster",
]

DOCKER_REPOSITORY = "great-expectations/great_expectations"

client = docker.from_env()
python_docker_tags_opt = click.option(
    "--python-docker-tags",
    "-p",
    multiple=True,
    default=PYTHON_DOCKER_TAGS,
    help="Build the image on top of these python images",
    show_default=True,
)
great_expectations_version_opt = click.option(
    "--ge-version", "-g", default=versioneer.get_version(), show_default=True
)
docker_repository_opt = click.option(
    "--repository",
    "-r",
    default=DOCKER_REPOSITORY,
    help="Target repository for the docker images",
)


def mk_image_tag(python_docker_tag: str, ge_version: str):
    return f"python-{python_docker_tag}-ge-{ge_version}"


@click.group()
def cli():
    pass


@cli.command()
@python_docker_tags_opt
@great_expectations_version_opt
@docker_repository_opt
def build_images(python_docker_tags: List[str], ge_version: str, repository: str):
    for python_docker_tag in python_docker_tags:
        tag = mk_image_tag(python_docker_tag, ge_version)
        image_name = f"{repository}:{tag}"

        logger.info("Building image: " + image_name)

        for line in client.api.build(
            path=".",
            dockerfile="docker/Dockerfile",
            tag=image_name,
            buildargs={"PYTHON_DOCKER_TAG": python_docker_tag},
            rm=True,
            decode=True,
        ):
            stripped = line.get("stream", "").strip()
            if stripped:
                logger.info(stripped)


@cli.command()
@python_docker_tags_opt
@great_expectations_version_opt
@docker_repository_opt
def push_images(python_docker_tags: List[str], ge_version: str, repository: str):
    for python_docker_tag in python_docker_tags:
        tag = mk_image_tag(python_docker_tag, ge_version)

        logger.info(f"Pushing image {repository}:{tag}")

        for line in client.api.push(
            repository=repository, tag=tag, stream=True, decode=True
        ):
            status = line.get("status", "").strip()
            if status:
                progress = line.get("progress", "").strip()
                if progress:
                    logger.info(f"{status}: {progress}")
                else:
                    logger.info(status)

            error_detail = line.get("errorDetail", {}).get("message", "").strip()
            if error_detail:
                logger.error(error_detail)


if __name__ == "__main__":
    cli()
