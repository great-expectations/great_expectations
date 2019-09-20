import os
import subprocess
import logging

logger = logging.getLogger(__name__)

tag = "v0.8.0__develop"
git_directory = os.environ.get("GE_DEV_DIR")


def get_git_describe_string():
    return subprocess.check_output(["git", "describe"]).decode().strip()


def get_git_revision_hash():
    return subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode()


def get_git_revision_short_hash():
    return subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).decode()


if git_directory is not None:
    start_dir = os.getcwd()

    try:
        os.chdir(git_directory)
        __version__ = get_git_describe_string()
    except subprocess.CalledProcessError:
        logger.warning("Unable to identify version tag using git.")
        __version__ = tag
    finally:
        os.chdir(start_dir)
else:
    logger.debug("Using default version tag.")
    __version__ = tag
