# Purpose: Aggregate all contrib packages into a single JSON file to populate the gallery
#
# The generated file is sent to S3 through our CI/CD to be rendered on the front-end.


import json
import logging
import os
from dataclasses import asdict
from typing import List

import pip

from contrib.cli.great_expectations_contrib.commands import (
    read_package_from_file,
    sync_package,
)
from contrib.cli.great_expectations_contrib.package import (
    GreatExpectationsContribPackageManifest,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def gather_all_contrib_package_paths() -> List[str]:
    """Iterate through contrib/ and identify the relative paths to all contrib packages.

    A contrib package is defined by the existence of a .great_expectations_package.json file.

    Returns:
        List of relative paths pointing to contrib packages
    """
    package_paths: List[str] = []
    for root, _, files in os.walk("contrib/"):
        for file in files:
            if file == "package_info.yml":
                package_paths.append(root)

    logger.info(f"Found {len(package_paths)} contrib packages")
    return package_paths


def gather_all_package_manifests(package_paths: List[str]) -> List[dict]:
    """Takes a list of relative paths to contrib packages and collects dictionaries to represent package state.

    Args:
        package_paths: A list of relative paths point to contrib packages

    Returns:
        A list of dictionaries that represents contributor package manifests
    """
    payload: List[dict] = []
    root = os.getcwd()
    for path in package_paths:
        try:
            # Go to package, install deps, read manifest, and sync it
            os.chdir(path)
            _pip_install("-r", "requirements.txt")
            package_path: str = ".great_expectations_package.json"

            package: GreatExpectationsContribPackageManifest = read_package_from_file(
                package_path
            )
            sync_package(package, package_path)

            # Serialize to dict to append to payload
            json_data: dict = asdict(package)
            payload.append(json_data)
            logger.info(
                f"Successfully serialized {package.package_name} to dict and appended to manifest list"
            )
        except Exception as e:
            logger.warning(
                f"Something went wrong when syncing {path} and serializing to dict: {e}"
            )
        finally:
            # Always ensure we revert back to the project root
            os.chdir(root)

    return payload


def _pip_install(*args: str) -> None:
    args_list: List[str] = list(args)
    if hasattr(pip, "main"):
        pip.main(args_list)
    else:
        pip._internal.main(args_list)


def write_results_to_disk(path: str, package_manifests: List[dict]) -> None:
    """Take the list of package manifests and write to JSON file.

    Args:
        path: The relative path to write to
        package_manifest: A list of dictionaries that represents contributor package manifests
    """
    with open(path, "w") as outfile:
        json.dump(package_manifests, outfile, indent=4)
        logger.info(f"Successfully wrote package manifests to {path}")


if __name__ == "__main__":
    pwd = os.path.abspath(os.getcwd())
    root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
    try:
        os.chdir(root)
        package_paths = gather_all_contrib_package_paths()
        payload = gather_all_package_manifests(package_paths)
        write_results_to_disk(os.path.join(pwd, "./package_manifests.json"), payload)
    finally:
        os.chdir(pwd)
