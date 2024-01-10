from __future__ import annotations
from functools import cached_property
import json
import shutil
from pathlib import Path
import re
from typing import TYPE_CHECKING, Optional
from io import BytesIO
import zipfile

import requests

from docs.prepare_prior_versions import prepare_prior_versions

if TYPE_CHECKING:
    from invoke.context import Context


class DocsBuilder:
    def __init__(
        self,
        context: Context,
        current_directory: Path,
        is_pull_request: bool,
        is_local: bool,
    ) -> None:
        self._context = context
        self._current_directory = current_directory
        self._is_pull_request = is_pull_request
        self._is_local = is_local

    def build_docs(self) -> None:
        """Build API docs + docusaurus docs.
        Currently used in our netlify pipeline.
        """
        self._prepare()
        self.logger.print_header("Building docusaurus docs...")
        self._context.run("yarn build")

    def build_docs_locally(self) -> None:
        """Serv docs locally."""
        self._prepare()
        self.logger.print_header("Running yarn start to serve docs locally...")
        self._context.run("yarn start")

    def _prepare(self) -> None:
        S3_URL = "https://superconductive-public.s3.us-east-2.amazonaws.com/oss_docs_versions_20230615.zip"

        self._run("git pull")

        self.logger.print_header("Preparing to build docs...")
        self.logger.print(f"Copying previous versioned docs from {S3_URL}")
        response = requests.get(S3_URL)
        zip_data = BytesIO(response.content)
        versions: list[str]
        with zipfile.ZipFile(zip_data, "r") as zip_ref:
            versions_json = zip_ref.read("versions.json")
            versions = json.loads(versions_json)
        assert versions
        for version in versions:
            self.logger.print(
                f"Copying code referenced in docs from {version} and writing to versioned_code/version-{version}"
            )
            response = requests.get(
                f"https://github.com/great-expectations/great_expectations/archive/refs/tags/{version}.zip"
            )
            zip_data = BytesIO(response.content)
            with zipfile.ZipFile(zip_data, "r") as zip_ref:
                zip_ref.extractall(self._current_directory / "versioned_code")
                old_location = (
                    self._current_directory
                    / f"versioned_code/great_expectations-{version}"
                )
                new_location = (
                    self._current_directory / f"versioned_code/version-{version}"
                )
                shutil.move(str(old_location), str(new_location))

        self.logger.print_header(
            "Updating versioned code and docs via prepare_prior_versions.py..."
        )
        prepare_prior_versions()
        self.logger.print_header("Updated versioned code and docs")

        if self._is_pull_request:
            self.logger.print_header(
                "Building locally or from within a pull request, using the latest commit to build API docs so changes can be viewed in the Netlify deploy preview."
            )
        else:
            self._run(f"git checkout {self._latest_tag}")
            self._run("git pull")
            self.logger.print_header(
                f"Not in a pull request. Using latest released version {self._latest_tag} at {self._current_commit} to build API docs."
            )

        self.logger.print_header(
            "Building API docs for current version. Please ignore sphinx docstring errors in red/pink, for example: ERROR: Unexpected indentation."
        )
        # TODO: not this
        self._run("(cd ../../; invoke api-docs)")

        if self._is_local:
            self.logger.print_header(
                f"Building locally - Checking back out current branch ({self._current_branch}) before building the rest of the docs."
            )
            self._run(f"git checkout {self._current_branch}")
        else:
            self.logger.print_header(
                f"In a pull request or deploying in netlify (PULL_REQUEST = ${self._is_pull_request}) Checking out ${self._current_commit}."
            )
            self._run(f"git checkout {self._current_branch}")

        self._run("git pull")

    def _run(self, command: str) -> Optional[str]:
        result = self._context.run(command, hide=True)
        if not result:
            return None
        elif not result.ok:
            raise Exception(f"Failed to run command: {command}")
        return result.stdout.strip()

    def _run_and_get_output(self, command: str) -> str:
        output = self._run(command)
        assert output
        return output

    @cached_property
    def _current_commit(self) -> str:
        return self._run_and_get_output("git rev-parse HEAD")

    @cached_property
    def _current_branch(self) -> str:
        return self._run_and_get_output("git rev-parse --abbrev-ref HEAD")

    @cached_property
    def _latest_tag(self) -> str:
        tags_string = self._run("git tag")
        assert tags_string is not None
        tags = [t for t in tags_string.split() if self._tag_regex.match(t)]
        return sorted(tags)[-1]

    @cached_property
    def logger(self) -> Logger:
        return Logger()

    @cached_property
    def _tag_regex(self) -> re.Pattern:
        return re.compile(r"([0-9]+\.)+[0-9]+")


class Logger:
    @staticmethod
    def print_header(string: str) -> None:
        LINE = "================================================================================"
        Logger.print(LINE)
        Logger.print(string)
        Logger.print(LINE)

    @staticmethod
    def print(string: str) -> None:
        ORANGE = "\033[38;5;208m"
        END = "\033[1;37;0m"
        print(ORANGE + string + END)
