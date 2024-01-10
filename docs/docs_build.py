from __future__ import annotations
from functools import cached_property
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from invoke.context import Context


class DocsBuilder:
    def __init__(self, context: Context) -> None:
        self._context = context

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
        self.logger.print_header("Preparing to build docs...")

        S3_URL = "https://superconductive-public.s3.us-east-2.amazonaws.com/oss_docs_versions_20230615.zip"
        CURRENT_COMMIT = self._run("git rev-parse HEAD")
        CURRENT_BRANCH = self._run("git rev-parse --abbrev-ref HEAD")
        self._run("git pull")
        breakpoint()
        # git info

        # pull versions .zip
        #

    def _run(self, command: str) -> Optional[str]:
        result = self._context.run(command, hide=True)
        if not result:
            return None
        elif not result.ok:
            raise Exception(f"Failed to run command: {command}")
        return result.stdout.strip()

    @cached_property
    def logger(self) -> Logger:
        return Logger()


class Logger:
    @staticmethod
    def print_header(string: str) -> None:
        LINE = "================================================================================"
        Logger.print_orange(LINE)
        Logger.print_orange(string)
        Logger.print_orange(LINE)

    @staticmethod
    def print_orange(string: str) -> None:
        ORANGE = "\033[38;5;208m"
        END = "\033[1;37;0m"
        print(ORANGE + string + END)
