import glob
import logging
import os
import re
from typing import List

import click
import requests

logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


class LinkReport:
    def __init__(self, link: str, file: str, message: str):
        self.link = link
        self.file = file
        self.message = message

    def __str__(self):
        return f"{self.message}: File: {self.file}, Link: {self.link}"


class LinkChecker:
    def __init__(self, doc_path: str, site_prefix: str, skip_external: False):
        self._doc_path = doc_path.strip(os.path.sep)
        self._site_prefix = site_prefix.strip("/")
        self._skip_external = skip_external

        markdown_link_regex = (
            r"\[(.*?)\]\((.*?)\)"  # inline links, like [Description](link)
        )
        self._markdown_link_pattern = re.compile(markdown_link_regex)

        external_link_regex = r"^https?:\/\/"  # links that start with http or https
        self._external_link_pattern = re.compile(external_link_regex)

        # links that being with /{site_prefix}/(?P<path>), may end with #abc
        absolute_link_regex = r"^\/" + site_prefix + r"\/(?P<path>.*)(?:#\S+)?"
        self._absolute_link_pattern = re.compile(absolute_link_regex)

        # links starting with . or .., may end with #abc
        relative_link_regex = r"^(?P<path>\.\.?.*\.md)(?:#\S+)?"
        self._relative_link_pattern = re.compile(relative_link_regex)

    def _check_external_link(self, link: str, file: str) -> LinkReport:
        if self._skip_external:
            return None

        logger.debug("Checking external link %s in file %s", link, file)

        try:
            response = requests.get(link)

            if response.status_code >= 400:
                logger.info("External link %s failed in file %s", link, file)
                return LinkReport(
                    link,
                    file,
                    f"External link returned status code: {response.status_code}",
                )
            else:
                logger.debug(
                    "External link %s successful in file %s, response code: %i",
                    link,
                    file,
                    response.status_code,
                )
                return None
        except ConnectionError:
            logger.info(
                "External link %s in file %s raised a connection error", link, file
            )

    def _check_absolute_link(self, link: str, file: str, path: str) -> LinkReport:
        logger.debug("Checking absolute link %s in file %s", link, file)

        # absolute links should point to files that exist (with the .md extension added)
        md_file = os.path.join(self._doc_path, path.rstrip("/")) + ".md"
        logger.debug("Absolute link %s resolved to path %s", link, md_file)

        if not os.path.isfile(md_file):
            logger.info("Absolute link %s in file %s was not found", link, file)
            return LinkReport(link, file, f"Linked file {md_file} does not exist")
        else:
            logger.debug("Absolute link %s in file %s found", link, file)
            return None

    def _check_relative_link(self, link: str, file: str, path: str) -> LinkReport:
        logger.debug("Checking relative link %s in file %s", link, file)

        # link should be relative to the location of the current file
        directory = os.path.dirname(file)
        md_file = os.path.join(self._doc_path, directory, path)
        logger.debug("Relative link %s resolved to path %s", link, md_file)

        if not os.path.isfile(md_file):
            logger.info("Relative link %s in file %s was not found", link, file)
            return LinkReport(link, file, f"Linked file {md_file} does not exist")
        else:
            logger.debug("Relative link %s in file %s found", link, file)
            return None

    def check_link(self, link: str, file: str) -> LinkReport:
        has_match = False

        if self._external_link_pattern.match(link):
            has_match = True
            result = self._check_external_link(link, file)
        else:
            match = self._relative_link_pattern.match(link)
            if match:
                has_match = True
                result = self._check_relative_link(link, file, match.group("path"))
            else:
                match = self._absolute_link_pattern.match(link)
                if match:
                    has_match = True
                    result = self._check_absolute_link(link, file, match.group("path"))

        if not has_match:
            logger.info("Link %s in file %s is an invalid format", link, file)
            result = LinkReport(link, file, "Invalid link format")

        return result

    def check_file(self, file: str) -> List[LinkReport]:
        with open(file) as f:
            contents = f.read()

        matches = self._markdown_link_pattern.findall(contents)

        result: List[LinkReport] = []

        for match in matches:
            _, link = match
            report = self.check_link(link, file)
            if report is not None:
                result.append(report)

        return result


@click.command()
@click.option("--path", "-p", default=".", help="Path to markdown files")
@click.option(
    "--site-prefix", "-s", default="", help="Prefix for resolving absolute paths"
)
@click.option("--skip-external", is_flag=True)
def scan_docs(path: str, site_prefix: str, skip_external: bool):
    # verify that our path is correct
    if not os.path.isdir(path):
        print(f"Docs path: {path} is not a directory")
        exit(1)

    # prepare our return value
    result: List[LinkReport] = list()
    checker = LinkChecker(path, site_prefix, skip_external)

    for file in glob.glob(f"{path}/**/*.md", recursive=True):
        report = checker.check_file(file)
        if report:
            result.extend(report)

    print("----------------------------------------------")
    print("------------- Broken Link Report -------------")
    print("----------------------------------------------")
    for line in result:
        print(line)


def main():
    scan_docs()


if __name__ == "__main__":
    main()
