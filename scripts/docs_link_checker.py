import glob
import logging
import os
import re
from typing import List

import click
import requests

logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


class LinkReport:
    def __init__(self, link: str, file: str, message: str):
        self.link = link
        self.file = file
        self.message = message

    def __str__(self):
        return f"{self.message}: File: {self.file}, Link: {self.link}"


class LinkChecker:
    def __init__(
        self, docs_path: str, docs_root: str, site_prefix: str, skip_external: False
    ):
        self._docs_path = docs_path.strip(os.path.sep)
        self._docs_root = docs_root.strip(os.path.sep)
        self._site_prefix = site_prefix.strip("/")
        self._skip_external = skip_external

        markdown_link_regex = r"!?\[(.*?)\]\((.*?)\)"  # inline links, like [Description](link), images start with !
        self._markdown_link_pattern = re.compile(markdown_link_regex)

        external_link_regex = r"^https?:\/\/"  # links that start with http or https
        self._external_link_pattern = re.compile(external_link_regex)

        # links that being with /{site_prefix}/(?P<path>), may end with #abc
        absolute_link_regex = r"^\/" + site_prefix + r"\/(?P<path>[\w\/-]+?)(?:#\S+)?$"
        self._absolute_link_pattern = re.compile(absolute_link_regex)

        # docroot links start without a . or a slash
        docroot_link_regex = r"^(?P<path>\w[\.\w\/-]+\.md)(?:#\S+)?$"
        self._docroot_link_pattern = re.compile(docroot_link_regex)

        # links starting a . or .., file ends with .md, may include an anchor with #abc
        relative_link_regex = r"^(?P<path>\.\.?[\.\w\/-]+\.md)(?:#\S+)?$"
        self._relative_link_pattern = re.compile(relative_link_regex)

        absolute_image_regex = r"^\/" + site_prefix + r"\/(?P<path>[\w\/-]+\.\w{3,4})$"
        self._absolute_image_pattern = re.compile(absolute_image_regex)

        # ending with a 3-4 character suffix
        relative_image_regex = r"^(?P<path>\.\.?[\.\w\/-]+\.\w{3,4})$"
        self._relative_image_pattern = re.compile(relative_image_regex)

    def _is_image_link(self, markdown_link: str):
        return markdown_link.startswith("!")

    def _is_doc_link(self, markdown_link: str):
        return not self._is_image_link(markdown_link)

    def _is_anchor_link(self, link: str):
        return link.startswith("#")

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

    def _get_os_path(self, path: str) -> str:
        """Gets an os-specific path from a path found in a markdown file"""
        return path.replace("/", os.path.sep)

    def _get_absolute_path(self, path: str) -> str:
        return os.path.join(self._docs_root, self._get_os_path(path))

    def _get_relative_path(self, file: str, path: str) -> str:
        # link should be relative to the location of the current file
        directory = os.path.dirname(file)
        return os.path.join(directory, self._get_os_path(path))

    def _get_docroot_path(self, path: str) -> str:
        return os.path.join(self._docs_root, self._get_os_path(path))

    def _check_absolute_link(self, link: str, file: str, path: str) -> LinkReport:
        logger.debug("Checking absolute link %s in file %s", link, file)

        # absolute links should point to files that exist (with the .md extension added)
        md_file = self._get_absolute_path(path).rstrip("/") + ".md"
        logger.debug("Absolute link %s resolved to path %s", link, md_file)

        if not os.path.isfile(md_file):
            logger.info("Absolute link %s in file %s was not found", link, file)
            return LinkReport(link, file, f"Linked file {md_file} does not exist")
        else:
            logger.debug("Absolute link %s in file %s found", link, file)
            return None

    def _check_absolute_image(self, link: str, file: str, path: str) -> LinkReport:
        logger.debug("Cheking absolute image %s in file %s", link, file)

        image_file = self._get_absolute_path(path)
        if not os.path.isfile(image_file):
            logger.info("Absolute image %s in file %s was not found", link, file)
            return LinkReport(link, file, f"Image {image_file} not found")
        else:
            logger.debug("Absolute image %s in file %s found", link, file)
            return None

    def _check_relative_link(self, link: str, file: str, path: str) -> LinkReport:
        logger.debug("Checking relative link %s in file %s", link, file)

        md_file = self._get_relative_path(file, path)
        logger.debug("Relative link %s resolved to path %s", link, md_file)

        if not os.path.isfile(md_file):
            logger.info("Relative link %s in file %s was not found", link, file)
            return LinkReport(link, file, f"Linked file {md_file} does not exist")
        else:
            logger.debug("Relative link %s in file %s found", link, file)
            return None

    def _check_relative_image(self, link: str, file: str, path: str) -> LinkReport:
        logger.debug("Cheking relative image %s in file %s", link, file)

        image_file = self._get_relative_path(file, path)
        if not os.path.isfile(image_file):
            logger.info("Relative image %s in file %s was not found", link, file)
            return LinkReport(link, file, f"Image {image_file} not found")
        else:
            logger.debug("Relative image %s in file %s found", link, file)
            return None

    def _check_docroot_link(self, link: str, file: str, path: str) -> LinkReport:
        logger.debug("Checking docroot link %s in file %s", link, file)

        md_file = self._get_docroot_path(path)
        if not os.path.isfile(md_file):
            logger.info("Docroot link %s in file %s was not found", link, file)
            return LinkReport(link, file, f"Image {image_file} not found")
        else:
            logger.debug("Docroot link %s in file %s found", link, file)
            return None

    def check_link(self, match: re.Match, file: str) -> LinkReport:
        link = match.group(2)

        # skip links that are anchor only (start with #)
        if self._is_anchor_link(link):
            return None

        if self._external_link_pattern.match(link):
            result = self._check_external_link(link, file)
        elif self._is_image_link(match.group(0)):
            match = self._relative_image_pattern.match(link)
            if match:
                result = self._check_relative_image(link, file, match.group("path"))
            else:
                match = self._absolute_image_pattern.match(link)
                if match:
                    result = self._check_absolute_image(link, file, match.group("path"))
                else:
                    result = LinkReport(link, file, "Invalid image link format")
        else:
            match = self._relative_link_pattern.match(link)
            if match:
                result = self._check_relative_link(link, file, match.group("path"))
            else:
                match = self._absolute_link_pattern.match(link)
                if match:
                    result = self._check_absolute_link(link, file, match.group("path"))
                else:
                    match = self._docroot_link_pattern.match(link)
                    if match:
                        result = self._check_docroot_link(
                            link, file, match.group("path")
                        )
                    else:
                        result = LinkReport(link, file, "Invalid link format")

        return result

    def check_file(self, file: str) -> List[LinkReport]:
        with open(file) as f:
            contents = f.read()

        matches = self._markdown_link_pattern.finditer(contents)

        result: List[LinkReport] = []

        for match in matches:
            report = self.check_link(match, file)
            if report is not None:
                result.append(report)

        return result


@click.command()
@click.option("--path", "-p", default=".", help="Path to markdown files to check")
@click.option(
    "--docs-root", "-r", default=None, help="Root to all docs for link checking"
)
@click.option(
    "--site-prefix",
    "-s",
    default="",
    help="Top-most folder in the docs URL for resolving absolute paths",
)
@click.option("--skip-external", is_flag=True)
def scan_docs(path: str, docs_root: str, site_prefix: str, skip_external: bool):
    # verify that our path is correct
    if not os.path.isdir(path):
        print(f"Docs path: {path} is not a directory")
        exit(1)

    if docs_root is None:
        docs_root = path
    elif not os.path.isdir(docs_root):
        print(f"Docs root path: {docs_root} is not a directory")
        exit(1)

    # prepare our return value
    result: List[LinkReport] = list()
    checker = LinkChecker(path, docs_root, site_prefix, skip_external)

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
