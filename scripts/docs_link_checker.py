#!/usr/bin/env python3
"""A command-line tool used to check links in docusaurus markdown documentation

To check all of our markdown documentation, run:
python docs_link_checker.py -p docs -r docs -s docs --skip-external

The above command:
    - -p docs (also --path): The path to the markdown files you want to check. For example, if you wanted to check only the tutorial files, you could specify docs/tutorials
    - -r docs (also --docs-root): The root of the docs folder, used to resolve absolute and docroot paths
    - -s docs (also --site-prefix): The site path prefix, used to resolve abosulte paths (ex: in http://blah/docs, it is the docs part)
    - --skip-external: If present, external (http) links are not checked
"""

import glob
import logging
import os
import re
from typing import List, Optional

import click
import requests

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


class LinkReport:
    """Used to capture the details of a broken link

    Attributes:
        link: The link that is broken.
        file: The file in which the link is found.
        message: A message describing the failure.
    """

    def __init__(self, link: str, file: str, message: str):
        self.link = link
        self.file = file
        self.message = message

    def __str__(self):
        return f"{self.message}: File: {self.file}, Link: {self.link}"


class LinkChecker:
    """Checks image and file links in a set of markdown files."""

    def __init__(
        self,
        docs_path: str,
        docs_root: str,
        site_prefix: str,
        skip_external: bool = False,
    ):
        """Initializes LinkChecker

        Args:
            docs_path: The directory of markdown (.md) files whose links you want to check
            docs_root: The root directory, used to resolve absolute and docroot paths
            site_prefix: The top-level folder (ex: /docs) used to resolve absolute links to local files
            skip_external: Whether or not to skip checking external (http..) links
        """
        self._docs_path = docs_path.strip(os.path.sep)
        self._docs_root = docs_root.strip(os.path.sep)
        self._site_prefix = site_prefix.strip("/")
        self._skip_external = skip_external

        markdown_link_regex = r"!?\[(.*)\]\((.*?)\)"  # inline links, like [Description](link), images start with !
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

    def _is_image_link(self, markdown_link: str) -> bool:
        return markdown_link.startswith("!")

    def _is_doc_link(self, markdown_link: str) -> bool:
        return not self._is_image_link(markdown_link)

    def _is_anchor_link(self, link: str) -> bool:
        return link.startswith("#")

    def _check_external_link(self, link: str, file: str) -> Optional[LinkReport]:
        if self._skip_external:
            return None

        logger.debug(f"Checking external link {link} in file {file}", link, file)

        try:
            response = requests.get(link)

            if 400 <= response.status_code < 500:
                logger.info(
                    f"External link {link} failed in file {file} with code {response.status_code}"
                )
                return LinkReport(
                    link,
                    file,
                    f"External link returned status code: {response.status_code}",
                )
            else:
                logger.debug(
                    f"External link {link} successful in file {file}, response code: {response.status_code}",
                )
                return None
        except requests.exceptions.ConnectionError as err:
            logger.info(
                f"External link {link} in file {file} raised a connection error"
            )
            return LinkReport(
                link, file, f"External link raised a connection error {err.errno}"
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

    def _check_absolute_link(
        self, link: str, file: str, path: str
    ) -> Optional[LinkReport]:
        logger.debug(f"Checking absolute link {link} in file {file}")

        # absolute links should point to files that exist (with the .md extension added)
        md_file = self._get_absolute_path(path).rstrip("/") + ".md"
        logger.debug(f"Absolute link {link} resolved to path {md_file}")

        if not os.path.isfile(md_file):
            logger.info(f"Absolute link {link} in file {file} was not found")
            return LinkReport(link, file, f"Linked file {md_file} not found")
        else:
            logger.debug(f"Absolute link {link} in file {file} found")
            return None

    def _check_absolute_image(
        self, link: str, file: str, path: str
    ) -> Optional[LinkReport]:
        logger.debug(f"Checking absolute image {link} in file {file}")

        image_file = self._get_absolute_path(path)
        if not os.path.isfile(image_file):
            logger.info(f"Absolute image {link} in file {file} was not found")
            return LinkReport(link, file, f"Image {image_file} not found")
        else:
            logger.debug(f"Absolute image {link} in file {file} found")
            return None

    def _check_relative_link(
        self, link: str, file: str, path: str
    ) -> Optional[LinkReport]:
        logger.debug(f"Checking relative link {link} in file {file}")

        md_file = self._get_relative_path(file, path)
        logger.debug(f"Relative link {link} resolved to path {md_file}")

        if not os.path.isfile(md_file):
            logger.info(f"Relative link {link} in file {file} was not found")
            return LinkReport(link, file, f"Linked file {md_file} not found")
        else:
            logger.debug(f"Relative link {link} in file{file} found")
            return None

    def _check_relative_image(
        self, link: str, file: str, path: str
    ) -> Optional[LinkReport]:
        logger.debug(f"Checking relative image {link} in file {file}")

        image_file = self._get_relative_path(file, path)
        if not os.path.isfile(image_file):
            logger.info(f"Relative image {link} in file {file} was not found")
            return LinkReport(link, file, f"Image {image_file} not found")
        else:
            logger.debug(f"Relative image {link} in file {file} found")
            return None

    def _check_docroot_link(
        self, link: str, file: str, path: str
    ) -> Optional[LinkReport]:
        logger.debug(f"Checking docroot link {link} in file {file}")

        md_file = self._get_docroot_path(path)
        if not os.path.isfile(md_file):
            logger.info(f"Docroot link {link} in file {file} was not found")
            return LinkReport(link, file, f"Linked file {md_file} not found")
        else:
            logger.debug(f"Docroot link {link} in file {file} found")
            return None

    def _check_link(self, match: re.Match, file: str) -> Optional[LinkReport]:
        """Checks that a link is valid. Valid links are:
        - Absolute links that begin with a forward slash and the specified site prefix (ex: /docs) with no suffix
        - Absolute images with an image suffix
        - Relative links that begin with either . or .. and have a .md suffix
        - Relative images with an image suffix
        - Docroot links that begin with a character (neither . or /) are relative to the doc root (ex: /docs) and have a .md suffix

        Args:
            match: A positive match of a markdown link (ex: [...](...)) or image
            file: The file where the match was found

        Returns:
            A LinkReport if the link is broken, otherwise None
        """
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
        """Looks for all the links in a file and checks them.

        Returns:
            A list of broken links, or an empty list if no links are broken
        """
        with open(file) as f:
            contents = f.read()

        matches = self._markdown_link_pattern.finditer(contents)

        result: List[LinkReport] = []

        for match in matches:
            report = self._check_link(match, file)

            if report:
                result.append(report)

            # sometimes the description may contain a reference to an image
            nested_match = self._markdown_link_pattern.match(match.group(1))
            if nested_match:
                report = self._check_link(nested_match, file)

                if report:
                    result.append(report)

        return result


@click.command(help="Checks links and images in Docusaurus markdown files")
@click.option(
    "--path",
    "-p",
    type=click.Path(exists=True, file_okay=True),
    default=".",
    help="Path to markdown file(s) to check",
)
@click.option(
    "--docs-root",
    "-r",
    type=click.Path(exists=True, file_okay=False),
    default=None,
    help="Root to all docs for link checking",
)
@click.option(
    "--site-prefix",
    "-s",
    default=None,
    help="Top-most folder in the docs URL for resolving absolute paths",
)
@click.option("--skip-external", is_flag=True)
def scan_docs(
    path: str, docs_root: Optional[str], site_prefix: str, skip_external: bool
) -> None:
    if docs_root is None:
        docs_root = path
    elif not os.path.isdir(docs_root):
        click.echo(f"Docs root path: {docs_root} is not a directory")
        exit(1)

    # prepare our return value
    result: List[LinkReport] = list()
    checker = LinkChecker(path, docs_root, site_prefix, skip_external)

    if os.path.isdir(path):
        # if the path is a directory, get all .md files within it
        for file in glob.glob(f"{path}/**/*.md", recursive=True):
            report = checker.check_file(file)
            if report:
                result.extend(report)
    elif os.path.isfile(path):
        # else we support checking one file at a time
        result.extend(checker.check_file(path))
    else:
        click.echo(f"Docs path: {path} is not a directory or file")
        exit(1)

    if result:
        click.echo("----------------------------------------------")
        click.echo("------------- Broken Link Report -------------")
        click.echo("----------------------------------------------")
        for line in result:
            click.echo(line)

        exit(1)
    else:
        click.echo("No broken links found")
        exit(0)


def main():
    scan_docs()


if __name__ == "__main__":
    main()
