"""Prepare prior docs versions of GX for inclusion into the latest docs under the version dropdown.

There are changes to paths that need to be made to prior versions of docs.
"""
from __future__ import annotations
from dataclasses import dataclass
from functools import total_ordering

import glob
import pathlib
import re

from typing_extensions import override


def _docs_dir() -> pathlib.Path:
    """Base directory for docs (contains docusaurus folder)."""
    return pathlib.Path().absolute()


def _path_to_versioned_docs() -> pathlib.Path:
    return _docs_dir() / "docusaurus/versioned_docs"


def _path_to_versioned_code() -> pathlib.Path:
    return _docs_dir() / "docusaurus/versioned_code"


def version_dir_name(version: Version) -> str:
    return f"version-{version}"


def change_paths_for_docs_file_references(
    version: Version, verbose: bool = False
) -> None:
    """Change file= style references to use versioned_docs paths.

    This is used in v0.14 docs like v0.14.13 since we moved to using named
    snippets only for v0.15.50 and later.
    """
    path = _docs_dir() / f"docusaurus/versioned_docs/{version_dir_name(version)}/"
    files = glob.glob(f"{path}/**/*.md", recursive=True)
    pattern = re.compile(r"((.*)(file *= *)((../)*))(.*)")
    path_to_insert = f"versioned_code/version-{version}/"

    print(f"Processing {len(files)} files in change_paths_for_docs_file_references...")
    for file_path in files:
        with open(file_path, "r+") as f:
            contents = f.read()
            contents = re.sub(pattern, rf"\1{path_to_insert}\6", contents)
            f.seek(0)
            f.truncate()
            f.write(contents)
        if verbose:
            print(f"processed {file_path}")
    print(f"Processed {len(files)} files in change_paths_for_docs_file_references")


def prepend_version_info_to_name_for_snippet_by_name_references(
    version: Version,
    verbose: bool = False,
) -> None:
    """Prepend version info e.g. name="snippet_name" -> name="version-0.15.50 snippet_name" """

    version_string = version_dir_name(version)
    pattern = re.compile(r"((.*)(name *= *\"))(.*)")

    print(f"Processing prepend_version_info_to_name_for_snippet_by_name_references...")
    for path in (
        _path_to_versioned_docs() / version_string,
        _path_to_versioned_code() / version_string,
    ):
        dir_name = path.name
        files = []
        for extension in (".md", ".mdx", ".py", ".yml", ".yaml"):
            files.extend(glob.glob(f"{path}/**/*{extension}", recursive=True))
        print(
            f"    Processing {len(files)} files for path {path} in prepend_version_info_to_name_for_snippet_by_name_references..."
        )
        for file_path in files:
            with open(file_path, "r+") as f:
                contents = f.read()
                contents = re.sub(pattern, rf"\1{dir_name} \4", contents)
                f.seek(0)
                f.truncate()
                f.write(contents)
            if verbose:
                print(f"processed {file_path}")
        print(
            f"    Processed {len(files)} files for path {path} in prepend_version_info_to_name_for_snippet_by_name_references"
        )


def prepend_version_info_to_name_for_href_absolute_links(
    version: Version, verbose: bool = False
) -> None:
    """Prepend version info to absolute links: /docs/... becomes /docs/{version}/..."""

    href_pattern = re.compile(r"(?P<href>href=[\"\']/docs/)(?P<link>\S*[\"\'])")
    print(f"Processing prepend_version_info_to_name_for_href_absolute_links...")
    for path in _path_to_versioned_docs() / version_dir_name(
        version
    ), _path_to_versioned_code() / version_dir_name(version):
        files = []
        for extension in (".md", ".mdx"):
            files.extend(glob.glob(f"{path}/**/*{extension}", recursive=True))
        print(
            f"    Processing {len(files)} files for path {path} in prepend_version_info_to_name_for_href_absolute_links..."
        )
        for file_path in files:
            with open(file_path, "r+") as f:
                contents = f.read()
                # href="/docs/link" -> href="/docs/0.14.13/link"
                contents = re.sub(
                    href_pattern, rf"\g<href>{version}/\g<link>", contents
                )
                f.seek(0)
                f.truncate()
                f.write(contents)
            if verbose:
                print(f"processed {file_path}")
        print(
            f"    Processed {len(files)} files for path {path} in prepend_version_info_to_name_for_href_absolute_links"
        )


def update_tag_references_for_correct_version(
    version: Version,
    verbose: bool = False,
) -> None:
    """Change _tag.mdx to point to appropriate version."""

    path = _path_to_versioned_docs() / version_dir_name(version)

    method_name_for_logging = "update_tag_references_for_correct_version"
    print(f"Processing {method_name_for_logging}...")

    files = [path / "term_tags/_tag.mdx"]
    print(
        f"    Processing {len(files)} files for path {path} in {method_name_for_logging}..."
    )
    for file_path in files:
        with open(file_path, "r+") as f:
            contents = f.read()
            # <a href={'/docs/' + data[props.tag].url}>{props.text}</a>
            # to ->
            # <a href={'/docs/0.14.13/' + data[props.tag].url}>{props.text}</a>
            # where 0.14.13 is replaced with the corresponding doc version e.g. 0.14.13, 0.15.50, etc.
            contents = _update_tag_references_for_correct_version_substitution(
                contents, version
            )
            f.seek(0)
            f.truncate()
            f.write(contents)
        if verbose:
            print(f"processed {file_path}")
    print(
        f"    Processed {len(files)} files for path {path} in {method_name_for_logging}"
    )
    print(f"Processed {method_name_for_logging}")


def _update_tag_references_for_correct_version_substitution(
    contents: str, version: Version
) -> str:
    """Change _tag.mdx to point to appropriate version.

    Args:
        contents: String to perform substitution.
        version: String of version number e.g. "0.15.50"

    Returns:
        Updated contents
    """
    pattern = re.compile(r"(?P<href><a href=\{'/docs/)(?P<rest>')")
    contents = re.sub(pattern, rf"\g<href>{version}/\g<rest>", contents)
    return contents


def use_relative_path_for_imports(
    version: Version,
    verbose: bool = False,
) -> None:
    """Use relative imports instead of @site

    e.g. `import TechnicalTag from '../term_tags/_tag.mdx';`
    instead of `import TechnicalTag from '@site/docs/term_tags/_tag.mdx';`
    """

    path = _path_to_versioned_docs() / version_dir_name(version)

    method_name_for_logging = "use_relative_imports_for_tag_references"
    print(f"Processing {method_name_for_logging}...")
    files: list[pathlib.Path] = []
    for extension in (".md", ".mdx"):
        files.extend(path.glob(f"**/*{extension}"))
    print(
        f"    Processing {len(files)} files for path {path} in {method_name_for_logging}..."
    )
    for file_path in files:
        with open(file_path, "r+") as f:
            contents = f.read()
            contents = _use_relative_path_for_imports_substitution(
                contents, path, file_path
            )
            contents = _use_relative_path_for_imports_substitution_path_starting_with_forwardslash(
                contents, path, file_path
            )
            f.seek(0)
            f.truncate()
            f.write(contents)
        if verbose:
            print(f"processed {file_path}")
    print(
        f"    Processed {len(files)} files for path {path} in {method_name_for_logging}"
    )


def _use_relative_path_for_imports_substitution(
    contents: str, path_to_versioned_docs: pathlib.Path, path_to_document: pathlib.Path
) -> str:
    """Change import path to use relative instead of @site alias.

    e.g. `import TechnicalTag from '../term_tags/_tag.mdx';`
    instead of `import TechnicalTag from '@site/docs/term_tags/_tag.mdx';`

    Args:
        contents: String to perform substitution.
        path_to_versioned_docs: e.g. "docs/docusaurus/versioned_docs/version-0.14.13/"
        path_to_document: Path to the document containing the import to substitute.

    Returns:
        Updated contents
    """
    relative_path = path_to_document.relative_to(path_to_versioned_docs)
    dotted_relative_path = "/".join(".." for _ in range(len(relative_path.parts) - 1))
    pattern = re.compile(
        r"(?P<import>import .* from ')(?P<at_site>@site/docs/)(?P<rest>.*)"
    )
    contents = re.sub(pattern, rf"\g<import>{dotted_relative_path}/\g<rest>", contents)
    return contents


def _use_relative_path_for_imports_substitution_path_starting_with_forwardslash(
    contents: str, path_to_versioned_docs: pathlib.Path, path_to_document: pathlib.Path
) -> str:
    """Change import path to use relative instead of starting from /docs/.

    e.g. `import TechnicalTag from '../../term_tags/_tag.mdx';`
    instead of `import TechnicalTag from '/docs/term_tags/_tag.mdx';`

    Also if the path starts with / and the relative path is the same directory, then we will
    replace the path with a dot. E.g.  `import CLIRemoval from '/components/warnings/_cli_removal.md'` ->
    `import CLIRemoval from './components/warnings/_cli_removal.md'`

    Args:
        contents: String to perform substitution.
        path_to_versioned_docs: e.g. "docs/docusaurus/versioned_docs/version-0.14.13/"
        path_to_document: Path to the document containing the import to substitute.

    Returns:
        Updated contents
    """
    relative_path = path_to_document.relative_to(path_to_versioned_docs)
    dotted_relative_path = "/".join(".." for _ in range(len(relative_path.parts) - 1))
    pattern = re.compile(
        r"(?P<import>import .* from ')(?P<slash_docs>/docs/)(?P<rest>.*)"
    )
    contents = re.sub(pattern, rf"\g<import>{dotted_relative_path}/\g<rest>", contents)

    contents = contents.replace(
        "'/components/warnings/_cli_removal.md'",
        "'./components/warnings/_cli_removal.md'",
    )

    return contents


def prepend_version_info_to_name_for_md_relative_links(
    version: Version,
    verbose: bool = False,
) -> None:
    """Prepend version info to md relative links.

    Links to ../../../../docs/guides/validation/index.md#checkpoints
    Should link to: ../../../../docs/0.16.16/guides/validation/#checkpoints

    Args:
        verbose: Whether to print verbose output.
    """

    # Currently fixes the following:
    # docs/docusaurus/versioned_docs/version-0.16.16/deployment_patterns/how_to_use_gx_with_aws/components/_checkpoint_create_and_run.md
    # Links to ../../../../docs/guides/validation/index.md#checkpoints
    # Should link to: ../../../../docs/0.16.16/guides/validation/#checkpoints
    # docs/docusaurus/versioned_docs/version-0.16.16/deployment_patterns/how_to_use_gx_with_aws/components/_data_docs_build_and_view.md
    # Links to ../../../../docs/guides/validation/index.md#actions
    # Should link to: ../../../../docs/0.16.16/guides/validation/index.md#actions

    dir_name = version_dir_name(version)
    path = _path_to_versioned_docs() / dir_name

    method_name_for_logging = (
        "prepend_version_info_to_name_for_md_relative_links_to_index_files"
    )
    print(f"Processing {method_name_for_logging}...")

    files = []
    for extension in (".md", ".mdx"):
        files.extend(glob.glob(f"{path}/**/*{extension}", recursive=True))
    print(
        f"    Processing {len(files)} files for path {path} in {method_name_for_logging}..."
    )
    # NOTE: update files_to_process if there are more files that use relative markdown links.
    # Alternatively, remove this list if all files should be processed.
    files_to_process = [
        "_data_docs_build_and_view.md",
        "_checkpoint_create_and_run.md",
    ]
    files = [file for file in files if file.split("/")[-1] in files_to_process]
    for file_path in files:
        with open(file_path, "r+") as f:
            contents = f.read()
            contents = (
                _prepend_version_info_to_name_for_md_relative_links_to_index_files(
                    contents=contents, version=version
                )
            )
            contents = _prepend_version_info_to_name_for_md_relative_links(
                contents=contents, version=version
            )
            f.seek(0)
            f.truncate()
            f.write(contents)
        if verbose:
            print(f"processed {file_path}")
    print(
        f"    Processed {len(files)} files for path {path} in {method_name_for_logging}"
    )


def _prepend_version_info_to_name_for_md_relative_links(
    contents: str, version: Version
) -> str:
    """
    Fixes issues like this:
    Location of link: docs/docusaurus/versioned_docs/version-0.17.23/deployment_patterns/how_to_use_gx_with_aws/components/_checkpoint_create_and_run.md
    Link structure: ../../../../docs/guides/validation/checkpoints/checkpoint_lp.md
    Output link structure: ../../../../version-0.17.23/guides/validation/checkpoints/checkpoint_lp.md

    """
    pattern = re.compile(r"(?P<up_dir>(.*\.\.))/docs/(?P<path>(.*\.mdx?))")
    contents = re.sub(pattern, rf"\g<up_dir>/version-{version}/\g<path>", contents)

    return contents


def prepend_version_info_to_name_for_md_images(
    version: Version, verbose: bool = False
) -> None:
    """Prepend version info to md relative image links.

    Links to ../../../static/img/<FILENAME>
    Should link to: ../../../../versioned_code/version-<VERSION>/docs/docusaurus/static/img/<FILENAME>
    Args:
        verbose: Whether to print verbose output.
    """

    path = _path_to_versioned_docs() / version_dir_name(version)

    method_name_for_logging = "prepend_version_info_to_name_for_md_images"
    print(f"Processing {method_name_for_logging}...")

    files = []
    for extension in (".md", ".mdx"):
        files.extend(glob.glob(f"{path}/**/*{extension}", recursive=True))
    print(
        f"    Processing {len(files)} files for path {path} in {method_name_for_logging}..."
    )
    for file_path in files:
        with open(file_path, "r+") as f:
            contents = f.read()
            contents = _prepend_version_info_to_name_for_md_image(
                contents=contents, version=version
            )
            f.seek(0)
            f.truncate()
            f.write(contents)
        if verbose:
            print(f"processed {file_path}")
    print(
        f"    Processed {len(files)} files for path {path} in {method_name_for_logging}"
    )


def _prepend_version_info_to_name_for_md_relative_links_to_index_files(
    contents: str, version: Version
) -> str:
    pattern = re.compile(
        r"(?P<docs>(.*\.\./docs/))(?P<middle>(.*))(?P<index>(index\.md))(?P<rest>(.*))"
    )
    contents = re.sub(pattern, rf"\g<docs>{version}/\g<middle>\g<rest>", contents)

    return contents


def _prepend_version_info_to_name_for_md_image(contents: str, version: Version) -> str:
    pattern = re.compile(r"\.\./(?P<path>(static/img/.*\.(gif|png)))")
    contents = re.sub(
        pattern,
        rf"../../versioned_code/version-{version}/docs/docusaurus/\g<path>",
        contents,
    )

    return contents


def prepend_version_info_for_md_absolute_links(
    version: Version,
    verbose: bool = False,
) -> None:
    """Add version info to md absolute links.

    e.g. `- [How to instantiate a Data Context on an EMR Spark Cluster](/docs/0.16.16/deployment_patterns/how_to_instantiate_a_data_context_on_an_emr_spark_cluster)`
    instead of `- [How to instantiate a Data Context on an EMR Spark Cluster](/docs/deployment_patterns/how_to_instantiate_a_data_context_on_an_emr_spark_cluster)`

    This also does not add the version if there already is a version in the link (e.g. when we manually reference earlier versions).

    Args:
        verbose: Whether to print verbose output.
    """

    path = _path_to_versioned_docs() / version_dir_name(version)

    method_name_for_logging = "prepend_version_info_for_md_absolute_links"
    print(f"Processing {method_name_for_logging}...")

    files: list[pathlib.Path] = []
    for extension in (".md", ".mdx"):
        files.extend(path.glob(f"**/*{extension}"))
    print(
        f"    Processing {len(files)} files for path {path} in {method_name_for_logging}..."
    )
    for file_path in files:
        with open(file_path, "r+") as f:
            contents = f.read()
            contents = _prepend_version_info_for_md_absolute_links(contents, version)
            f.seek(0)
            f.truncate()
            f.write(contents)
        if verbose:
            print(f"processed {file_path}")
    print(
        f"    Processed {len(files)} files for path {path} in {method_name_for_logging}"
    )


def _prepend_version_info_for_md_absolute_links(contents: str, version: Version) -> str:
    # The negative lookahead (?!\d{1,2}\.\d{1,2}\.\d{1,2}) ensures that we don't add the version if there
    # already is a version in the link (e.g. when we manually reference earlier versions):

    pattern = re.compile(
        r"(?P<text>\[(.*?)\])(?P<link_start_no_version>\(/docs/(?!\d{1,2}\.\d{1,2}\.\d{1,2}))(?P<link_rest>(.*?)\))"
    )
    contents = re.sub(
        pattern, rf"\g<text>\g<link_start_no_version>{version}/\g<link_rest>", contents
    )
    return contents


def prepare_prior_versions(versions: list[Version]) -> None:
    print("Starting to process files in prepare_prior_versions.py...")
    for version in versions:
        prepare_prior_version(version)
    print("Finished processing files in prepare_prior_versions.py")


def prepare_prior_version(version: Version) -> None:
    print(f"Starting to process files for version {version}")
    if version < Version(0, 15):
        change_paths_for_docs_file_references(version)
    if version >= Version(0, 15):
        prepend_version_info_to_name_for_snippet_by_name_references(version)

    prepend_version_info_to_name_for_href_absolute_links(version)
    update_tag_references_for_correct_version(version)
    use_relative_path_for_imports(version)

    if version >= Version(0, 16):
        prepend_version_info_to_name_for_md_relative_links(version)

    prepend_version_info_for_md_absolute_links(version)

    if version >= Version(0, 16):
        prepend_version_info_to_name_for_md_images(version)


@total_ordering
@dataclass(frozen=True)
class Version:
    major: int = 0
    minor: int = 0
    patch: int = 0

    def as_tuple(self) -> tuple[int, int, int]:
        return self.major, self.minor, self.patch

    def __lt__(self, other: Version) -> bool:
        return self.as_tuple() < other.as_tuple()

    @override
    def __str__(self) -> str:
        return ".".join(str(x) for x in self.as_tuple())

    @staticmethod
    def from_string(string: str) -> Version:
        major, minor, patch = [int(x) for x in string.split(".")]
        return Version(major, minor, patch)
