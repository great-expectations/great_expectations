"""This module provides logic for building Sphinx Docs via an Invoke command.

It is currently specific to this build pattern but can be generalized if
needed in the future.

Typical usage example:

    @invoke.task()
    def my_task(
        ctx,
    ):

        doc_builder = SphinxInvokeDocsBuilder(ctx=ctx)
        doc_builder.exit_with_error_if_docs_dependencies_are_not_installed()
        doc_builder.build_docs()
        ...
"""
from __future__ import annotations

import ast
import enum
import importlib
import logging
import os
import pathlib
import shutil
import sys
from dataclasses import dataclass
from typing import Dict
from urllib.parse import urlparse

import invoke

from docs.sphinx_api_docs_source.check_public_api_docstrings import (
    get_public_api_definitions,
    get_public_api_module_level_function_definitions,
)
from docs.sphinx_api_docs_source.public_api_report import (
    Definition,
    get_shortest_dotted_path,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


class SidebarEntryType(enum.Enum):
    """Type of sidebar entry."""

    MODULE = "module"
    CLASS = "class"


@dataclass
class SidebarEntry:
    """Paths and metadata for a sidebar entry."""

    name: str
    definition: Definition
    class_min_dotted_path: str | None
    md_relpath: pathlib.Path
    mdx_relpath: pathlib.Path
    type: SidebarEntryType


class SphinxInvokeDocsBuilder:
    """Utility class to support building API docs using Sphinx and Invoke."""

    def __init__(
        self,
        ctx: invoke.context.Context,
        api_docs_source_path: pathlib.Path,
        repo_root: pathlib.Path,
    ) -> None:
        """Creates SphinxInvokeDocsBuilder instance.

        Args:
            ctx: Invoke context for use in running commands.
            api_docs_source_path: Path to the api docs source. Where sphinx-build is run, should include conf.py.
            repo_root: Path to the repo root.
        """
        self.ctx = ctx
        self.api_docs_source_path = api_docs_source_path
        self.docs_path = api_docs_source_path.parent
        self.repo_root = repo_root
        self.gx_path = self.repo_root / "great_expectations"

        self.temp_sphinx_html_dir = self.repo_root / "temp_sphinx_api_docs_build_dir"
        self.docusaurus_api_docs_path = self.docs_path / pathlib.Path(
            "docusaurus/docs/reference/api"
        )
        self.definitions: Dict[str, Definition] = {}
        self.sidebar_entries: Dict[str, SidebarEntry] = {}
        self.written_class_md_stubs: dict[
            pathlib.Path, list[str]
        ] = {}  # Dict of {path_to_class_def: ["ClassName1", "ClassName2", ...]}

    def build_docs(self) -> None:
        """Main method to build Sphinx docs and convert to Docusaurus."""

        self.exit_with_error_if_docs_dependencies_are_not_installed()

        self._remove_temp_html()
        self._remove_md_stubs()
        self._remove_existing_api_docs()

        self._build_class_md_stubs()
        self._build_module_md_stubs()

        self._build_html_api_docs_in_temp_folder()

        logger.info("Creating mdx files from HTML for serving with docusaurus.")
        self._create_mdx_files_for_docusaurus_from_sphinx_html()
        logger.info("Created mdx files for serving with docusaurus.")

        self._remove_md_stubs()

        self._remove_temp_html()

    def exit_with_error_if_docs_dependencies_are_not_installed(self) -> None:
        """Checks and report which dependencies are not installed."""

        module_dependencies = ("sphinx", "myst_parser", "pydata_sphinx_theme", "bs4")
        modules_not_installed = []

        for module_name in module_dependencies:
            try:
                importlib.import_module(module_name)
            except ImportError:
                modules_not_installed.append(module_name)

        if modules_not_installed:
            raise invoke.Exit(
                f"Please make sure to install missing docs dependencies: {', '.join(modules_not_installed)} by running pip install -r {self.api_docs_source_path / 'requirements-dev-api-docs.txt'}",
                code=1,
            )
        logger.debug("Dependencies installed, proceeding.")

    def _build_html_api_docs_in_temp_folder(self):
        """Builds html api documentation in temporary folder."""

        sphinx_api_docs_source_dir = self.api_docs_source_path
        if sphinx_api_docs_source_dir not in sys.path:
            sys.path.append(str(sphinx_api_docs_source_dir))

        cmd = f"sphinx-build -M html {self.api_docs_source_path} {self.temp_sphinx_html_dir} -E"
        self.ctx.run(cmd, echo=True, pty=True)
        logger.debug("Raw Sphinx HTML generated.")

    def _remove_existing_api_docs(self) -> None:
        """Removes the existing api docs."""
        if self.docusaurus_api_docs_path.is_dir():
            shutil.rmtree(self.docusaurus_api_docs_path)
        pathlib.Path(self.docusaurus_api_docs_path).mkdir(parents=True, exist_ok=True)
        logger.debug("Existing Docusaurus API docs removed.")

    def _create_mdx_files_for_docusaurus_from_sphinx_html(self) -> None:
        """Creates API docs as mdx files to serve from docusaurus.

        Content for the mdx files is between <section> tags in the sphinx
        generated html docs.
        """

        # Read the generated html and process the content for conversion to mdx
        # Write out to .mdx file using the relative file directory structure
        for html_file_path in self._get_generated_html_file_paths():
            logger.debug(f"Processing: {str(html_file_path.absolute())}")
            with open(html_file_path.absolute()) as f:
                html_file_contents = f.read()
                doc_str = self._parse_and_process_html_to_mdx(
                    html_file_path, html_file_contents
                )

                # Write out mdx files
                output_path = self.docusaurus_api_docs_path / self._get_mdx_file_path(
                    sidebar_entry=self._get_sidebar_entry(html_file_path=html_file_path)
                )
                output_path.parent.mkdir(parents=True, exist_ok=True)
                logger.debug(f"Writing out mdx file: {str(output_path.absolute())}")
                with open(output_path, "w") as fout:
                    fout.write(doc_str)

    def _get_generated_html_file_paths(self):
        """Collect html file paths from Sphinx-generated html, skipping known index paths."""
        static_html_file_path = pathlib.Path(self.temp_sphinx_html_dir) / "html"
        paths = static_html_file_path.glob("**/*.html")
        files = [
            p
            for p in paths
            if p.is_file()
            and p.name
            not in ("genindex.html", "search.html", "index.html", "py-modindex.html")
            and "_static" not in str(p)
        ]
        return files

    def _parse_and_process_html_to_mdx(
        self, html_file_path: pathlib.Path, html_file_contents: str
    ) -> str:
        """Parse sphinx generated html and make changes suitable for use in docusaurus.

        Args:
            html_file_path: Path to the html file to process.
            html_file_contents: Contents of the html file to process.

        Returns:
            Content suitable for use in a docusaurus mdx file.
        """
        from bs4 import (
            BeautifulSoup,
        )  # Importing here since it is not a library requirement

        soup = BeautifulSoup(html_file_contents, "html.parser")

        # Retrieve and remove the title (it will also be autogenerated by docusaurus)
        title = soup.find("h1").extract()
        title_str = title.get_text(strip=True)
        title_str = title_str.replace("#", "")

        sidebar_entry = self._get_sidebar_entry(html_file_path=html_file_path)

        # Add .py to module titles
        if sidebar_entry.type == SidebarEntryType.MODULE:
            stem_path = pathlib.Path(
                self._get_mdx_file_path(sidebar_entry=sidebar_entry).stem
            )
            mdx_stripped_path = str(stem_path.with_suffix(""))
            if mdx_stripped_path.lower() == title_str.lower():
                title_str = str(stem_path.with_suffix(".py"))

        # Add class="sphinx-api-doc" to section tag to reference in css
        doc = soup.find("section")
        doc["class"] = "sphinx-api-doc"

        # Change style to styles to avoid rendering errors
        tags_with_style = doc.find_all("col", style=lambda value: value in value)
        for tag in tags_with_style:
            style = tag["style"]
            del tag["style"]
            tag["styles"] = style

        # Process documentation links
        external_refs = doc.find_all(class_="reference external")
        for external_ref in external_refs:
            url = external_ref.string
            url_parts = urlparse(url)
            url_path = url_parts.path.strip("/").split("/")
            url_text = url_path[-1]

            formatted_text = url_text.replace("_", " ").title()

            external_ref.string = formatted_text

        # Process internal links
        # Note: Currently the docusaurus link checker does not work well with
        # anchor links, so we need to make these links absolute.
        # We also need to use the shortened path version to match
        # what is used for the page url.
        internal_refs = doc.find_all(class_="reference internal")
        for internal_ref in internal_refs:
            href = internal_ref["href"]

            split_href = href.split("#")

            href_path = pathlib.Path(split_href[0])

            if str(href_path) == ".":
                # For self referential links, use the file path
                shortened_path_version = self._get_mdx_file_path(
                    sidebar_entry=self._get_sidebar_entry(html_file_path=html_file_path)
                ).with_suffix("")
            else:
                shortened_path_version = self._get_mdx_file_path(
                    sidebar_entry=self._get_sidebar_entry(html_file_path=href_path)
                ).with_suffix("")

            fragment = ""
            if len(split_href) > 1:
                fragment = split_href[1]

            absolute_href = (
                self._get_base_url() + str(shortened_path_version) + "#" + fragment
            )
            internal_ref["href"] = absolute_href

        doc_str = str(doc)

        # Add front matter and handle code blocks
        code_block_exists = "CodeBlock" in doc_str
        doc_str = self._add_doc_front_matter(
            doc=doc_str, title=title_str, import_code_block=code_block_exists
        )
        if code_block_exists:
            doc_str = self._clean_up_code_blocks(doc_str)

        return doc_str

    def _get_sidebar_entry(self, html_file_path: pathlib.Path) -> SidebarEntry:
        """Get the sidebar entry from html file path.

        Args:
            html_file_path: Path of the generated html file. Stem used as key.

        Returns:
            SidebarEntry corresponding to html file path.
        """
        return self.sidebar_entries.get(html_file_path.stem)

    def _get_mdx_file_path(self, sidebar_entry: SidebarEntry) -> pathlib.Path:
        """Get the mdx file path for a processed sphinx-generated html file.

        If the subject is a class, the shortest dotted import path will
        be used to generate the path.

        Args:
            html_file_path: Path to the html file that is being processed.

        Returns:
            Path within the API docs folder to create the mdx file.
        """

        output_path = sidebar_entry.mdx_relpath

        return output_path

    def _get_class_mdx_relative_filepath(self, definition: Definition) -> pathlib.Path:
        """Get the filepath to use for the docusaurus mdx file from a class definition."""

        shortest_dotted_path = get_shortest_dotted_path(
            definition=definition, repo_root_path=self.repo_root
        )
        # Use parent since file will create sidebar entry with class name
        # (if .parent is not used, then there will be a duplicate).
        path_prefix = pathlib.Path(shortest_dotted_path.replace(".", "/")).parent

        # Remove the `great_expectations` top level
        if path_prefix.parts[0] == "great_expectations":
            path_prefix = pathlib.Path(*path_prefix.parts[1:])

        # Use the `_class` suffix to differentiate between classes and folders with
        # the same name since the docusaurus sidebar is case insensitive. E.g.
        # great_expectations/checkpoint path and Checkpoint class
        definition_path = path_prefix / f"{definition.name}_class"

        return definition_path.with_suffix(".mdx")

    def _get_module_mdx_relative_filepath(self, definition: Definition) -> pathlib.Path:
        """Get the filepath to use for the docusaurus mdx file from a module definition."""

        if definition.filepath.is_absolute():
            definition_path = definition.filepath.relative_to(self.gx_path)
        else:
            definition_path = definition.filepath

        return definition_path.with_suffix(".py.mdx")

    def _get_base_url(self) -> str:
        """The base url for use in generating absolute links.

        Note, this will need to be modified if we begin to nest
        directories inside of /docs/reference/api/
        """
        # URL is an environment variable provided by Netlify
        base_url = os.getenv("URL", "http://localhost:3000")
        return f"{base_url}/docs/reference/api/"

    def _remove_temp_html(self) -> None:
        """Remove the Sphinx-generated temporary html files + related files."""
        temp_dir = pathlib.Path(self.temp_sphinx_html_dir)
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)

        logger.debug("Removed existing generated raw Sphinx HTML.")

    def _build_class_md_stubs(self) -> None:
        """Build markdown stub files with rst directives for auto documenting classes."""
        definitions = get_public_api_definitions()

        for definition in definitions:
            if isinstance(definition.ast_definition, ast.ClassDef):
                self.definitions[definition.name] = definition

                sidebar_entry = SidebarEntry(
                    name=definition.name,
                    definition=definition,
                    class_min_dotted_path=get_shortest_dotted_path(
                        definition=definition, repo_root_path=self.repo_root
                    ),
                    md_relpath=self._get_md_file_name_from_entity_name(
                        definition=definition
                    ),
                    mdx_relpath=self._get_class_mdx_relative_filepath(
                        definition=definition
                    ),
                    type=SidebarEntryType.CLASS,
                )

                self.sidebar_entries[definition.name] = sidebar_entry

                md_stub = self._create_class_md_stub(definition=definition)
                self._write_stub(
                    stub=md_stub,
                    path=sidebar_entry.md_relpath,
                )

                filepath_to_add = self.written_class_md_stubs.get(
                    definition.filepath, []
                )
                filepath_to_add.append(definition.name)
                self.written_class_md_stubs[definition.filepath] = filepath_to_add

    def _write_stub(self, stub: str, path: pathlib.Path) -> None:
        """Write the markdown stub file with appropriate filename."""
        filepath = self.api_docs_source_path / path
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with filepath.open("w") as f:
            f.write(stub)

    def _build_module_md_stubs(self) -> None:
        """Build markdown stub files with rst directives for auto documenting modules."""

        definitions = get_public_api_module_level_function_definitions()

        for definition in definitions:
            self.definitions[definition.name] = definition

            sidebar_entry = SidebarEntry(
                name=definition.name,
                definition=definition,
                class_min_dotted_path=get_shortest_dotted_path(
                    definition=definition, repo_root_path=self.repo_root
                ),
                md_relpath=self._get_module_flat_path(definition=definition),
                mdx_relpath=self._get_module_mdx_relative_filepath(
                    definition=definition
                ),
                type=SidebarEntryType.MODULE,
            )

            sidebar_entry_key = str(
                self._get_module_flat_path(definition=definition, suffix="")
            )
            self.sidebar_entries[sidebar_entry_key] = sidebar_entry

            md_stub = self._create_module_md_stub(definition=definition)

            self._write_stub(
                stub=md_stub,
                path=sidebar_entry.md_relpath,
            )

    def _get_module_flat_path(
        self, definition: Definition, suffix=".md"
    ) -> pathlib.Path:
        """Turn a path into a single filename.

        e.g. "my/path/to/file.md" -> "my_path_to_file.md"
        """
        relpath = definition.filepath.relative_to(self.gx_path).with_suffix(suffix)
        flat_path = str(relpath).replace("/", "_")
        return pathlib.Path(flat_path)

    def _get_entity_name(self, definition: Definition) -> str:
        """Get the name of the entity (class, module, function)."""
        return definition.ast_definition.name

    def _get_module_name(self, definition: Definition) -> str:
        """Get the name of the module from the definition."""
        return definition.filepath.with_suffix("").parts[-1]

    def _get_md_file_name_from_entity_name(
        self, definition: Definition
    ) -> pathlib.Path:
        """Generate markdown file name from the entity definition."""
        entity_name = self._get_entity_name(definition=definition)
        return pathlib.Path(f"{entity_name}.md")

    def _get_md_file_name_from_dotted_path_prefix(
        self, definition: Definition
    ) -> pathlib.Path:
        """Generate markdown file name from the dotted path prefix."""
        dotted_path_prefix = self._get_dotted_path_prefix(definition=definition)
        path = pathlib.Path(dotted_path_prefix.replace(".", "/") + ".md")
        return path

    def _get_dotted_path_prefix(self, definition: Definition):
        """Get the dotted path up to the class or function name."""
        path = definition.filepath
        relpath = path.relative_to(self.repo_root)
        dotted_path_prefix = str(".".join(relpath.parts)).replace(".py", "")
        return dotted_path_prefix

    def _create_class_md_stub(self, definition: Definition) -> str:
        """Create the markdown stub content for a class."""
        class_name = self._get_entity_name(definition=definition)
        dotted_import = get_shortest_dotted_path(
            definition=definition, repo_root_path=self.repo_root
        )
        return f"""```{{eval-rst}}

:orphan:

```

# {class_name}

```{{eval-rst}}
.. autoclass:: {dotted_import}
   :members:
   :inherited-members:

```
"""

    def _create_module_md_stub(self, definition: Definition) -> str:
        """Create the markdown stub content for a module.

        Note, this does not exclude classes that are already documented
        on their own class-specific pages.
        """
        dotted_path_prefix = self._get_dotted_path_prefix(definition=definition)
        file_name = dotted_path_prefix.split(".")[-1]

        return f"""```{{eval-rst}}

:orphan:

```

# {file_name}

```{{eval-rst}}
.. automodule:: {dotted_path_prefix}
   :members:
   :inherited-members:
   :noindex:

```
"""

    def _remove_md_stubs(self):
        """Remove all markdown stub files."""

        excluded_files: tuple[pathlib.Path, ...] = (
            self.api_docs_source_path / "index.md",
            self.api_docs_source_path / "README.md",
        )

        all_files: tuple[pathlib.Path] = tuple(self.api_docs_source_path.glob("*.md"))

        for file in all_files:
            if file not in excluded_files:
                file.unlink()

    def _add_doc_front_matter(
        self, doc: str, title: str, import_code_block: bool = False
    ) -> str:
        """Add front matter to the beginning of doc.

        Args:
            doc: Document to add front matter to.
            title: Desired title for the doc.
            import_code_block: Whether to include import of docusaurus code block component.

        Returns:
            Document with front matter added.
        """
        import_code_block_content = ""
        if import_code_block:
            import_code_block_content = "import CodeBlock from '@theme/CodeBlock';"

        doc_front_matter = (
            "---\n"
            f"title: {title}\n"
            f"sidebar_label: {title}\n"
            "---\n"
            f"{import_code_block_content}"
            "\n\n"
        )
        doc = doc_front_matter + doc

        return doc

    def _clean_up_code_blocks(self, doc: str) -> str:
        """Revert escaped characters in code blocks.

        CodeBlock common characters <,>,` get escaped when generating HTML.
        Also quotes use a different quote character. This method cleans up
        these items so that the code block is rendered appropriately.
        """
        doc = doc.replace("&lt;", "<").replace("&gt;", ">")
        doc = (
            doc.replace("“", '"').replace("”", '"').replace("‘", "'").replace("’", "'")
        )
        doc = doc.replace("<cite>{", "`").replace("}</cite>", "`")
        doc = doc.replace("${", r"\${")
        return doc
