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
import importlib
import logging
import os
import pathlib
import shutil
import sys
from typing import Dict
from urllib.parse import urlparse

import invoke
from bs4 import BeautifulSoup

from scripts.check_public_api_docstrings import (
    get_public_api_definitions,
    get_public_api_module_level_function_definitions,
)
from scripts.public_api_report import Definition, get_shortest_dotted_path

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.StreamHandler())
LOGGER.setLevel(logging.INFO)


class SphinxInvokeDocsBuilder:
    """Utility class to support building API docs using Sphinx and Invoke."""

    def __init__(self, ctx: invoke.context.Context, base_path: pathlib.Path) -> None:
        """Creates SphinxInvokeDocsBuilder instance.

        Args:
            ctx: Invoke context for use in running commands.
            base_path: Path command is run in for use in determining relative paths.
        """
        self.ctx = ctx
        self.base_path = base_path
        self.docs_path = base_path.parent
        self.repo_root = self.docs_path.parent

        self.temp_sphinx_html_dir = (
            self.repo_root / "temp_docs_build_dir" / "sphinx_api_docs"
        )
        self.docusaurus_api_docs_path = self.docs_path / pathlib.Path("reference/api")
        self.class_definitions: Dict[str, Definition] = {}

    def build_docs(self) -> None:
        """Main method to build Sphinx docs and convert to Docusaurus."""

        self.exit_with_error_if_docs_dependencies_are_not_installed()

        self._remove_temp_html()
        self._remove_md_stubs()
        self._remove_existing_api_docs()

        self._build_class_md_stubs()
        self._build_module_md_stubs()

        self._build_html_api_docs_in_temp_folder()

        self._process_and_create_docusaurus_mdx_files()

        self._remove_md_stubs()

        self._remove_temp_html()

    @staticmethod
    def exit_with_error_if_docs_dependencies_are_not_installed() -> None:
        """Checks and report which dependencies are not installed."""

        module_dependencies = ("sphinx", "myst_parser", "pydata_sphinx_theme")
        modules_not_installed = []

        for module_name in module_dependencies:
            try:
                importlib.import_module(module_name)
            except ImportError:
                modules_not_installed.append(module_name)

        if modules_not_installed:
            raise invoke.Exit(
                f"Please make sure to install missing docs dependencies: {', '.join(modules_not_installed)} by running pip install -r docs/sphinx_api_docs_source/requirements-dev-api-docs.txt",
                code=1,
            )

        LOGGER.debug("Dependencies installed, proceeding.")

    def _build_html_api_docs_in_temp_folder(self):
        """Builds html api documentation in temporary folder."""

        sphinx_api_docs_source_dir = pathlib.Path.cwd()
        if sphinx_api_docs_source_dir not in sys.path:
            sys.path.append(str(sphinx_api_docs_source_dir))

        cmd = f"sphinx-build -M html ./ {self.temp_sphinx_html_dir} -E"
        self.ctx.run(cmd, echo=True, pty=True)
        LOGGER.debug("Raw Sphinx HTML generated.")

    def _remove_existing_api_docs(self) -> None:
        """Removes the existing api docs."""
        if self.docusaurus_api_docs_path.is_dir():
            shutil.rmtree(self.docusaurus_api_docs_path)
        pathlib.Path(self.docusaurus_api_docs_path).mkdir(parents=True, exist_ok=True)
        LOGGER.debug("Existing Docusaurus API docs removed.")

    def _process_and_create_docusaurus_mdx_files(self) -> None:
        """Creates API docs as mdx files to serve from docusaurus from content between <section> tags in the sphinx generated docs."""

        # First get file paths
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

        # Read the generated html and process the content for conversion to mdx
        # Write out to .mdx file using the relative file directory structure
        for html_file in files:
            LOGGER.info(f"Processing: {str(html_file.absolute())}")
            with open(html_file.absolute()) as f:
                soup = BeautifulSoup(f.read(), "html.parser")

                # Retrieve and remove the title (it will also be autogenerated by docusaurus)
                title = soup.find("h1").extract()
                title_str = title.get_text(strip=True)
                title_str = title_str.replace("#", "")

                # Add class="sphinx-api-doc" to section tag to reference in css
                doc = soup.find("section")
                doc["class"] = "sphinx-api-doc"

                # Change style to styles to avoid rendering errors
                tags_with_style = doc.find_all(
                    "col", style=lambda value: value in value
                )
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
                            html_file_path=html_file
                        ).with_suffix(".html")
                    else:
                        # Use the shortened path generated from the href
                        shortened_path_version = self._get_mdx_file_path(
                            html_file_path=pathlib.Path(split_href[0])
                        ).with_suffix(".html")

                    fragment = ""
                    if len(split_href) > 1:
                        fragment = split_href[1]

                    absolute_href = (
                        self._get_base_url()
                        + str(shortened_path_version)
                        + "#"
                        + fragment
                    )
                    internal_ref["href"] = absolute_href

                doc_str = str(doc)

                code_block_exists = "CodeBlock" in doc_str
                doc_str = self._add_doc_front_matter(
                    doc=doc_str, title=title_str, import_code_block=code_block_exists
                )
                if code_block_exists:
                    doc_str = self._clean_up_code_blocks(doc_str)

                # Write out mdx files
                output_path = self.docusaurus_api_docs_path / self._get_mdx_file_path(
                    html_file_path=html_file
                )
                output_path.parent.mkdir(parents=True, exist_ok=True)
                LOGGER.info(f"Writing out mdx file: {str(output_path.absolute())}")
                with open(output_path, "w") as fout:
                    fout.write(doc_str)

        LOGGER.info("Created mdx files for serving with docusaurus.")

    def _get_mdx_file_path(self, html_file_path: pathlib.Path) -> pathlib.Path:
        """Get the mdx file path for a processed sphinx-generated html file.

        If the subject is a class, the shortest dotted import path will
        be used to generate the path.

        Args:
            html_file_path: Path to the html file that is being processed.

        Returns:
            Path within the API docs folder to create the mdx file.
        """

        static_html_file_path = pathlib.Path(self.temp_sphinx_html_dir) / "html"

        # Get the definition
        definition = self.class_definitions.get(html_file_path.stem)

        if definition:
            # Use definition to find the shortest path
            shortest_dotted_path = get_shortest_dotted_path(
                definition=definition, repo_root_path=self.repo_root
            )
            # Use parent since file will create sidebar entry with class name
            # (if .parent is not used, then there will be a duplicate).
            path_prefix = pathlib.Path(shortest_dotted_path.replace(".", "/")).parent

            # Remove the `great_expectations` top level
            if path_prefix.parts[0] == "great_expectations":
                path_prefix = pathlib.Path(*path_prefix.parts[1:])

            # Join the shortest path and write output
            output_path = (path_prefix / html_file_path.stem).with_suffix(".mdx")
        else:
            output_path = html_file_path.relative_to(static_html_file_path).with_suffix(
                ".mdx"
            )
            # Remove the `great_expectations` top level
            if output_path.parts[0] == "great_expectations":
                output_path = pathlib.Path(*output_path.parts[1:])

        return output_path

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

        LOGGER.debug("Removed existing generated raw Sphinx HTML.")

    def _build_class_md_stubs(self) -> None:
        """Build markdown stub files with rst directives for auto documenting classes."""
        definitions = get_public_api_definitions()

        for definition in definitions:
            if isinstance(definition.ast_definition, ast.ClassDef):
                self.class_definitions[definition.name] = definition
                md_stub = self._create_class_md_stub(definition=definition)
                self._write_stub(
                    stub=md_stub,
                    path=self._get_md_file_name_from_entity_name(definition=definition),
                )

    def _write_stub(self, stub: str, path: pathlib.Path) -> None:
        """Write the markdown stub file with appropriate filename."""
        filepath = self.base_path / path
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with filepath.open("w") as f:
            f.write(stub)

    def _build_module_md_stubs(self) -> None:
        """Build markdown stub files with rst directives for auto documenting modules."""

        definitions = get_public_api_module_level_function_definitions()

        for definition in definitions:
            md_stub = self._create_module_md_stub(definition=definition)
            self._write_stub(
                stub=md_stub,
                path=self._get_md_file_name_from_dotted_path_prefix(
                    definition=definition
                ),
            )

    def _get_entity_name(self, definition: Definition) -> str:
        """Get the name of the entity (class, module, function)."""
        return definition.ast_definition.name

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
        return f"""# {class_name}

```{{eval-rst}}
.. autoclass:: {dotted_import}
   :members:
   :inherited-members:

```
"""

    def _create_module_md_stub(self, definition: Definition) -> str:
        """Create the markdown stub content for a module."""
        dotted_path_prefix = self._get_dotted_path_prefix(definition=definition)
        file_name = dotted_path_prefix.split(".")[-1]

        return f"""# {file_name}

```{{eval-rst}}
.. automodule:: {dotted_path_prefix}
   :members:
   :inherited-members:

```
"""

    def _remove_md_stubs(self):
        """Remove all markdown stub files."""

        excluded_files: tuple[pathlib.Path, ...] = (
            self.base_path / "index.md",
            self.base_path / "README.md",
        )

        all_files: tuple[pathlib.Path] = tuple(self.base_path.glob("*.md"))

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
