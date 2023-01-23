"""This module provides logic for building Sphinx Docs via an Invoke command.

It is currently specific to this build pattern but can be generalized if
needed in the future.

Typical usage example:

    @invoke.task(
        help={
            "clean": "Clean out existing documentation first. Defaults to True.",
        }
    )
    def my_task(
        ctx,
        clean=True,
    ):

        doc_builder = SphinxInvokeDocsBuilder(ctx=ctx)
        doc_builder.exit_with_error_if_docs_dependencies_are_not_installed()
        doc_builder.build_docs(clean=clean)
        ...
"""
import ast
import importlib
import logging
import os
import pathlib
import shutil
from typing import Dict, Tuple
from urllib.parse import urlparse

import invoke
from bs4 import BeautifulSoup

from scripts.check_public_api_docstrings import (
    get_public_api_definitions,
    get_public_api_module_level_function_definitions,
)
from scripts.public_api_report import Definition, get_shortest_dotted_path

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


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

        logger.debug("Dependencies installed, proceeding.")

    def _build_html_api_docs_in_temp_folder(self):
        """Builds html api documentation in temporary folder."""
        cmd = f"sphinx-build -M html ./ {self.temp_sphinx_html_dir} -E"
        self.ctx.run(cmd, echo=True, pty=True)
        logger.debug("Raw Sphinx HTML generated.")

    def _remove_existing_api_docs(self) -> None:
        """Removes the existing api docs."""
        if self.docusaurus_api_docs_path.is_dir():
            shutil.rmtree(self.docusaurus_api_docs_path)
        pathlib.Path(self.docusaurus_api_docs_path).mkdir(parents=True, exist_ok=True)
        logger.debug("Existing Docusaurus API docs removed.")

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
            logger.info(f"Processing: {str(html_file.absolute())}")
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

                    shortened_path_version = self._get_mdx_file_path(
                        pathlib.Path(split_href[0])
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

                # Add front matter
                doc_front_matter = (
                    "---\n"
                    f"title: {title_str}\n"
                    f"sidebar_label: {title_str}\n"
                    "---\n"
                    "\n"
                )
                doc_str = doc_front_matter + doc_str

                # Write out mdx files
                output_path = self.docusaurus_api_docs_path / self._get_mdx_file_path(
                    html_file_path=html_file
                )
                output_path.parent.mkdir(parents=True, exist_ok=True)
                logger.info(f"Writing out mdx file: {str(output_path.absolute())}")
                with open(output_path, "w") as fout:
                    fout.write(doc_str)

        logger.info("Created mdx files for serving with docusaurus.")

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

            # Join the shortest path and write output
            output_path = (path_prefix / html_file_path.stem).with_suffix(".mdx")
        else:
            output_path = html_file_path.relative_to(static_html_file_path).with_suffix(
                ".mdx"
            )

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

        logger.debug("Removed existing generated raw Sphinx HTML.")

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

        excluded_files: Tuple[pathlib.Path, ...] = (
            self.base_path / "index.md",
            self.base_path / "README.md",
        )

        all_files: Tuple[pathlib.Path] = tuple(self.base_path.glob("*.md"))

        for file in all_files:
            if file not in excluded_files:
                file.unlink()
