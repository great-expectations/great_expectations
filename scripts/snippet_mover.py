import re
from glob import glob
from pathlib import Path
from typing import Pattern

from great_expectations.compatibility.pydantic import BaseModel, Field


class Snippet(BaseModel):
    name: str
    doc_paths: set[Path] = Field(default_factory=set)


class SnippetModule(BaseModel):
    snippets: list[Snippet] = Field(default_factory=list)
    moved: bool = False
    original_path: Path
    new_path: Path | None = None


class SnippetMover:
    """Move snippets used in docs from the tests directory to the docs directory."""

    def __init__(self, gx_root_dir: Path):
        self._root_dir = gx_root_dir
        self._snippet_lookup: dict[str, Snippet] = {}
        self._snippet_module_lookup: dict[Path, SnippetModule] = {}
        self._non_test_snippets: list[Snippet] = []  # can't move these
        self._orphaned_snippet_modules: list[
            SnippetModule
        ] = []  # not referenced by a test
        self._version_prefix = "version-0.17.23"
        self._docs_prefix = Path("docs/docusaurus/versioned_docs") / Path(
            self._version_prefix
        )
        self._default_snippet_path = self._docs_prefix / Path("snippets")
        self._docs_root_dir = gx_root_dir / self._docs_prefix
        self._code_namespace = Path("great_expectations-0.17.23")
        self._tests_prefix = self._code_namespace / Path("tests")
        self._tests_root_dir = gx_root_dir / self._tests_prefix
        self._general_files_to_update = (
            self._tests_prefix / Path("integration/test_script_runner.py"),
            self._code_namespace
            / Path("ci/checks/check_name_tag_snippets_referenced.py"),
            self._code_namespace / Path("ci/checks/check_integration_test_gets_run.py"),
            self._tests_prefix
            / Path("integration/test_definitions/postgresql/integration_tests.py"),
            self._tests_prefix
            / Path("integration/test_definitions/abs/integration_tests.py"),
            self._tests_prefix
            / Path("integration/test_definitions/athena/integration_tests.py"),
            self._tests_prefix
            / Path("integration/test_definitions/aws_glue/integration_tests.py"),
            self._tests_prefix
            / Path("integration/test_definitions/bigquery/integration_tests.py"),
            self._tests_prefix
            / Path("integration/test_definitions/gcs/integration_tests.py"),
            self._tests_prefix
            / Path("integration/test_definitions/mssql/integration_tests.py"),
            self._tests_prefix
            / Path(
                "integration/test_definitions/multiple_backend/integration_tests.py"
            ),
            self._tests_prefix
            / Path("integration/test_definitions/mysql/integration_tests.py"),
            self._tests_prefix
            / Path("integration/test_definitions/redshift/integration_tests.py"),
            self._tests_prefix
            / Path("integration/test_definitions/s3/integration_tests.py"),
            self._tests_prefix
            / Path("integration/test_definitions/snowflake/integration_tests.py"),
            self._tests_prefix
            / Path("integration/test_definitions/spark/integration_tests.py"),
            self._tests_prefix
            / Path("integration/test_definitions/sqlite/integration_tests.py"),
            self._tests_prefix
            / Path("integration/test_definitions/trino/integration_tests.py"),
        )
        self._custom_cases: dict[Path, Path] = {
            # these are custom overrides for how to rename specific files if things get wonky
            self._tests_prefix
            / Path(
                "integration/docusaurus/connecting_to_your_data/cloud/azure/spark/inferred_and_runtime_yaml_example.py"
            ): Path("inferred_and_runtime_yaml_example_spark_azure.py"),
            self._tests_prefix
            / Path(
                "integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_yaml_example.py"
            ): Path("inferred_and_runtime_yaml_example_pandas_gcs.py"),
            self._tests_prefix
            / Path(
                "integration/docusaurus/connecting_to_your_data/cloud/s3/spark/inferred_and_runtime_yaml_example.py"
            ): Path("inferred_and_runtime_yaml_example_spark_s3.py"),
        }
        self._report_path = gx_root_dir / Path("scripts/snippet_mover_report.txt")
        # make sure we have a valid dir to put snippets referenced by multiple docs
        self.ensure_dir(self._root_dir / self._default_snippet_path)

    def run(self):
        """High level business logic"""
        self.find_snippet_references_in_tests()
        self.find_snippet_references_in_docs()
        self.calculate_snippet_destinations()
        self.rename_snippets()
        self.move_snippets()
        self.build_report()

    def find_snippet_references_in_tests(self):
        """Find snippets defined in tests and catalog them.

        Snippets also exist in the codebase, but we can't move them.
        """
        py_code_match = "**/*.py"
        yml_code_match = "**/*.yml"
        yaml_code_match = "**/*.yaml"
        code_paths = [
            *self.get_all_files_by_match(self._tests_root_dir, match=py_code_match),
            *self.get_all_files_by_match(self._tests_root_dir, match=yml_code_match),
            *self.get_all_files_by_match(self._tests_root_dir, match=yaml_code_match),
        ]
        # add the tests prefix so paths are relative to root_dir
        code_paths = [self._tests_prefix / code_path for code_path in code_paths]
        code_snippet_expression = re.compile(r"snippet\W*name=\"(.*)\"")
        for code_path in code_paths:
            snippet_names = self.search_file_for_snippets(
                path=self._root_dir / code_path,
                expression=code_snippet_expression,
            )
            if not len(snippet_names):
                continue  # no snippets in this file
            snippet_module = SnippetModule(original_path=Path(code_path))
            self._snippet_module_lookup[snippet_module.original_path] = snippet_module
            for snippet_name in snippet_names:
                snippet = Snippet(
                    name=snippet_name,
                )
                self._snippet_lookup[snippet_name] = snippet
                snippet_module.snippets.append(snippet)

    def find_snippet_references_in_docs(self):
        """find all snippet references in the docs and associate them with their source.

        A single code snippet can be referenced by multiple docs."""
        md_match = "**/*.md"
        mdx_match = "**/*.mdx"
        doc_paths = [
            *self.get_all_files_by_match(self._docs_root_dir, match=md_match),
            *self.get_all_files_by_match(self._docs_root_dir, match=mdx_match),
        ]
        # add the docs prefix so paths are relative to root_dir
        doc_paths = [self._docs_prefix / doc_path for doc_path in doc_paths]
        doc_snippet_expression = re.compile(r"```\W*\w*\W*name=\"(.*)\"")
        for doc_path in doc_paths:
            snippet_names = self.search_file_for_snippets(
                path=self._root_dir / doc_path,
                expression=doc_snippet_expression,
            )
            for snippet_name in snippet_names:
                snippet = self._snippet_lookup.get(snippet_name)
                if not snippet:
                    # this snippet is referenced in code, not tests, so we can't move it
                    self._non_test_snippets.append(Snippet(name=snippet_name))
                    continue
                snippet.doc_paths.add(doc_path)

    def calculate_snippet_destinations(self):
        """Determine where each snippet should be moved."""
        orphaned_snippet_paths = set()
        for snippet_module in self._snippet_module_lookup.values():
            # which dir should we move this module to?
            snippet_docs = {
                doc_path
                for snippet in snippet_module.snippets
                for doc_path in snippet.doc_paths
            }
            if len(snippet_docs) > 1:
                # this snippet is referenced by multiple docs, so we'll move it to the default dir
                snippet_dest_dir = self._default_snippet_path
            elif len(snippet_docs) == 1:
                # this snippet is referenced by a single doc, so we'll move it adjacent to the doc
                snippet_dest_dir = list(snippet_docs)[0].parent
                # except that random gitignored dir, of course, where snippets go to vanish ;(
                # shouldn't be applicable to a legacy build
                # BLACK_HOLE_PATH = Path("docs/docusaurus/docs/reference")
                # if (
                #     snippet_dest_dir.parts[: len(BLACK_HOLE_PATH.parts)]
                #     == BLACK_HOLE_PATH.parts
                # ):
                #     snippet_dest_dir = self._default_snippet_path
            else:
                # weird, snippet module isn't referenced by any docs
                orphaned_snippet_paths.add(snippet_module.original_path)
                self._orphaned_snippet_modules.append(snippet_module)
                continue

            # what should the module be named?
            if snippet_module.original_path in self._custom_cases:
                # this allows us to manually override this naming for edge cases such as duplicate filenames
                snippet_module.new_path = (
                    snippet_dest_dir / self._custom_cases[snippet_module.original_path]
                )
            elif Path(
                snippet_dest_dir / snippet_module.original_path.parts[-1]
            ).is_file():
                raise RuntimeError(
                    f"Error: moving {snippet_module.original_path} to {snippet_dest_dir / snippet_module.original_path.parts[-1]} would overwrite another module."
                )
            else:
                # keep the snippet's original filename
                snippet_module.new_path = (
                    snippet_dest_dir / snippet_module.original_path.parts[-1]
                )

        for orphaned_path in orphaned_snippet_paths:
            self._snippet_module_lookup.pop(orphaned_path)

    def rename_snippets(self):
        """Replace any references to the snippet's old path with the new one."""
        for snippet_module in self._snippet_module_lookup.values():
            if not snippet_module.new_path:
                # can't move this snippet, so don't rename anything
                continue
            paths_to_update = [
                self._root_dir / snippet_module.original_path,
                *[
                    self._root_dir / doc_path
                    for snippet in snippet_module.snippets
                    for doc_path in snippet.doc_paths
                ],
                *[
                    self._root_dir / gen_path
                    for gen_path in self._general_files_to_update
                ],
            ]
            for path in paths_to_update:
                # account for the version prefix
                version_prefix_len = len(str(self._version_prefix)) + 1
                self.find_and_replace_text_in_file(
                    path=path,
                    old_str=str(snippet_module.original_path)[version_prefix_len:],
                    new_str=str(snippet_module.new_path),
                )

    def move_snippets(self):
        """Move the original code snippet to its new location."""
        for snippet_module in self._snippet_module_lookup.values():
            if not snippet_module.new_path:
                # can't move this snippet
                continue

            self.move_file(
                src=snippet_module.original_path,
                dest=snippet_module.new_path,
            )
            snippet_module.moved = True

    def build_report(self):
        moved_snippets = [
            snippet_module
            for snippet_module in self._snippet_module_lookup.values()
            if snippet_module.moved is True
        ]
        unmoved_snippets = [
            snippet_module
            for snippet_module in self._snippet_module_lookup.values()
            if snippet_module.moved is False
        ]
        total_snippets = len(self._snippet_module_lookup)

        shared_snippets = [
            snippet
            for snippet in self._snippet_lookup.values()
            if len(snippet.doc_paths) > 1
        ]

        snippet_total = len([snippet for snippet in self._snippet_lookup.values()])

        spacer = "\n"
        section_divider = (spacer * 4) + ("*" * 78) + spacer * 4
        text = (
            f"Total snippet module count: {total_snippets}\n"
            + f"Total snippets found: {snippet_total}\n"
            + f"Moved snippet module count: {len(moved_snippets)}\n"
            + f"Shared snippet count: {len(shared_snippets)}\n"
            + section_divider
            + f"{len(self._non_test_snippets)} snippets not found in tests:\n"
            + spacer.join([snippet.name for snippet in self._non_test_snippets])
            + section_divider
            + f"{len(unmoved_snippets)} Unmoved snippet modules:\n"
            + spacer.join([str(snippet.original_path) for snippet in unmoved_snippets])
            + section_divider
            + f"{len(self._orphaned_snippet_modules)} modules contain snippets but are not referenced by the docs:\n"
            + spacer.join(
                [
                    str(snippet.original_path)
                    for snippet in self._orphaned_snippet_modules
                ]
            )
        )
        with open(self._report_path, "w") as file:
            file.write(text)

    @classmethod
    def get_all_files_by_match(cls, root_dir: Path, match: str) -> list[str]:
        """Build a list of all filenames that match a given string within the root directory."""
        # glob.glob and Path.glob don't have exactly the same behavior - we need relative paths returned, so we use glob
        return glob(pathname=match, recursive=True, root_dir=root_dir)  # noqa: PTH207

    def search_file_for_snippets(
        self, path: Path, expression: Pattern[str]
    ) -> list[str]:
        """Build a list of all snippets referenced within a doc."""
        absolute_path = self._root_dir / path
        with open(absolute_path) as file:
            text = file.read()
            return re.findall(expression, text)

    def move_file(self, src: Path, dest: Path) -> None:
        Path.rename(self._root_dir / src, self._root_dir / dest)

    @classmethod
    def ensure_dir(cls, path: Path) -> None:
        """Check that a directory exists at the given path, and if not, create one."""
        if not path.is_dir():
            Path.mkdir(path, parents=True)

    def find_and_replace_text_in_file(
        self, path: Path, old_str: str, new_str: str
    ) -> None:
        absolute_path = self._root_dir / path
        with open(absolute_path) as file:
            text = file.read()
        text = re.sub(old_str, new_str, text)
        with open(absolute_path, "w") as file:
            file.write(text)


if __name__ == "__main__":
    GX_ROOT_DIR = Path.cwd()
    if not Path(GX_ROOT_DIR / Path("great_expectations")).is_dir():
        raise RuntimeError("SnippetMover must be invoked from the GX root directory.")
    snippet_mover = SnippetMover(gx_root_dir=GX_ROOT_DIR)
    snippet_mover.run()
