import datetime
import os
import re
from collections import defaultdict
from glob import glob
from pathlib import Path
from typing import Pattern

from pydantic import BaseModel, Field


class Snippet(BaseModel):
    name: str
    doc_paths: set[Path] = Field(default_factory=set)


class SnippetModule(BaseModel):
    snippets: list[Snippet] = Field(default_factory=list)
    moved: bool = False
    original_path: Path | None
    new_path: Path | None = None


class SnippetMover:
    """
    Move snippets used in docs from the tests directory to the docs directory.

    How it works
    preprocess:
    - search docs for snippets, and store doc path by snippet name
        if snippet_name already exists (presumably with a different doc path),
        put it in a generic snippets dir, but might not be necessary
    - search tests for snippets, and store a list of snippet names by code path

    process:
    - for each code_path, make a set of the doc_paths for each snippet_name.
        presumably, that's just a single doc_path, and we now have a target
        save to {code_path: target_dir}
    - for each code_path: target_dir
        move the file
        if any of the snippet names include code_path, update them:
            code_path
            doc_path
            test_script_runner
    """

    def __init__(self, gx_root_dir: str):
        self._root_dir = gx_root_dir
        self._snippet_lookup: dict[str, Snippet] = {}
        self._snippet_module_lookup: dict[Path, SnippetModule] = {}
        self._non_test_snippets: list[Snippet] = []  # can't move these
        self._default_snippet_path = Path("docs/docusaurus/docs/snippets")
        self._docs_prefix = "docs"
        self._docs_root_dir = os.path.join(gx_root_dir, self._docs_prefix)
        self._tests_prefix = "tests"
        self._tests_root_dir = os.path.join(gx_root_dir, self._tests_prefix)
        self._general_files_to_update = (
            "tests/integration/test_script_runner.py",
            "ci/checks/check_name_tag_snippets_referenced.py",
        )
        self._report_path = os.path.join(gx_root_dir, "snippet_mover_report.txt")
        # make sure we have a valid dir to put snippets referenced by multiple docs
        self.ensure_dir(Path(os.path.join(self._root_dir, self._default_snippet_path)))

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
        code_match = "**/*.py"  # should this be more permissive?
        code_paths = self.get_all_files_by_match(self._tests_root_dir, match=code_match)
        # add the tests prefix so paths are relative to root_dir
        code_paths = [
            os.path.join(self._tests_prefix, code_path) for code_path in code_paths
        ]
        code_snippet_expression = re.compile(r"snippet\W+name=\"(.*)\"")
        for code_path in code_paths:
            snippet_module = SnippetModule(original_path=Path(code_path))
            self._snippet_module_lookup[snippet_module.original_path] = snippet_module
            snippet_names = self.search_file_for_snippets(
                path=os.path.join(self._root_dir, code_path),
                expression=code_snippet_expression,
            )
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
        doc_paths = [
            os.path.join(self._docs_prefix, doc_path) for doc_path in doc_paths
        ]
        doc_snippet_expression = re.compile(r"```\w*\W+name=\"(.*)\"")
        for doc_path in doc_paths:
            snippet_names = self.search_file_for_snippets(
                path=os.path.join(self._root_dir, doc_path),
                expression=doc_snippet_expression,
            )
            doc_path = Path(doc_path)
            for snippet_name in snippet_names:
                snippet = self._snippet_lookup.get(snippet_name)
                if not snippet:
                    # this snippet is referenced in code, not tests, so we can't move it
                    self._non_test_snippets.append(Snippet(name=snippet_name))
                    continue
                snippet.doc_paths.add(doc_path)

    def calculate_snippet_destinations(self):
        """Determine where each snippet should be moved."""
        for snippet_module in self._snippet_module_lookup.values():
            if not snippet_module.original_path:
                # can't move this file
                continue
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
            else:
                # weird, snippet isn't referenced by any docs
                continue
            # keep the snippet's original filename the same
            snippet_module.new_path = Path(
                os.path.join(snippet_dest_dir, snippet_module.original_path.parts[-1])
            )

    def rename_snippets(self):
        """Replace any references to the snippet's old path with the new one."""
        for snippet_module in self._snippet_module_lookup.values():
            if not snippet_module.new_path:
                # can't move this snippet, so don't rename anything
                continue
            paths_to_update = [
                os.path.join(self._root_dir, snippet_module.original_path),
                *[
                    os.path.join(self._root_dir, doc_path)
                    for snippet in snippet_module.snippets
                    for doc_path in snippet.doc_paths
                ],
                *[
                    os.path.join(self._root_dir, gen_path)
                    for gen_path in self._general_files_to_update
                ],
            ]
            for path in paths_to_update:
                self.find_and_replace_text_in_file(
                    path=path,
                    old_str=str(snippet_module.original_path),
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
            if snippet_module.moved
        ]
        unmoved_snippets = [
            snippet_module
            for snippet_module in self._snippet_module_lookup.values()
            if not snippet_module.moved
        ]
        total_snippets = len(self._snippet_module_lookup)
        assert total_snippets == moved_snippets + unmoved_snippets, "huh?"

        shared_snippets = [
            snippet
            for snippet in self._snippet_lookup.values()
            if len(snippet.doc_paths) > 1
        ]

        spacer = "\n"
        section_divider = (spacer * 4) + ("*" * 78) + spacer * 4
        text = (
            f"Total snippet module count: {total_snippets}\n"
            + f"Moved snippet module count: {len(moved_snippets)}\n"
            + f"Shared snippet count: {len(shared_snippets)}\n"
            + section_divider
            + f"{len(self._non_test_snippets)} snippets not found in tests:\n"
            + spacer.join([snippet.name for snippet in self._non_test_snippets])
            + section_divider
            + f"{len(unmoved_snippets)} Unmoved snippet modules:\n"
            + spacer.join([str(snippet.original_path) for snippet in unmoved_snippets])
        )
        with open(self._report_path, "w") as file:
            file.write(text)

    @classmethod
    def get_all_files_by_match(cls, root_dir: str, match: str) -> list[str]:
        """Build a list of all filenames that match a given string within the root directory."""
        return glob(pathname=match, recursive=True, root_dir=root_dir)

    def search_file_for_snippets(
        self, path: str, expression: Pattern[str]
    ) -> list[str]:
        """Build a list of all snippets referenced within a doc."""
        absolute_path = os.path.join(self._root_dir, path)
        with open(absolute_path) as file:
            text = file.read()
            return re.findall(expression, text)

    def move_file(self, src: Path, dest: Path) -> None:
        os.rename(os.path.join(self._root_dir, src), os.path.join(self._root_dir, dest))

    @classmethod
    def ensure_dir(cls, path: Path) -> None:
        """Check that a directory exists at the given path, and if not, create one."""
        if not path.is_dir():
            os.makedirs(path)

    def find_and_replace_text_in_file(
        self, path: Path, old_str: str, new_str: str
    ) -> None:
        absolute_path = os.path.join(self._root_dir, path)
        with open(absolute_path) as file:
            text = file.read()
        text = re.sub(old_str, new_str, text)
        with open(absolute_path, "w") as file:
            file.write(text)


if __name__ == "__main__":
    GX_ROOT_DIR = os.getcwd()
    if not Path(os.path.join(GX_ROOT_DIR, "great_expectations")).is_dir():
        raise RuntimeError("SnippetMover must be invoked from the GX root directory.")
    snippet_mover = SnippetMover(gx_root_dir=GX_ROOT_DIR)
    snippet_mover.run()
