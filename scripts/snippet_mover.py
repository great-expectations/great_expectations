import os
import re
from collections import defaultdict
from pathlib import Path
from glob import glob
from typing import Pattern


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
        self._doc_paths_by_snippet_name: dict[str, set[Path]] = defaultdict(set)
        self._code_path_by_snippet_name: dict[str, Path] = {}
        self._default_snippet_path = Path(
            os.path.join(gx_root_dir, "docs/docusaurus/docs/snippets")
        )
        self._docs_prefix = "docs"
        self._docs_root_dir = os.path.join(gx_root_dir, self._docs_prefix)
        self._tests_prefix = "tests"
        self._tests_root_dir = os.path.join(gx_root_dir, self._tests_prefix)
        self._orphaned_snippet_paths: set[(str, str)] = set()
        self._general_files_to_update = ("tests/integration/test_script_runner.py",)

    def run(self):
        self.ensure_dir(self._default_snippet_path)
        # preprocess
        self.store_doc_paths_by_snippet_name()
        self.store_test_path_by_snippet_name()
        # process
        self.move_and_rename_snippets()

    def store_test_path_by_snippet_name(self):
        """Find snippets in tests and store the path by snippet_name.

        Snippets also exist in the codebase, but we can't move them.
        """
        code_match = "**/*.py"  # this should maybe be updated to be more permissive
        code_paths = self.get_all_files_by_match(self._tests_root_dir, match=code_match)
        # add the tests prefix so paths are relative to root_dir
        code_paths = [
            os.path.join(self._tests_prefix, code_path) for code_path in code_paths
        ]
        code_snippet_expression = re.compile(r"snippet name=\"(.*)\"")
        for code_path in code_paths:
            snippet_names = self.search_file_for_snippets(
                path=os.path.join(self._root_dir, code_path),
                expression=code_snippet_expression,
            )
            code_path = Path(code_path)
            for snippet_name in snippet_names:
                if self._code_path_by_snippet_name.get(snippet_name):
                    raise Exception(
                        "found the same snippet name in multiple code paths"
                    )
                self._code_path_by_snippet_name[snippet_name] = code_path

    def store_doc_paths_by_snippet_name(self):
        """find snippets in docs and store the path by snippet name"""
        markdown_match = "**/*.md"
        doc_paths = self.get_all_files_by_match(
            self._docs_root_dir, match=markdown_match
        )
        # add the docs prefix so paths are relative to root_dir
        doc_paths = [
            os.path.join(self._docs_prefix, doc_path) for doc_path in doc_paths
        ]
        doc_snippet_expression = re.compile(r"```\w* name=\"(.*)\"")
        for doc_path in doc_paths:
            snippet_names = self.search_file_for_snippets(
                path=os.path.join(self._root_dir, doc_path),
                expression=doc_snippet_expression,
            )
            doc_path = Path(doc_path)
            for snippet_name in snippet_names:
                self._doc_paths_by_snippet_name[snippet_name].add(doc_path)

    def move_and_rename_snippets(self):
        for snippet_name, code_path in self._code_path_by_snippet_name.items():
            doc_paths = self._doc_paths_by_snippet_name.get(snippet_name)
            if not doc_paths:
                self._orphaned_snippet_paths.add((snippet_name, code_path))
                continue
            if len(doc_paths) > 1:
                # this snippet is referenced by multiple docs, so we move it to the default dir
                snippet_dest_dir = self._default_snippet_path
            else:
                # this snippet is referenced by a single doc, so we move it next to the doc
                snippet_dest_dir = list(doc_paths)[0].parent
            # keep the filename the same
            snippet_dest = Path(os.path.join(snippet_dest_dir, code_path.parts[-1]))
            paths_to_update = [
                os.path.join(self._root_dir, code_path),
                *[os.path.join(self._root_dir, doc_path) for doc_path in doc_paths],
                *[
                    os.path.join(self._root_dir, gen_path)
                    for gen_path in self._general_files_to_update
                ],
            ]
            for path in paths_to_update:
                self.find_and_replace_text_in_file(
                    path=path, old_str=str(code_path), new_str=str(snippet_dest)
                )
            self.move_file(src=code_path, dest=snippet_dest)

    @classmethod
    def get_all_files_by_match(cls, root_dir: str, match: str) -> list[str]:
        """Build a list of all filenames that match a given string within the root directory."""
        return glob(pathname=match, recursive=True, root_dir=root_dir)

    def search_file_for_snippets(
        self, path: str, expression: Pattern[str]
    ) -> list[str]:
        """Build a list of all snippets referenced within a doc."""
        absolute_path = os.path.join(self._root_dir, path)
        with open(absolute_path, "r") as file:
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
        with open(absolute_path, "r") as file:
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
