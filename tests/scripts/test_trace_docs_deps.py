from typing import List

import py.path

from scripts import find_docusaurus_refs, get_import_paths, get_local_imports


def test_find_docusaurus_refs_parses_correctly(tmpdir_factory):
    docs_dir: py.path.local = tmpdir_factory.mktemp("docs")
    temp_file: py.path.local = docs_dir.join("file.md")
    with open(temp_file, "w") as f:
        f.write(
            "```python file=../../../../../../../../../../../../../../../a/b/c/script1.py#L1\n"
        )
        f.write(
            "```python file=../../../../../../../../../../../../../../../d/e/f/script2.py#L5-10"
        )

    assert sorted(find_docusaurus_refs(str(docs_dir))) == [
        "a/b/c/script1.py",
        "d/e/f/script2.py",
    ]


def test_find_docusaurus_refs_does_not_duplicate(tmpdir_factory):
    docs_dir: py.path.local = tmpdir_factory.mktemp("docs")
    temp_file: py.path.local = docs_dir.join("temp_file.md")
    with open(temp_file, "w") as f:
        f.write(
            "```python file=../../../../../../../../../../../../../../../a/b/c/script_a.py#L1\n"
        )
        f.write(
            "```python file=../../../../../../../../../../../../../../../a/b/c/script_a.py#L5-10"
        )

    assert len(find_docusaurus_refs(str(docs_dir))) == 1


def test_get_local_imports_parses_correctly(tmpdir_factory):
    temp_dir: py.path.local = tmpdir_factory.mktemp("temp_dir")
    temp_file: py.path.local = temp_dir.join("temp_file.py")
    with open(temp_file, "w") as f:
        f.write(
            "from great_expectations.rule_based_profiler.profiler import Profiler\n"
        )
        f.write("from great_expectations.core.util import nested_update")

    assert sorted(get_local_imports([str(temp_file)])) == [
        "great_expectations.core.util",
        "great_expectations.rule_based_profiler.profiler",
    ]


def test_get_local_imports_discards_external_dependencies(tmpdir_factory):
    temp_dir: py.path.local = tmpdir_factory.mktemp("temp_dir")
    temp_file: py.path.local = temp_dir.join("temp_file.py")
    with open(temp_file, "w") as f:
        f.write("from ruamel import YAML\n")
        f.write("import pytest")

    assert len(get_local_imports([str(temp_file)])) == 0


def test_get_local_imports_discards_general_ge_imports(tmpdir_factory):
    temp_dir: py.path.local = tmpdir_factory.mktemp("temp_dir")
    temp_file: py.path.local = temp_dir.join("temp_file.py")
    with open(temp_file, "w") as f:
        f.write("import great_expectations\n")
        f.write("import great_expectations as ge")

    assert len(get_local_imports([str(temp_file)])) == 0
