"""
great_expectations/datasource/datasource_serializer.py:23: error: Method "serialize" is not using @override but is overriding a method in class "great_expectations.core.serializer.AbstractConfigSerializer"  [explicit-override]
"""
from __future__ import annotations

import pathlib
import sys
from collections import Counter
from pprint import pformat as pf
from typing import Iterable, Iterator, NamedTuple

from mypy import api
from typing_extensions import override


class Result(NamedTuple):
    source: pathlib.Path
    lineno: int
    message: str
    error_code: str

    @override
    def __repr__(self) -> str:
        return f"({self.source}, {self.lineno}, {self.error_code})"


def iterate_lines(stdout: str, limit: int = 15) -> Iterator[Result]:
    for i, line in enumerate(stdout.splitlines()):
        if i >= limit:
            break
        source, lineno, _, full_message = line.split(":")
        message, error_code = full_message.split("  ")
        if error_code != "[explicit-override]":
            continue
        yield Result(pathlib.Path(source), int(lineno), message.lstrip(), error_code)


LINENO_MAPPINGS: Counter[pathlib.Path] = Counter()


def insert_override(source: pathlib.Path, lineno: int) -> None:
    LINENO_MAPPINGS.update([source])
    with open(source) as f_in:
        lines = f_in.readlines()
    lines.insert(lineno - 2 + LINENO_MAPPINGS[source], "    @override\n")
    with open(source, "w") as f_out:
        f_out.writelines(lines)


def add_imports(sources: Iterable[pathlib.Path]):
    for source in sources:
        with open(source) as f_in:
            lines = f_in.readlines()
        lines.insert(0, "from typing_extensions import override\n")
        with open(source, "w") as f_out:
            f_out.writelines(lines)


result = api.run(sys.argv[1:])

stdout = result[0]

for error in iterate_lines(stdout):
    print(error)
    insert_override(error.source, error.lineno)

add_imports(LINENO_MAPPINGS.keys())

print(f"{pf(LINENO_MAPPINGS)}")
