"""Drift detection between the generated datasource factory methods and their type stub.

The data source manager generates one create, update, create-or-update, and delete method per
registered fluent datasource type. Those methods only exist at runtime through dynamic
attribute assignment, so a type checker only sees them if a matching declaration is hand
maintained in the manager's stub file. This module derives the expected declarations from the
live registry and compares them against what the stub actually declares, so a declaration that
falls out of sync with the registry is caught without invoking a type checker at all.
"""

from __future__ import annotations

import ast
import inspect
import pathlib
from typing import FrozenSet

import pytest

from great_expectations.datasource.fluent import sources
from great_expectations.datasource.fluent.sources import DataSourceManager

# Longest-first so a create-or-update declaration is not misread as a create declaration
# followed by a nonsense type part (e.g. "add_or_update_postgres" must not be parsed as
# prefix "add_" plus type "or_update_postgres").
_FACTORY_PREFIXES: tuple[str, ...] = (
    "add_or_update_",
    "add_",
    "update_",
    "delete_",
)


def _stub_path() -> pathlib.Path:
    """The manager's type stub, located from the module's own file rather than a fixed path."""
    module_file = inspect.getfile(sources)
    return pathlib.Path(module_file).with_suffix(".pyi")


def _registered_type_names() -> FrozenSet[str]:
    return frozenset(DataSourceManager.type_lookup.type_names())


def _expected_factory_methods() -> FrozenSet[str]:
    """Every generated factory method name, derived from the live fluent type registry."""
    type_names = _registered_type_names()
    return frozenset(
        f"{prefix}{type_name}" for prefix in _FACTORY_PREFIXES for type_name in type_names
    )


def _split_factory_name(method_name: str) -> tuple[str, str] | None:
    """Split a method name into its prefix and type part, matching longest-first.

    Returns None if the name does not start with any of the recognized prefixes.
    """
    for prefix in _FACTORY_PREFIXES:
        if method_name.startswith(prefix):
            return prefix, method_name[len(prefix) :]
    return None


def _factory_type_part(method_name: str) -> str:
    """The type part of a name already known to be factory-shaped."""
    split = _split_factory_name(method_name)
    assert split is not None, f"{method_name} is not factory-shaped"
    return split[1]


def _declared_method_names(stub_path: pathlib.Path) -> FrozenSet[str]:
    """Method names declared on the data source manager class in a stub file.

    Parses the stub as a syntax tree rather than importing it, because a stub file is not an
    importable runtime module. Overload groups (repeated declarations of the same name, most
    commonly decorated with ``@overload``) collapse to a single occurrence of that name.
    """
    tree = ast.parse(stub_path.read_text(), filename=str(stub_path))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == DataSourceManager.__name__:
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    names.add(item.name)
    return frozenset(names)


def _declared_factory_methods(stub_path: pathlib.Path) -> FrozenSet[str]:
    """Declared method names on the manager class that are shaped like a factory method."""
    return frozenset(
        name for name in _declared_method_names(stub_path) if _split_factory_name(name)
    )


@pytest.mark.unit
def test_expected_factory_methods_are_not_vacuous() -> None:
    """The expected set must be nonempty and exactly four times the registered type count.

    Without this guard, a registry that yields no type names would make the drift check pass
    trivially, having compared nothing against nothing.
    """
    registered_type_names = _registered_type_names()
    expected = _expected_factory_methods()

    assert expected, "Derived no generated factory methods, which means the registry appears empty."
    assert len(expected) == 4 * len(registered_type_names), (
        f"Expected {4 * len(registered_type_names)} generated factory methods "
        f"(4 prefixes x {len(registered_type_names)} registered types), "
        f"but derived {len(expected)}."
    )


@pytest.mark.unit
def test_longest_prefix_matches_first() -> None:
    """A create-or-update name resolves to that prefix, not to create plus a nonsense type."""
    split = _split_factory_name("add_or_update_postgres")
    assert split == ("add_or_update_", "postgres"), (
        f"Expected 'add_or_update_postgres' to split into prefix 'add_or_update_' and type "
        f"'postgres', but got {split!r}."
    )


@pytest.mark.unit
def test_stub_declares_every_generated_factory_method() -> None:
    """Every generated factory method for a registered type must appear in the stub."""
    stub_path = _stub_path()
    expected = _expected_factory_methods()
    declared = _declared_method_names(stub_path)

    missing = sorted(expected - declared)
    assert not missing, (
        "The following generated factory methods are missing from "
        f"{stub_path}: {missing}. Each is generated for a registered datasource type; "
        "add a matching declaration to the stub for each missing method and its type: "
        + ", ".join(f"{name} ({_factory_type_part(name)})" for name in missing)
    )


@pytest.mark.unit
def test_stub_declares_no_factory_methods_for_unregistered_types() -> None:
    """A factory-shaped stub declaration whose type part is not registered is stale."""
    stub_path = _stub_path()
    registered_type_names = _registered_type_names()
    declared_factory_methods = _declared_factory_methods(stub_path)

    stale = sorted(
        name
        for name in declared_factory_methods
        if _factory_type_part(name) not in registered_type_names
    )
    assert not stale, (
        f"The following declarations in {stub_path} are factory-shaped but their type part is "
        f"not a registered datasource type: {stale}. Remove or correct each stale declaration."
    )
