"""Report of which methods and classes are used in our public documentation.

This module contains utilities that scan our documentation examples and compare
with our codebase to determine which methods are used in examples and thus
should be considered part of our public API.

The utilities are generally used as follows:

DocsExampleParser.get_names_from_usage_in_docs_examples()

1. AST walk through docs examples to find all imports of classes and methods
    that are GX related (by checking the import location).
2. AST walk through docs examples to find all method calls (currently we only
    retrieve the names, not the location of the method definition). These are
    not filtered to be only GX related, we filter in step 6.
3. Scan all code examples for class names used in yaml strings.

CodeParser.get_all_class_method_and_function_definitions()

4. AST walk through full GX codebase to find all classes and method names from
    their definitions, and capture the definition file location.

PublicAPIChecker
5. Get list of classes & methods that are marked `public_api`.

parse_docs_contents_for_class_names()

Scan docs content for class names used in yaml strings.

CodeReferenceFilter

6. Filter list of classes & methods from docs examples to only those found in
    the GX codebase (e.g. filter out print() or other python or 3rd party
    classes/methods).
7. Use this filtered list against the list of class and method definitions in
    the GX codebase to generate the full list with definition locations in the
    GX codebase.
8. Filter out private methods and classes.
9. Filter or include based on Include and Exclude directives (include overrides).
10. Filter out methods that are not marked @public_api

PublicAPIReport
11. Generate report based on list of Definitions.


Typical usage example:

  main() method provided with typical usage.
  These utilities can also be used in tests to determine if there has been a
  change to our public API.
"""
from __future__ import annotations

import ast
import glob
import logging
import operator
import pathlib
import re
import sys
from dataclasses import dataclass
from typing import List, Set, Union, cast

from docs.sphinx_api_docs_source.include_exclude_definition import (
    IncludeExcludeDefinition,
)
from docs.sphinx_api_docs_source import public_api_excludes
from docs.sphinx_api_docs_source import public_api_includes
from docs.sphinx_api_docs_source import public_api_missing_threshold

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Removed from imports due to circular import issues
PUBLIC_API_DECORATOR_NAME = "public_api"


@dataclass(frozen=True)
class Definition:
    """Class, method or function definition information from AST parsing.

    Args:
        name: name of class, method or function.
        filepath: Where the definition was found.
        ast_definition: Full AST tree of the class,
            method or function definition.
    """

    name: str
    filepath: pathlib.Path
    ast_definition: Union[ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef]


class FileContents:
    """File contents as a string and file path.

    Args:
        filepath: Absolute path to the file.
        contents: String of the file contents.
    """

    def __init__(self, filepath: pathlib.Path, contents: str):
        self.filepath = filepath
        self.contents = contents

    @classmethod
    def create_from_local_file(cls, filepath: pathlib.Path) -> FileContents:
        with open(filepath) as f:
            file_contents: str = f.read()
        return cls(filepath=filepath, contents=file_contents)

    @classmethod
    def create_from_local_files(cls, filepaths: Set[pathlib.Path]) -> Set[FileContents]:
        return {cls.create_from_local_file(filepath) for filepath in filepaths}


class DocsExampleParser:
    """Parse examples from docs to find classes, methods and functions used."""

    def __init__(self, file_contents: Set[FileContents]) -> None:
        self.file_contents = file_contents

    def get_names_from_usage_in_docs_examples(self) -> Set[str]:
        """Get names in docs examples of classes, methods and functions used.

        Usages are retrieved from imports and function / method calls.

        Returns:
            Names of classes, methods and functions as a set of strings.
        """
        all_usages = set()
        for file_contents in self.file_contents:
            file_usages = self._get_names_of_all_usages_in_file(
                file_contents=file_contents
            )
            all_usages |= file_usages
        return all_usages

    def _get_names_of_all_usages_in_file(self, file_contents: FileContents) -> Set[str]:
        """Retrieve the names of all class, method + functions used in file_contents."""

        tree = ast.parse(file_contents.contents)

        function_calls = self._get_all_function_calls(tree=tree)
        function_names = self._get_non_private_function_names(calls=function_calls)
        logger.debug(f"function_names: {function_names}")

        gx_imports = self._list_all_gx_imports(tree=tree)
        import_names = self._get_non_private_gx_import_names(imports=gx_imports)
        logger.debug(f"import_names: {import_names}")

        pattern = re.compile(r"class_name: (\w+)")
        matches = re.finditer(pattern, file_contents.contents)
        yaml_names = {m.group(1) for m in matches}

        return function_names | import_names | yaml_names

    def _list_all_gx_imports(
        self, tree: ast.AST
    ) -> List[Union[ast.Import, ast.ImportFrom]]:
        """Get all the GX related imports in an ast tree."""

        imports: List[Union[ast.Import, ast.ImportFrom]] = []

        for node in ast.walk(tree):
            node_is_imported_from_gx = isinstance(
                node, ast.ImportFrom
            ) and node.module.startswith(  # type: ignore[union-attr]
                "great_expectations"
            )
            node_is_gx_import = isinstance(node, ast.Import) and any(
                n.name.startswith("great_expectations") for n in node.names
            )
            if node_is_imported_from_gx:
                cast(ast.ImportFrom, node)
                imports.append(node)  # type: ignore[arg-type]
            elif node_is_gx_import:
                cast(ast.Import, node)
                imports.append(node)  # type: ignore[arg-type]

        return imports

    def _get_non_private_gx_import_names(
        self, imports: List[Union[ast.Import, ast.ImportFrom]]
    ) -> Set[str]:
        """From ast trees, get names of all non private GX related imports."""

        names = []
        for import_ in imports:
            if not isinstance(import_, (ast.Import, ast.ImportFrom)):
                raise TypeError(
                    f"`imports` should only contain ast.Import, ast.ImportFrom types, you provided {type(import_)}"
                )

            # Generally there is only 1 alias,
            # but we add all names if there are multiple aliases to be safe.
            names.extend([n.name for n in import_.names if not n.name.startswith("_")])

        return set(names)

    def _get_all_function_calls(self, tree: ast.AST) -> List[ast.Call]:
        """Get all the function calls from an ast tree."""
        calls = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                calls.append(node)

        return calls

    def _get_non_private_function_names(self, calls: List[ast.Call]) -> Set[str]:
        """Get function names that are not private from ast.Call objects."""
        names = []
        for call in calls:
            name = None
            if isinstance(call.func, ast.Attribute):
                name = call.func.attr
            elif isinstance(call.func, ast.Name):
                name = call.func.id

            if name and not name.startswith("_"):
                names.append(name)

        return set(names)


class CodeParser:
    """Parse code for class, method and function definitions."""

    def __init__(
        self,
        file_contents: Set[FileContents],
    ) -> None:
        """Create a CodeParser.

        Args:
            file_contents: A set of FileContents objects to parse.
        """
        self.file_contents = file_contents

    def get_all_class_method_and_function_names(
        self,
    ) -> Set[str]:
        """Get string names of all classes, methods and functions in all FileContents."""
        all_usages = set()
        for file_contents in self.file_contents:
            usages = self._get_all_class_method_and_function_names_from_file_contents(
                file_contents=file_contents
            )
            all_usages |= usages
        return all_usages

    def _get_all_class_method_and_function_names_from_file_contents(
        self, file_contents: FileContents
    ) -> Set[str]:
        """Get string names of all classes, methods and functions in a single FileContents."""
        definitions: Set[
            Union[ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef]
        ] = self._get_all_entity_definitions_from_file_contents(
            file_contents=file_contents
        )

        return {definition.name for definition in definitions}

    def get_all_class_method_and_function_definitions(
        self,
    ) -> Set[Definition]:
        """Get Definition objects for all class, method and function definitions."""
        all_usages: Set[Definition] = set()
        for file_contents in self.file_contents:
            entity_definitions = self._get_all_entity_definitions_from_file_contents(
                file_contents=file_contents
            )
            all_usages |= self._build_file_usage_definitions(
                file_contents=file_contents, entity_definitions=entity_definitions
            )
        return all_usages

    def get_module_level_function_definitions(self) -> Set[Definition]:
        """Get Definition objects only for functions defined at the module level."""
        all_usages: Set[Definition] = set()
        for file_contents in self.file_contents:
            module_level_function_definitions = (
                self._get_module_level_function_definitions_from_file_contents(
                    file_contents=file_contents
                )
            )
            all_usages |= self._build_file_usage_definitions(
                file_contents=file_contents,
                entity_definitions=module_level_function_definitions,
            )
        return all_usages

    def _build_file_usage_definitions(
        self,
        file_contents: FileContents,
        entity_definitions: Set[
            Union[ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef]
        ],
    ) -> Set[Definition]:
        """Build Definitions from FileContents."""
        file_usages_definitions: List[Definition] = []
        for usage in entity_definitions:
            candidate_definition = Definition(
                name=usage.name,
                filepath=file_contents.filepath,
                ast_definition=usage,
            )
            file_usages_definitions.append(candidate_definition)

        return set(file_usages_definitions)

    def _get_all_entity_definitions_from_file_contents(
        self, file_contents: FileContents
    ) -> Set[Union[ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef]]:
        """Parse FileContents to retrieve entity definitions as ast trees."""
        tree = ast.parse(file_contents.contents)
        all_defs: List[Union[ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef]] = []
        all_defs.extend(self._list_class_definitions(tree=tree))
        all_defs.extend(self._list_function_definitions(tree=tree))

        return set(all_defs)

    def _get_module_level_function_definitions_from_file_contents(
        self, file_contents: FileContents
    ) -> Set[Union[ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef]]:
        """Parse FileContents to retrieve module level function definitions as ast trees."""
        tree = ast.parse(file_contents.contents)
        defs = self._list_module_level_function_definitions(tree=tree)
        return set(defs)

    def _list_class_definitions(self, tree: ast.AST) -> List[ast.ClassDef]:
        """List class definitions from an ast tree."""

        class_defs = []

        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                class_defs.append(node)

        return class_defs

    def _list_function_definitions(
        self, tree: ast.AST
    ) -> List[Union[ast.FunctionDef, ast.AsyncFunctionDef]]:
        """List function definitions from an ast tree."""
        function_definitions = []
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                function_definitions.append(node)

        return function_definitions

    def _list_module_level_function_definitions(
        self, tree: ast.AST
    ) -> List[Union[ast.FunctionDef, ast.AsyncFunctionDef]]:
        """List function definitions that appear outside of classes."""

        function_definitions = []
        for node in ast.iter_child_nodes(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                function_definitions.append(node)

        return function_definitions


def parse_docs_contents_for_class_names(file_contents: Set[FileContents]) -> Set[str]:
    """Parse contents of documentation for class names.

    Parses based on class names used in yaml examples e.g. Datasource and
    SqlAlchemyExecutionEngine in the below example:

    name: my_datasource_name
    class_name: Datasource
    execution_engine:
      class_name: SqlAlchemyExecutionEngine

    Args:
        file_contents: Contents from .md and .mdx files that may contain yaml examples.

    Returns:
        Unique class names used in documentation files.
    """

    all_usages = set()
    pattern = re.compile(r"class_name: (\w+)")

    for single_file_contents in file_contents:
        matches = re.finditer(pattern, single_file_contents.contents)
        yaml_names = {m.group(1) for m in matches}
        all_usages |= yaml_names

    return all_usages


def get_shortest_dotted_path(
    definition: Definition, repo_root_path: pathlib.Path
) -> str:
    """Get the shortest dotted path to a class definition.

    e.g. if a class is imported in a parent __init__.py file use that
    instead of the file with the class definition.

    Args:
        definition: Class definition.
        repo_root_path: Repository root path to make sure paths are relative.

    Returns:
        Dotted representation of shortest import path
            e.g. great_expectations.core.ExpectationSuite
    """

    if definition.filepath.is_absolute():
        relative_definition_path = definition.filepath.relative_to(repo_root_path)
    else:
        relative_definition_path = definition.filepath

    # Traverse parent folders from definition.filepath
    shortest_path_prefix = str(".".join(relative_definition_path.parts)).replace(
        ".py", ""
    )
    shortest_path = f"{shortest_path_prefix}.{definition.name}"

    path_parts = list(relative_definition_path.parts)
    while path_parts:
        # Keep traversing, shortening path if shorter path is found.
        path_parts.pop()
        # if __init__.py is found, ast parse and check for import of the class
        init_path = repo_root_path / pathlib.Path(*path_parts, "__init__.py")
        if init_path.is_file():
            import_names = []
            with open(init_path) as f:
                file_contents: str = f.read()

            import_names.extend(_get_import_names(file_contents))

            # If name is found in imports, shorten path to where __init__.py is found
            if definition.name in import_names:
                shortest_path_prefix = str(".".join(path_parts))
                shortest_path = f"{shortest_path_prefix}.{definition.name}"

    return shortest_path


def _get_import_names(code: str) -> List[str]:
    """Get import names from import statements.

    Args:
        code: Code with imports to parse.

    Returns:
        Flattened list of names from imports.
    """

    tree = ast.parse(code)

    import_names = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                import_names.append(alias.name)

    return import_names


class PublicAPIChecker:
    """Check if functions, methods and classes are marked part of the PublicAPI."""

    def __init__(
        self,
        code_parser: CodeParser,
    ) -> None:
        self.code_parser = code_parser

    def get_all_public_api_definitions(self) -> Set[Definition]:
        """Get definitions that are marked with the public api decorator."""
        definitions: List[Definition] = []

        for (
            definition
        ) in self.code_parser.get_all_class_method_and_function_definitions():
            if self.is_definition_marked_public_api(definition):
                definitions.append(definition)

        return set(definitions)

    def get_module_level_function_public_api_definitions(self) -> Set[Definition]:
        """Get module level function definitions that are marked with the public api decorator."""
        definitions: List[Definition] = []

        for definition in self.code_parser.get_module_level_function_definitions():
            if self.is_definition_marked_public_api(definition):
                definitions.append(definition)

        return set(definitions)

    def is_definition_marked_public_api(self, definition: Definition) -> bool:
        """Determine if a definition is marked with the public api decorator."""

        result = False
        found_decorators = self._get_decorator_names(
            ast_definition=definition.ast_definition
        )

        if PUBLIC_API_DECORATOR_NAME in found_decorators:
            result = True

        return result

    def _get_decorator_names(
        self, ast_definition: Union[ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef]
    ) -> Set[str]:
        """Get all decorator names for a single definition from an ast tree."""

        def flatten_attr(node):
            if isinstance(node, ast.Attribute):
                return f"{str(flatten_attr(node.value))}.{node.attr}"
            elif isinstance(node, ast.Name):
                return str(node.id)
            else:
                pass

        found_decorators = []
        for decorator in ast_definition.decorator_list:
            if isinstance(decorator, ast.Name):
                found_decorators.append(decorator.id)
            elif isinstance(decorator, ast.Attribute):
                found_decorators.append(flatten_attr(decorator))

        return set(found_decorators)


class CodeReferenceFilter:
    """Bring together various parsing and filtering tools to build a filtered set of Definitions.

    Also adds the capability of filtering and including whole files or entities manually.
    """

    DEFAULT_INCLUDES = public_api_includes.DEFAULT_INCLUDES
    DEFAULT_EXCLUDES = public_api_excludes.DEFAULT_EXCLUDES

    def __init__(
        self,
        repo_root: pathlib.Path,
        docs_example_parser: DocsExampleParser,
        code_parser: CodeParser,
        public_api_checker: PublicAPIChecker,
        references_from_docs_content: set[str] | None = None,
        excludes: Union[List[IncludeExcludeDefinition], None] = None,
        includes: Union[List[IncludeExcludeDefinition], None] = None,
    ) -> None:
        """Create a CodeReferenceFilter.

        Args:
            repo_root: Repository root directory, for use in creating relative paths.
            docs_example_parser: A DocsExampleParser initialized with the file
                contents from all docs examples to process.
            code_parser: A CodeParser initialized with library code.
            public_api_checker: A PublicAPIChecker to aid in filtering.
            excludes: Override default excludes by supplying a list of
                IncludeExcludeDefinition instances.
            includes: Override default includes by supplying a list of
                IncludeExcludeDefinition instances. Note: Includes override
                excludes if they are conflicting.
        """
        self.repo_root = repo_root
        self.docs_example_parser = docs_example_parser
        self.code_parser = code_parser
        self.public_api_checker = public_api_checker

        if not references_from_docs_content:
            self.references_from_docs_content = set()
        else:
            self.references_from_docs_content = references_from_docs_content

        if not excludes:
            self.excludes = self.DEFAULT_EXCLUDES
        else:
            self.excludes = excludes

        if not includes:
            self.includes = self.DEFAULT_INCLUDES
        else:
            self.includes = includes

    def filter_definitions(self) -> Set[Definition]:
        """Main method to perform all filtering.

        Filters Definitions of entities (class, method and function).
        Returned Definitions:
            1. Appear in published documentation examples.
            2. Are not private.
            3. Are included or not excluded by an IncludeExcludeDefinition.
            4. Are not marked with the @public_api decorator.

        Returns:
            Definitions that pass all filters.
        """
        usages_in_docs_examples_and_docs_content: Set[str] = (
            self._docs_examples_usages() | self.references_from_docs_content
        )
        gx_definitions_used_in_docs_examples: Set[
            Definition
        ] = self._filter_gx_definitions_from_docs_examples(
            gx_usages_in_docs_examples=usages_in_docs_examples_and_docs_content
        )
        non_private_definitions: Set[Definition] = self._filter_private_entities(
            definitions=gx_definitions_used_in_docs_examples
        )
        included_definitions: Set[Definition] = self._filter_or_include(
            definitions=non_private_definitions
        )
        definitions_not_marked_public_api: Set[
            Definition
        ] = self._filter_for_definitions_not_marked_public_api(
            definitions=included_definitions
        )

        return definitions_not_marked_public_api

    def _docs_examples_usages(self) -> Set[str]:
        """Filter list of classes & methods from docs examples to only those found in
        the GX codebase

            (e.g. filter out print() or other python or 3rd party classes/methods).
        """
        doc_example_usages: Set[
            str
        ] = self.docs_example_parser.get_names_from_usage_in_docs_examples()
        gx_code_definitions = self.code_parser.get_all_class_method_and_function_names()

        doc_example_usages_of_gx_code = doc_example_usages.intersection(
            gx_code_definitions
        )
        return doc_example_usages_of_gx_code

    def _filter_gx_definitions_from_docs_examples(
        self, gx_usages_in_docs_examples: Set[str]
    ) -> Set[Definition]:
        """Filter the list of GX definitions except those used in docs examples.

        Use the docs examples filtered list against the list of class and method
        definitions in the GX codebase to generate the full list with definition
        locations in the GX codebase.

        Returns:
            Set of Definition objects with filepath locations.
        """
        gx_code_definitions = (
            self.code_parser.get_all_class_method_and_function_definitions()
        )
        gx_code_definitions_appearing_in_docs_examples = {
            d for d in gx_code_definitions if d.name in gx_usages_in_docs_examples
        }
        return gx_code_definitions_appearing_in_docs_examples

    def _filter_private_entities(self, definitions: Set[Definition]) -> Set[Definition]:
        """Filter out private entities (classes, methods and functions with leading underscore)."""
        return {d for d in definitions if not self._is_definition_private(definition=d)}

    def _filter_or_include(self, definitions: Set[Definition]) -> Set[Definition]:
        """Filter definitions per all IncludeExcludeDefinition directives.

        Includes override excludes, and also don't require the included entity
        to be used in docs examples.
        """
        included_definitions: List[Definition] = []
        all_gx_code_definitions = (
            self.code_parser.get_all_class_method_and_function_definitions()
        )
        for definition in definitions:
            definition_filepath = self._repo_relative_filepath(
                filepath=definition.filepath
            )
            exclude: bool = self._is_filepath_excluded(
                definition_filepath
            ) or self._is_definition_excluded(definition)
            include: bool = self._is_filepath_included(
                definition_filepath
            ) or self._is_definition_included(definition)

            if include or not exclude:
                included_definitions.append(definition)

        for definition in all_gx_code_definitions:
            definition_filepath = self._repo_relative_filepath(
                filepath=definition.filepath
            )
            include_from_all_gx_definitions: bool = self._is_filepath_included(
                definition_filepath
            ) or self._is_definition_included(definition)
            if (
                include_from_all_gx_definitions
                and definition not in included_definitions
            ):
                included_definitions.append(definition)

        return set(included_definitions)

    def _repo_relative_filepath(self, filepath: pathlib.Path) -> pathlib.Path:
        if filepath.is_absolute():
            return filepath.relative_to(self.repo_root)
        else:
            return filepath

    def _repo_relative_filepath_comparison(
        self, filepath_1: pathlib.Path, filepath_2: pathlib.Path
    ) -> bool:
        return str(self._repo_relative_filepath(filepath_1)) == str(
            self._repo_relative_filepath(filepath_2)
        )

    def _filter_for_definitions_not_marked_public_api(
        self, definitions: Set[Definition]
    ) -> Set[Definition]:
        """Return only those Definitions that are not marked with the public api decorator."""
        return {
            d
            for d in definitions
            if not self.public_api_checker.is_definition_marked_public_api(d)
        }

    def _is_filepath_excluded(self, filepath: pathlib.Path) -> bool:
        """Check whether an entire filepath is excluded."""
        full_filepaths_excluded = [p.filepath for p in self.excludes if not p.name]
        return filepath in full_filepaths_excluded

    def _is_definition_excluded(self, definition: Definition) -> bool:
        """Check whether a definition (filepath / name combo) is excluded."""
        definitions_excluded = [d for d in self.excludes if d.name and d.filepath]
        for definition_excluded in definitions_excluded:
            filepath_excluded = self._repo_relative_filepath_comparison(definition.filepath, definition_excluded.filepath)  # type: ignore[arg-type]
            name_excluded = definition.name == definition_excluded.name
            if filepath_excluded and name_excluded:
                return True
        return False

    def _is_filepath_included(self, filepath: pathlib.Path) -> bool:
        """Check whether an entire filepath is included."""
        full_filepaths_included = [p.filepath for p in self.includes if not p.name]
        return filepath in full_filepaths_included

    def _is_definition_included(self, definition: Definition) -> bool:
        """Check whether a definition (filepath / name combo) is included."""
        definitions_included = [d for d in self.includes if d.name and d.filepath]
        for definition_included in definitions_included:
            filepath_included = self._repo_relative_filepath_comparison(definition.filepath, definition_included.filepath)  # type: ignore[arg-type]
            name_included = definition.name == definition_included.name
            if filepath_included and name_included:
                return True
        return False

    def _is_definition_private(self, definition: Definition) -> bool:
        """Check whether the name of a definition is for a private method or class."""
        return definition.name.startswith("_")


class PublicAPIReport:
    """Generate a report from entity definitions (class, method and function)."""

    def __init__(self, definitions: Set[Definition], repo_root: pathlib.Path) -> None:
        """Create a PublicAPIReport object.

        Args:
            definitions: Entity definitions to include in the report. Generally,
                these are filtered before inclusion.
            repo_root: Path to the repo root for stripping filenames relative
                to the repository root.
        """
        self.definitions = definitions
        self.repo_root = repo_root

    def write_printable_definitions_to_file(
        self,
        filepath: pathlib.Path,
    ) -> None:
        """Generate then write the printable version of definitions to a file.

        Args:
            filepath: Output filepath.
        """
        printable_definitions = self.generate_printable_definitions()
        with open(filepath, "w") as f:
            f.write("\n".join(printable_definitions))

    def generate_printable_definitions(
        self,
    ) -> List[str]:
        """Generate a printable (human readable) definition.

        Returns:
            List of strings representing each Definition.
        """
        sorted_definitions_list = sorted(
            list(self.definitions), key=operator.attrgetter("filepath", "name")
        )
        sorted_definitions_strings: List[str] = []
        for definition in sorted_definitions_list:
            if definition.filepath.is_absolute():
                filepath = str(definition.filepath.relative_to(self.repo_root))
            else:
                filepath = str(definition.filepath)
            sorted_definitions_strings.append(
                f"File: {filepath} Name: {definition.name}"
            )

        sorted_definitions_strings_no_dupes = self._deduplicate_strings(
            sorted_definitions_strings
        )

        return sorted_definitions_strings_no_dupes

    def _deduplicate_strings(self, strings: List[str]) -> List[str]:
        """Deduplicate a list of strings, keeping order intact."""
        seen = set()
        no_duplicates = []
        for s in strings:
            if s not in seen:
                no_duplicates.append(s)
                seen.add(s)

        return no_duplicates


def _repo_root() -> pathlib.Path:
    repo_root_path = pathlib.Path(__file__).parents[2]
    return repo_root_path


def _default_doc_example_absolute_paths() -> Set[pathlib.Path]:
    """Get all paths of doc examples (docs examples)."""
    base_directory = _repo_root() / "tests" / "integration" / "docusaurus"
    paths = glob.glob(f"{base_directory}/**/*.py", recursive=True)
    return {pathlib.Path(p) for p in paths}


def _default_code_absolute_paths() -> Set[pathlib.Path]:
    """All Great Expectations modules related to the main library."""
    base_directory = _repo_root() / "great_expectations"
    paths = glob.glob(f"{base_directory}/**/*.py", recursive=True)
    return {pathlib.Path(p) for p in paths}


def _default_docs_absolute_paths() -> Set[pathlib.Path]:
    """All Great Expectations modules related to the main library."""
    base_directory = _repo_root() / "docs"
    paths = []
    for extension in ("md", "mdx", "yml", "yaml"):
        paths.extend(glob.glob(f"{base_directory}/**/*.{extension}", recursive=True))
    return {pathlib.Path(p) for p in paths}


def _parse_file_to_ast_tree(filepath: pathlib.Path) -> ast.AST:
    with open(filepath) as f:
        file_contents: str = f.read()

    tree = ast.parse(file_contents)
    return tree


def generate_public_api_report(write_to_file: bool = False) -> None:
    docs_example_file_contents = FileContents.create_from_local_files(
        _default_doc_example_absolute_paths()
    )

    code_file_contents = FileContents.create_from_local_files(
        _default_code_absolute_paths()
    )

    references_from_docs_content = parse_docs_contents_for_class_names(
        FileContents.create_from_local_files(_default_docs_absolute_paths())
    )

    docs_example_parser = DocsExampleParser(file_contents=docs_example_file_contents)

    code_parser = CodeParser(file_contents=code_file_contents)

    public_api_checker = PublicAPIChecker(code_parser=code_parser)

    code_reference_filter = CodeReferenceFilter(
        repo_root=_repo_root(),
        docs_example_parser=docs_example_parser,
        code_parser=code_parser,
        public_api_checker=public_api_checker,
        references_from_docs_content=references_from_docs_content,
    )

    filtered_definitions = code_reference_filter.filter_definitions()

    public_api_report = PublicAPIReport(
        definitions=filtered_definitions, repo_root=_repo_root()
    )

    missing_from_the_public_api = public_api_report.generate_printable_definitions()

    num_missing_msg = f"There are {len(missing_from_the_public_api)} items referenced in documentation that are not marked with the @public_api decorator and thus not rendered as part of our public API documentation."
    logger.info(num_missing_msg)
    # The missing_threshold should be reduced and kept at 0. Please do
    # not increase this threshold, but instead add to the public API by decorating
    # any methods or classes you are adding to documentation with the @public_api
    # decorator and any relevant "new" or "deprecated" public api decorators.
    # If the actual is lower than the threshold, please reduce the threshold.
    missing_threshold = len(
        public_api_missing_threshold.ITEMS_IGNORED_FROM_PUBLIC_API
    )  # TODO: reduce this number again once this works for the Fluent DS dynamic methods
    if len(missing_from_the_public_api) != missing_threshold:
        error_msg_prefix = f"There are {len(missing_from_the_public_api)} items missing from the public API, we currently allow {missing_threshold}."
        if len(missing_from_the_public_api) > missing_threshold:
            logger.error(f"{error_msg_prefix} Please add to the public API.")
            difference = set(missing_from_the_public_api) - set(
                public_api_missing_threshold.ITEMS_IGNORED_FROM_PUBLIC_API
            )
            logger.error(
                f"The {len(difference)} items missing from the public API that are not accounted for are as follows:"
            )
            for item in difference:
                logger.error(item)
        else:
            logger.error(f"{error_msg_prefix} Please reduce the threshold.")
            difference = set(
                public_api_missing_threshold.ITEMS_IGNORED_FROM_PUBLIC_API
            ) - set(missing_from_the_public_api)
            logger.error(
                f"The {len(difference)} items that are now accounted for and should be removed from the threshold list in docs/sphinx_api_docs_source/public_api_missing_threshold.py are:"
            )
            for item in difference:
                logger.error(item)
        sys.exit(1)
    else:
        logger.info(
            "All of the missing items are accounted for in the missing threshold, but this threshold should be reduced to 0 over time."
        )

    if write_to_file:
        public_api_report.write_printable_definitions_to_file(
            filepath=_repo_root() / "public_api_report.txt",
        )


if __name__ == "__main__":
    generate_public_api_report()
