from __future__ import annotations

import ast
import importlib
import json
import logging
import os
import re
import sys
import traceback
from glob import glob
from io import StringIO
from subprocess import CalledProcessError, CompletedProcess, check_output, run
from typing import Dict, List, Optional, Tuple

import click
import pkg_resources

from great_expectations.core.expectation_diagnostics.supporting_types import (
    ExpectationBackendTestResultCounts,
)
from great_expectations.data_context.data_context import DataContext
from great_expectations.exceptions.exceptions import ExpectationNotFoundError
from great_expectations.expectations.expectation import Expectation

logger = logging.getLogger(__name__)
chandler = logging.StreamHandler(stream=sys.stdout)
chandler.setLevel(logging.DEBUG)
chandler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%dT%H:%M:%S")
)
logger.addHandler(chandler)
logger.setLevel(logging.DEBUG)


expectation_tracebacks = StringIO()
expectation_checklists = StringIO()
expectation_docstrings = StringIO()
ALL_GALLERY_BACKENDS = (
    "bigquery",
    "mssql",
    "mysql",
    "pandas",
    "postgresql",
    "redshift",
    "snowflake",
    "spark",
    "sqlite",
    "trino",
)
IGNORE_NON_V3_EXPECTATIONS = (
    "expect_column_bootstrapped_ks_test_p_value_to_be_greater_than",
    "expect_column_chisquare_test_p_value_to_be_greater_than",
    "expect_column_pair_cramers_phi_value_to_be_less_than",
    "expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than",
    "expect_multicolumn_values_to_be_unique",
)
IGNORE_FAULTY_EXPECTATIONS = ("expect_column_values_to_be_valid_india_zip",)


def execute_shell_command(command: str) -> int:
    """
    Wrap subprocess command in a try/except block to provide a convenient method for pip installing dependencies.

    :param command: bash command -- as if typed in a shell/Terminal window
    :return: status code -- 0 if successful; all other values (1 is the most common) indicate an error
    """
    cwd: str = os.getcwd()  # noqa: PTH109

    path_env_var: str = os.pathsep.join([os.environ.get("PATH", os.defpath), cwd])
    env: dict = dict(os.environ, PATH=path_env_var)

    status_code: int = 0
    try:
        res: CompletedProcess = run(
            args=["bash", "-c", command],
            stdin=None,
            input=None,
            # stdout=None, # commenting out to prevent issues with `subprocess.run` in python <3.7.4
            # stderr=None, # commenting out to prevent issues with `subprocess.run` in python <3.7.4
            capture_output=True,
            shell=False,
            cwd=cwd,
            timeout=None,
            check=True,
            encoding=None,
            errors=None,
            text=True,
            env=env,
        )
        sh_out: str = res.stdout.strip()
        logger.info(sh_out)
    except CalledProcessError as cpe:
        status_code = cpe.returncode
        sys.stderr.write(cpe.output)
        sys.stderr.flush()
        exception_message: str = "A Sub-Process call Exception occurred.\n"
        exception_traceback: str = traceback.format_exc()
        exception_message += (
            f'{type(cpe).__name__}: "{str(cpe)}".  Traceback: "{exception_traceback}".'
        )
        logger.error(exception_message)

    return status_code


def get_expectations_info_dict(
    include_core: bool = True,
    include_contrib: bool = True,
    only_these_expectations: list[str] | None = None,
) -> dict:
    """Gather information for each expectation, including create/update time, run requirements among others.

    This method loads and parses the files where expectations are defined to collect this info.

    Args:
        include_core: Whether to include core expectations or not (default True).
        include_contrib: Whether to include community contributed expectations or not (default True).
        only_these_expectations: An optional list of the names of expectations to gather info for as the snake case string name e.g. `expect_column_values_to_not_be_null`

    Returns:
        A Dictionary containing the following information:
            updated_at: git last updated timestamp
            created_at: git created timestamp
            path: file path where the expectation is defined
            package: name of the package containing the expectation, typically the root directory. For core expectations it is `core` instead of the directory name.
            requirements: Additional packages that need to be installed to run the expectation.
            import_module_args: The location of the expectation for use in importing.
            sys_path: path to add to sys.path when importing an expectation.
    """
    if not only_these_expectations:
        only_these_expectations = []
    rx = re.compile(r".*?([A-Za-z]+?Expectation\b).*")
    result = {}
    files_found = []
    oldpwd = os.getcwd()  # noqa: PTH109
    os.chdir(f"..{os.path.sep}..")
    repo_path = os.getcwd()  # noqa: PTH109
    logger.debug("Finding requested Expectation files in the repo")

    if only_these_expectations:
        include_core = True
        include_contrib = True

    if include_core:
        files_found.extend(
            glob(
                os.path.join(  # noqa: PTH118
                    repo_path,
                    "great_expectations",
                    "expectations",
                    "core",
                    "expect_*.py",
                ),
                recursive=True,
            )
        )
    if include_contrib:
        files_found.extend(
            glob(
                os.path.join(repo_path, "contrib", "**", "expect_*.py"),  # noqa: PTH118
                recursive=True,
            )
        )

    for file_path in sorted(files_found):
        file_path = (  # noqa: PLW2901 # `for` loop variable overwritten
            file_path.replace(f"{repo_path}{os.path.sep}", "")
        )
        expectation_name = os.path.basename(file_path).replace(  # noqa: PTH119
            ".py", ""
        )
        if only_these_expectations and expectation_name not in only_these_expectations:
            continue
        if (
            expectation_name in IGNORE_NON_V3_EXPECTATIONS
            or expectation_name in IGNORE_FAULTY_EXPECTATIONS
        ):
            continue

        package_name = os.path.basename(  # noqa: PTH119
            os.path.dirname(os.path.dirname(file_path))  # noqa: PTH120
        )  # ,
        if package_name == "expectations":
            package_name = "core"

        requirements = []
        import_module_args = ()
        sys_path = ""
        if package_name != "core":
            requirements = get_contrib_requirements(file_path)["requirements"]
            parent_dir = os.path.dirname(os.path.dirname(file_path))  # noqa: PTH120
            grandparent_dir = os.path.dirname(parent_dir)  # noqa: PTH120

            if package_name == "great_expectations_experimental":
                import_module_args = (
                    f"expectations.{expectation_name}",
                    "great_expectations_experimental",
                )
                sys_path = os.path.join(  # noqa: PTH118
                    f"..{os.path.sep}..", parent_dir
                )
            else:
                import_module_args = (f"{package_name}.expectations",)
                sys_path = os.path.join(  # noqa: PTH118
                    f"..{os.path.sep}..", grandparent_dir
                )

        updated_at_cmd = f'git log -1 --format="%ai %ar" -- {repr(file_path)}'
        created_at_cmd = (
            f'git log --diff-filter=A --format="%ai %ar" -- {repr(file_path)}'
        )
        result[expectation_name] = {
            "updated_at": check_output(updated_at_cmd, shell=True)
            .decode("utf-8")
            .strip(),
            "created_at": check_output(created_at_cmd, shell=True)
            .decode("utf-8")
            .strip(),
            "path": file_path,
            "package": package_name,
            "requirements": requirements,
            "import_module_args": import_module_args,
            "sys_path": sys_path,
        }
        logger.debug(
            f"{expectation_name} ({package_name}) was created {result[expectation_name]['created_at']} and updated {result[expectation_name]['updated_at']}"
        )
        with open(file_path) as fp:
            text = fp.read()

        exp_type_set = set()
        for line in re.split("\r?\n", text):
            match = rx.match(line)
            if match:
                if not line.strip().startswith("#"):
                    exp_type_set.add(match.group(1))
        if file_path.startswith("great_expectations"):
            _prefix = "Core "
        else:
            _prefix = "Contrib "
        result[expectation_name]["exp_type"] = _prefix + sorted(exp_type_set)[0]
        logger.debug(
            f"Expectation type {_prefix}{sorted(exp_type_set)[0]} for {expectation_name} in {file_path}"
        )

    os.chdir(oldpwd)
    return result


def install_necessary_requirements(requirements) -> list:
    """Install any Expectation requirements that aren't already in the venv

    Return a list of things installed, so they may be uninstalled at the end
    """
    installed_packages = pkg_resources.working_set
    parsed_requirements = pkg_resources.parse_requirements(requirements)
    installed = []
    for req in parsed_requirements:
        is_satisfied = any(installed_pkg in req for installed_pkg in installed_packages)
        if not is_satisfied:
            logger.debug(f"Executing command: 'pip install \"{req}\"'")
            status_code = execute_shell_command(f'pip install "{req}"')
            if status_code == 0:
                installed.append(str(req))

    return installed


def uninstall_requirements(requirements):
    """Uninstall any requirements that were added to the venv"""
    print("\n\n\n=== (Uninstalling) ===")
    logger.info(
        "Uninstalling packages that were installed while running this script..."
    )
    for req in requirements:
        logger.debug(f"Executing command: 'pip uninstall -y \"{req}\"'")
        execute_shell_command(f'pip uninstall -y "{req}"')


def get_expectation_instances(expectations_info):
    """Return a dict of Expectation instances

    Any contrib Expectations must be imported so they appear in the Expectation registry
    """
    import great_expectations

    expectation_instances = {}
    for expectation_name in expectations_info:
        sys_path = expectations_info[expectation_name]["sys_path"]
        if sys_path and sys_path not in sys.path:
            sys.path.append(sys_path)
        import_module_args = expectations_info[expectation_name]["import_module_args"]
        if import_module_args:
            try:
                importlib.import_module(*import_module_args)
            except (ModuleNotFoundError, ImportError, Exception):
                logger.error(f"Failed to load expectation_name: {expectation_name}")
                print(traceback.format_exc())
                expectation_tracebacks.write(
                    f"\n\n----------------\n{expectation_name} ({expectations_info[expectation_name]['package']})\n"
                )
                expectation_tracebacks.write(traceback.format_exc())
                continue

        try:
            expectation_instances[
                expectation_name
            ] = great_expectations.expectations.registry.get_expectation_impl(
                expectation_name
            )()
        except ExpectationNotFoundError:
            logger.error(
                f"Failed to get Expectation implementation from registry: {expectation_name}"
            )
            print(traceback.format_exc())
            expectation_tracebacks.write(
                f"\n\n----------------\n{expectation_name} ({expectations_info[expectation_name]['package']})\n"
            )
            expectation_tracebacks.write(traceback.format_exc())
            continue
    return expectation_instances


def combine_backend_results(
    expectations_info, expectation_instances, diagnostic_objects, outfile_name
):
    expected_full_backend_files = [
        f"{backend}_full.json" for backend in ALL_GALLERY_BACKENDS
    ]
    found_full_backend_files = glob("*_full.json")

    if sorted(found_full_backend_files) == sorted(expected_full_backend_files):
        logger.info(
            f"All expected *_full.json files were found. Going to combine and write to {outfile_name}"
        )

        bad_key_names = []
        for fname in found_full_backend_files:
            with open(fname) as fp:
                text = fp.read()
            data = json.loads(text)

            for expectation_name in data:
                try:
                    expectations_info[expectation_name][
                        "backend_test_result_counts"
                    ].extend(data[expectation_name]["backend_test_result_counts"])
                except KeyError:
                    expectations_info[expectation_name][
                        "backend_test_result_counts"
                    ] = data[expectation_name]["backend_test_result_counts"]

        # Re-calculate maturity_checklist, library_metadata, and coverage_score
        for expectation_name in expectations_info:
            try:
                diagnostic_object = diagnostic_objects[expectation_name]
            except KeyError:
                logger.error(f"No diagnostic obj for {expectation_name}")
                bad_key_names.append(expectation_name)
                continue
            expectation_instance = expectation_instances[expectation_name]
            try:
                backend_test_result_counts_object = [
                    ExpectationBackendTestResultCounts(**backend_results)
                    for backend_results in expectations_info[expectation_name][
                        "backend_test_result_counts"
                    ]
                ]
            except KeyError:
                logger.error(f"No backend_test_result_counts for {expectation_name}")
                bad_key_names.append(expectation_name)
                continue
            maturity_checklist_object = expectation_instance._get_maturity_checklist(
                library_metadata=diagnostic_object.library_metadata,
                description=diagnostic_object.description,
                examples=diagnostic_object.examples,
                tests=diagnostic_object.tests,
                backend_test_result_counts=backend_test_result_counts_object,
            )
            expectations_info[expectation_name][
                "maturity_checklist"
            ] = maturity_checklist_object.to_dict()
            expectations_info[expectation_name][
                "coverage_score"
            ] = Expectation._get_coverage_score(
                backend_test_result_counts=backend_test_result_counts_object,
                execution_engines=diagnostic_object.execution_engines,
            )
            expectations_info[expectation_name]["library_metadata"][
                "maturity"
            ] = Expectation._get_final_maturity_level(
                maturity_checklist=maturity_checklist_object
            )

        for bad_key_name in bad_key_names:
            expectations_info.pop(bad_key_name)
        with open(f"./{outfile_name}", "w") as outfile:
            json.dump(expectations_info, outfile, indent=4)


def get_contrib_requirements(filepath: str) -> Dict:
    """
    Parse the python file from filepath to identify a "library_metadata" dictionary in any defined classes, and return a requirements_info object that includes a list of pip-installable requirements for each class that defines them.

    Note, currently we are handling all dependencies at the module level. To support future expandability and detail, this method also returns per-class requirements in addition to the concatenated list.

    Args:
        filepath: the path to the file to parse and analyze

    Returns:
        A dictionary:
        {
            "requirements": [ all_requirements_found_in_any_library_metadata_in_file ],
            class_name: [ requirements ]
        }

    """
    with open(filepath) as file:
        tree = ast.parse(file.read())

    requirements_info = {"requirements": []}
    for child in ast.iter_child_nodes(tree):
        if not isinstance(child, ast.ClassDef):
            continue
        current_class = child.name
        for node in ast.walk(child):
            if isinstance(node, ast.Assign):
                try:
                    target_ids = [target.id for target in node.targets]
                except (ValueError, AttributeError):
                    # some assignment types assign to non-node objects (e.g. Tuple)
                    target_ids = []
                if "library_metadata" in target_ids:
                    library_metadata = ast.literal_eval(node.value)
                    requirements = library_metadata.get("requirements", [])
                    if type(requirements) == str:
                        requirements = [requirements]
                    requirements_info[current_class] = requirements
                    requirements_info["requirements"] += requirements

    return requirements_info


def build_gallery(  # noqa: C901 - 17
    only_combine: bool = False,
    include_core: bool = True,
    include_contrib: bool = True,
    ignore_suppress: bool = False,
    ignore_only_for: bool = False,
    outfile_name: str = "",
    only_these_expectations: List[str] | None = None,
    only_consider_these_backends: List[str] | None = None,
    context: Optional[DataContext] = None,
) -> None:
    """
    Build the gallery object by running diagnostics for each Expectation and returning the resulting reports.

    Args:
        only_combine: if true, only combine the various backend_full.json files
        include_core: if true, include Expectations defined in the core module
        include_contrib: if true, include Expectations defined in contrib:
        only_these_expectations: list of specific Expectations to include
        only_consider_these_backends: list of backends to consider running tests against

    Returns:
        None

    """
    if only_these_expectations is None:
        only_these_expectations = []
    if only_consider_these_backends is None:
        only_consider_these_backends = []

    if only_combine:
        include_core = True
        include_contrib = True
        ignore_suppress = False
        ignore_only_for = False
        only_these_expectations = []
        only_consider_these_backends = ["sqlite"]
    backend_outfile_suffix = "partial"
    if include_core and include_contrib and not only_these_expectations:
        backend_outfile_suffix = "full"
    if ignore_suppress or ignore_only_for:
        backend_outfile_suffix += "_nonstandard"
    if only_consider_these_backends:
        only_consider_these_backends = [
            backend
            for backend in only_consider_these_backends
            if backend in ALL_GALLERY_BACKENDS
        ]
    else:
        only_consider_these_backends = list(ALL_GALLERY_BACKENDS)

    gallery_info_by_backend = {backend: {} for backend in only_consider_these_backends}
    diagnostic_objects = {}

    expectations_info = get_expectations_info_dict(
        include_core=include_core,
        include_contrib=include_contrib,
        only_these_expectations=only_these_expectations,
    )

    # Install any requirements needed by selected contrib Expectations
    contrib_requirements_set = set()
    for _info in expectations_info.values():
        contrib_requirements_set.update(_info["requirements"])
    _ = install_necessary_requirements(list(contrib_requirements_set))

    # Get Expectation instances and run diagnostics
    expectation_instances = get_expectation_instances(expectations_info)
    for expectation_name, expectation_instance in sorted(expectation_instances.items()):
        logger.debug(f"Running diagnostics for expectation_name: {expectation_name}")
        try:
            diagnostics = expectation_instance.run_diagnostics(
                ignore_suppress=ignore_suppress,
                ignore_only_for=ignore_only_for,
                for_gallery=True,
                debug_logger=logger,
                only_consider_these_backends=only_consider_these_backends,
                context=context,
            )
            diagnostic_objects[expectation_name] = diagnostics
            checklist_string = diagnostics.generate_checklist()
            expectation_checklists.write(
                f"\n\n----------------\n{expectation_name} ({expectations_info[expectation_name]['package']})\n"
            )
            expectation_checklists.write(f"{checklist_string}\n")
            if diagnostics["description"]["docstring"]:
                expectation_docstrings.write(
                    "\n\n"
                    + "=" * 80
                    + f"\n\n{expectation_name} ({expectations_info[expectation_name]['package']})\n"
                )
                expectation_docstrings.write(
                    f"{diagnostics['description']['docstring']}\n"
                )
                diagnostics["description"]["docstring"] = format_docstring_to_markdown(
                    diagnostics["description"]["docstring"]
                )
                expectation_docstrings.write("\n" + "." * 80 + "\n\n")
                expectation_docstrings.write(
                    f"{diagnostics['description']['docstring']}\n"
                )
        except Exception:
            logger.error(f"Failed to run diagnostics for: {expectation_name}")
            print(traceback.format_exc())
            expectation_tracebacks.write(
                f"\n\n----------------\n{expectation_name} ({expectations_info[expectation_name]['package']})\n"
            )
            expectation_tracebacks.write(traceback.format_exc())
        else:
            try:
                diagnostics_json_dict = diagnostics.to_json_dict()

                # Some items need to be recalculated when {backend}_full.json files get combined
                backend_test_result_counts = diagnostics_json_dict.pop(
                    "backend_test_result_counts"
                )
                diagnostics_json_dict.pop("maturity_checklist")
                diagnostics_json_dict.pop("coverage_score")

                # Some items just need to be popped off
                #   - the attributes from the actual diagnostics object will be used for
                #     regenerating the checklist
                diagnostics_json_dict.pop("tests")
                diagnostics_json_dict.pop("examples")

                # Add non-backend/test specific info from diagnostics to the in-memory dict
                expectations_info[expectation_name].update(diagnostics_json_dict)

                # Add test results to gallery_info_by_backend
                for test_result_counts in backend_test_result_counts:
                    _backend = test_result_counts["backend"]
                    gallery_info_by_backend[_backend][expectation_name] = {
                        "backend_test_result_counts": [test_result_counts],
                    }

            except TypeError:
                logger.error(f"Failed to create JSON for: {expectation_name}")
                print(traceback.format_exc())
                expectation_tracebacks.write(
                    f"\n\n----------------\n[JSON write fail] {expectation_name} ({expectations_info[expectation_name]['package']})\n"
                )
                expectation_tracebacks.write(traceback.format_exc())

    # Iterate the gallery_info_by_backend dict and write the backend-specific files
    for _backend in gallery_info_by_backend:
        if gallery_info_by_backend[_backend]:
            with open(f"{_backend}_{backend_outfile_suffix}.json", "w") as outfile:
                json.dump(gallery_info_by_backend[_backend], outfile, indent=4)

    # Only attempt to combine and write to file when no Expectations are skipped
    if backend_outfile_suffix == "full":
        combine_backend_results(
            expectations_info, expectation_instances, diagnostic_objects, outfile_name
        )


def format_docstring_to_markdown(docstr: str) -> str:
    """
    Add markdown formatting to a provided docstring

    Args:
        docstr: the original docstring that needs to be converted to markdown.

    Returns:
        str of Docstring formatted as markdown

    """
    r = re.compile(r"\s\s+", re.MULTILINE)
    clean_docstr_list = []
    prev_line = None
    in_code_block = False
    in_param = False
    first_code_indentation = None

    # Parse each line to determine if it needs formatting
    for original_line in docstr.split("\n"):
        # Remove excess spaces from lines formed by concatenated docstring lines.
        line = r.sub(" ", original_line)
        # In some old docstrings, this indicates the start of an example block.
        if line.strip() == "::":
            in_code_block = True
            clean_docstr_list.append("```")

        # All of our parameter/arg/etc lists start after a line ending in ':'.
        elif line.strip().endswith(":"):
            in_param = True
            # This adds a blank line before the header if one doesn't already exist.
            if prev_line != "":
                clean_docstr_list.append("")
            # Turn the line into an H4 header
            clean_docstr_list.append(f"#### {line.strip()}")
        elif line.strip() == "" and prev_line != "::":
            # All of our parameter groups end with a line break, but we don't want to exit a parameter block due to a
            # line break in a code block.  However, some code blocks start with a blank first line, so we want to make
            # sure we aren't immediately exiting the code block (hence the test for '::' on the previous line.
            in_param = False
            # Add the markdown indicator to close a code block, since we aren't in one now.
            if in_code_block:
                clean_docstr_list.append("```")
            in_code_block = False
            first_code_indentation = None
            clean_docstr_list.append(line)
        else:  # noqa: PLR5501
            if in_code_block:
                # Determine the number of spaces indenting the first line of code so they can be removed from all lines
                # in the code block without wrecking the hierarchical indentation levels of future lines.
                if first_code_indentation is None and line.strip() != "":
                    first_code_indentation = len(
                        re.match(r"\s*", original_line, re.UNICODE).group(0)
                    )
                if line.strip() == "" and prev_line == "::":
                    # If the first line of the code block is a blank one, just skip it.
                    pass
                else:
                    # Append the line of code, minus the extra indentation from being written in an indented docstring.
                    clean_docstr_list.append(original_line[first_code_indentation:])
            elif ":" in line.replace(":ref:", "") and in_param:
                # This indicates a parameter. arg. or other definition.
                clean_docstr_list.append(f"- {line.strip()}")
            else:
                # This indicates a regular line of text.
                clean_docstr_list.append(f"{line.strip()}")
        prev_line = line.strip()
    clean_docstr = "\n".join(clean_docstr_list)
    return clean_docstr


def _disable_progress_bars() -> Tuple[str, DataContext]:
    """Return context_dir and context that was created"""
    context_dir = os.path.join(  # noqa: PTH118
        os.path.sep, "tmp", f"gx-context-{os.getpid()}"
    )
    os.makedirs(context_dir)  # noqa: PTH103
    context = DataContext.create(context_dir, usage_statistics_enabled=False)
    context.variables.progress_bars = {
        "globally": False,
        "metric_calculations": False,
        "profilers": False,
    }
    context.variables.save_config()
    return (context_dir, context)


@click.command()
@click.option(
    "--only-combine",
    "only_combine",
    is_flag=True,
    default=False,
    help="Generate sqlite_full.json and combine data from other *_full.json files to outfile_name",
)
@click.option(
    "--no-core",
    "-C",
    "no_core",
    is_flag=True,
    default=False,
    help="Do not include core Expectations",
)
@click.option(
    "--no-contrib",
    "-c",
    "no_contrib",
    is_flag=True,
    default=False,
    help="Do not include contrib/package Expectations",
)
@click.option(
    "--ignore-suppress",
    "-S",
    "ignore_suppress",
    is_flag=True,
    default=False,
    help="Ignore the suppress_test_for list on Expectation sample tests",
)
@click.option(
    "--ignore-only-for",
    "-O",
    "ignore_only_for",
    is_flag=True,
    default=False,
    help="Ignore the only_for list on Expectation sample tests",
)
@click.option(
    "--outfile-name",
    "-o",
    "outfile_name",
    default="expectation_library_v2--staging.json",
    help="Name for the generated JSON file assembled from full backend files (no partials)",
)
@click.option(
    "--backends",
    "-b",
    "backends",
    help=(
        "Comma-separated names of backends (in a single string) to consider "
        "running tests against (bigquery, mssql, mysql, pandas, postgresql, "
        "redshift, snowflake, spark, sqlite, trino)"
    ),
)
@click.argument("args", nargs=-1)
def main(**kwargs):
    """Find Expectations, run their diagnostics methods, and generate JSON files with test result summaries for each backend

    - args: snake_name of specific Expectations to include (useful for testing)

    By default, all core and contrib Expectations are found and tested against
    every backend that can be connected to. If any specific Expectation names
    are passed in, only those Expectations will be tested.

    If all Expectations are included and there are no test running modifiers
    specified, the JSON files with tests result summaries will have the "full"
    suffix. If test running modifiers are specified (--ignore-suppress or
    --ignore-only-for), the JSON files will have the "nonstandard" suffix. If
    any Expectations are excluded, the JSON files will have the "partial"
    suffix.

    If all {backend}_full.json files are present and the --only-combine option
    is used, then the complete JSON file for the expectation gallery (including
    a lot of metadata for each Expectation) will be written to outfile_name
    (default: expectation_library_v2--staging.json).

    If running locally (i.e. not in CI), you can run docker containers for
    mssql, mysql, postgresql, and trino. Simply navigate to
    assets/docker/{backend} and run `docker-compose up -d`
    """
    backends = []
    if kwargs["backends"]:
        backends = [name.strip() for name in kwargs["backends"].split(",")]

    # context_dir, context = _disable_progress_bars()
    context = None

    build_gallery(
        only_combine=kwargs["only_combine"],
        include_core=not kwargs["no_core"],
        include_contrib=not kwargs["no_contrib"],
        ignore_suppress=kwargs["ignore_suppress"],
        ignore_only_for=kwargs["ignore_only_for"],
        outfile_name=kwargs["outfile_name"],
        only_these_expectations=kwargs["args"],
        only_consider_these_backends=backends,
        context=context,
    )
    tracebacks = expectation_tracebacks.getvalue()
    checklists = expectation_checklists.getvalue()
    docstrings = expectation_docstrings.getvalue()
    if tracebacks:
        with open("./gallery-tracebacks.txt", "w") as outfile:
            outfile.write(tracebacks)
    if checklists:
        with open("./checklists.txt", "w") as outfile:
            outfile.write(checklists)
    if docstrings:
        with open("./docstrings.txt", "w") as outfile:
            outfile.write(docstrings)

    # print(f"Deleting {context_dir}")
    # shutil.rmtree(context_dir)


if __name__ == "__main__":
    main()
