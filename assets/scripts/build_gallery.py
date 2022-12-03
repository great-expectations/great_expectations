import ast
import importlib
import json
import logging
import os
import re
import shutil
import sys
import traceback
from glob import glob
from io import StringIO
from subprocess import CalledProcessError, CompletedProcess, check_output, run
from typing import Dict, List, Optional, Tuple

import click
import pkg_resources

from great_expectations.data_context.data_context import DataContext

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


def execute_shell_command(command: str) -> int:
    """
    Wrap subprocess command in a try/except block to provide a convenient method for pip installing dependencies.

    :param command: bash command -- as if typed in a shell/Terminal window
    :return: status code -- 0 if successful; all other values (1 is the most common) indicate an error
    """
    cwd: str = os.getcwd()

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
            text=None,
            env=env,
            universal_newlines=True,
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


def get_expectation_file_info_dict(
    include_core: bool = True,
    include_contrib: bool = True,
    only_these_expectations: List[str] = [],
) -> dict:
    rx = re.compile(r".*?([A-Za-z]+?Expectation\b).*")
    result = {}
    files_found = []
    oldpwd = os.getcwd()
    os.chdir(f"..{os.path.sep}..")
    repo_path = os.getcwd()
    logger.debug(
        "Finding Expectation files in the repo and getting their create/update times"
    )

    if include_core:
        files_found.extend(
            glob(
                os.path.join(
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
                os.path.join(repo_path, "contrib", "**", "expect_*.py"),
                recursive=True,
            )
        )

    for file_path in sorted(files_found):
        file_path = file_path.replace(f"{repo_path}{os.path.sep}", "")
        package_name = os.path.basename(os.path.dirname(os.path.dirname(file_path)))
        if package_name == "expectations":
            package_name = "core"
        name = os.path.basename(file_path).replace(".py", "")
        if only_these_expectations and name not in only_these_expectations:
            continue

        updated_at_cmd = f'git log -1 --format="%ai %ar" -- {repr(file_path)}'
        created_at_cmd = (
            f'git log --diff-filter=A --format="%ai %ar" -- {repr(file_path)}'
        )
        result[name] = {
            "updated_at": check_output(updated_at_cmd, shell=True)
            .decode("utf-8")
            .strip(),
            "created_at": check_output(created_at_cmd, shell=True)
            .decode("utf-8")
            .strip(),
            "path": file_path,
            "package": package_name,
        }
        logger.debug(
            f"{name} ({package_name}) was created {result[name]['created_at']} and updated {result[name]['updated_at']}"
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
        result[name]["exp_type"] = _prefix + sorted(exp_type_set)[0]
        logger.debug(
            f"Expectation type {_prefix}{sorted(exp_type_set)[0]} for {name} in {file_path}"
        )

    os.chdir(oldpwd)
    return result


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


def build_gallery(
    include_core: bool = True,
    include_contrib: bool = True,
    ignore_suppress: bool = False,
    ignore_only_for: bool = False,
    only_these_expectations: List[str] = [],
    only_consider_these_backends: List[str] = [],
    context: Optional[DataContext] = None,
) -> Dict:
    """
    Build the gallery object by running diagnostics for each Expectation and returning the resulting reports.

    Args:
        include_core: if true, include Expectations defined in the core module
        include_contrib: if true, include Expectations defined in contrib:
        only_these_expectations: list of specific Expectations to include
        only_consider_these_backends: list of backends to consider running tests against

    Returns:
        None

    """
    gallery_info = dict()
    requirements_dict = {}
    logger.info("Loading great_expectations library.")
    installed_packages = pkg_resources.working_set
    installed_packages_txt = sorted(f"{i.key}=={i.version}" for i in installed_packages)
    logger.debug(f"Found the following packages: {installed_packages_txt}")

    expectation_file_info = get_expectation_file_info_dict(
        include_core=include_core,
        include_contrib=include_contrib,
        only_these_expectations=only_these_expectations,
    )
    import great_expectations

    core_expectations = (
        great_expectations.expectations.registry.list_registered_expectation_implementations()
    )
    if include_core:
        print("\n\n\n=== (Core) ===")
        logger.info("Getting base registered expectations list")
        logger.debug(f"Found the following expectations: {sorted(core_expectations)}")
        for expectation in core_expectations:
            if only_these_expectations and expectation not in only_these_expectations:
                # logger.debug(f"Skipping {expectation} since it's not requested")
                continue
            requirements_dict[expectation] = {"group": "core"}

    just_installed = set()
    failed_to_import_set = set()

    if include_contrib:
        print("\n\n\n=== (Contrib) ===")
        logger.info("Finding contrib modules")
        skip_dirs = ("cli", "tests")
        contrib_dir = os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "contrib",
        )

        for root, dirs, files in os.walk(contrib_dir):
            for dirname in skip_dirs:
                if dirname in dirs:
                    dirs.remove(dirname)
            if "expectations" in dirs:
                if root.endswith("great_expectations_experimental"):
                    sys.path.append(root)
                else:
                    # A package in contrib that may contain more Expectations
                    sys.path.append(os.path.dirname(root))
            for filename in files:
                if filename.endswith(".py") and filename.startswith("expect_"):
                    if (
                        only_these_expectations
                        and filename.replace(".py", "") not in only_these_expectations
                    ):
                        # logger.debug(f"Skipping {filename} since it's not requested")
                        continue
                    logger.debug(f"Getting requirements for module {filename}")
                    contrib_subdir_name = os.path.basename(os.path.dirname(root))
                    requirements_dict[filename[:-3]] = get_contrib_requirements(
                        os.path.join(root, filename)
                    )
                    requirements_dict[filename[:-3]]["group"] = contrib_subdir_name
        logger.info("Done finding contrib modules")

    for expectation in sorted(requirements_dict):
        # Temp
        if expectation in [
            "expect_column_kl_divergence_to_be_less_than",  # Infinity values break JSON
            "expect_column_values_to_be_valid_arn",  # Contrib Expectation where pretty much no test passes on any backend
        ]:
            continue
        group = requirements_dict[expectation]["group"]
        print(f"\n\n\n=== {expectation} ({group}) ===")
        requirements = requirements_dict[expectation].get("requirements", [])
        parsed_requirements = pkg_resources.parse_requirements(requirements)
        for req in parsed_requirements:
            is_satisfied = any(
                [installed_pkg in req for installed_pkg in installed_packages]
            )
            if is_satisfied or req in just_installed:
                continue
            logger.debug(f"Executing command: 'pip install \"{req}\"'")
            status_code = execute_shell_command(f'pip install "{req}"')
            if status_code == 0:
                just_installed.add(req)
            else:
                expectation_tracebacks.write(
                    f"\n\n----------------\n{expectation} ({group})\n"
                )
                expectation_tracebacks.write(f"Failed to pip install {req}\n\n")

        if group != "core":
            logger.debug(f"Importing {expectation}")
            try:
                if group == "great_expectations_experimental":
                    importlib.import_module(f"expectations.{expectation}", group)
                else:
                    importlib.import_module(f"{group}.expectations")
            except (ModuleNotFoundError, ImportError, Exception) as e:
                logger.error(f"Failed to load expectation: {expectation}")
                print(traceback.format_exc())
                expectation_tracebacks.write(
                    f"\n\n----------------\n{expectation} ({group})\n"
                )
                expectation_tracebacks.write(traceback.format_exc())
                failed_to_import_set.add(expectation)
                continue

        logger.debug(f"Running diagnostics for expectation: {expectation}")
        try:
            impl = great_expectations.expectations.registry.get_expectation_impl(
                expectation
            )
            diagnostics = impl().run_diagnostics(
                ignore_suppress=ignore_suppress,
                ignore_only_for=ignore_only_for,
                debug_logger=logger,
                only_consider_these_backends=only_consider_these_backends,
                context=context,
            )
            checklist_string = diagnostics.generate_checklist()
            expectation_checklists.write(
                f"\n\n----------------\n{expectation} ({group})\n"
            )
            expectation_checklists.write(f"{checklist_string}\n")
            if diagnostics["description"]["docstring"]:
                expectation_docstrings.write(
                    "\n\n" + "=" * 80 + f"\n\n{expectation} ({group})\n"
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
            logger.error(f"Failed to run diagnostics for: {expectation}")
            print(traceback.format_exc())
            expectation_tracebacks.write(
                f"\n\n----------------\n{expectation} ({group})\n"
            )
            expectation_tracebacks.write(traceback.format_exc())
        else:
            try:
                gallery_info[expectation] = diagnostics.to_json_dict()
                gallery_info[expectation]["created_at"] = expectation_file_info[
                    expectation
                ]["created_at"]
                gallery_info[expectation]["updated_at"] = expectation_file_info[
                    expectation
                ]["updated_at"]
                gallery_info[expectation]["package"] = expectation_file_info[
                    expectation
                ]["package"]
                gallery_info[expectation]["exp_type"] = expectation_file_info[
                    expectation
                ].get("exp_type")
            except TypeError as e:
                logger.error(f"Failed to create JSON for: {expectation}")
                print(traceback.format_exc())
                expectation_tracebacks.write(
                    f"\n\n----------------\n[JSON write fail] {expectation} ({group})\n"
                )
                expectation_tracebacks.write(traceback.format_exc())

    if just_installed:
        print("\n\n\n=== (Uninstalling) ===")
        logger.info(
            f"Uninstalling packages that were installed while running this script..."
        )
        for req in just_installed:
            logger.debug(f"Executing command: 'pip uninstall -y \"{req}\"'")
            execute_shell_command(f'pip uninstall -y "{req}"')

    expectation_filenames_set = set(requirements_dict.keys())
    full_registered_expectations_set = set(
        great_expectations.expectations.registry.list_registered_expectation_implementations()
    )
    if only_these_expectations:
        registered_expectations_set = (
            set(only_these_expectations) & full_registered_expectations_set
        )
        expectation_filenames_set = (
            set(only_these_expectations) & expectation_filenames_set
        )
    elif not include_core:
        registered_expectations_set = full_registered_expectations_set - set(
            core_expectations
        )
    else:
        registered_expectations_set = full_registered_expectations_set
    non_matched_filenames = (
        expectation_filenames_set - registered_expectations_set - failed_to_import_set
    )
    if failed_to_import_set:
        expectation_tracebacks.write(f"\n\n----------------\n(Not a traceback)\n")
        expectation_tracebacks.write("Expectations that failed to import:\n")
        for expectation in sorted(failed_to_import_set):
            expectation_tracebacks.write(f"- {expectation}\n")
    if non_matched_filenames:
        expectation_tracebacks.write(f"\n\n----------------\n(Not a traceback)\n")
        expectation_tracebacks.write(
            "Expectation filenames that don't match their defined Expectation name:\n"
        )
        for fname in sorted(non_matched_filenames):
            expectation_tracebacks.write(f"- {fname}\n")

        bad_names = sorted(
            list(registered_expectations_set - expectation_filenames_set)
        )
        expectation_tracebacks.write(
            f"\nRegistered Expectation names that don't match:\n"
        )
        for exp_name in bad_names:
            expectation_tracebacks.write(f"- {exp_name}\n")

    if include_core and not only_these_expectations:
        core_dir = os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "great_expectations",
            "expectations",
            "core",
        )
        core_expectations_filename_set = {
            fname.rsplit(".", 1)[0]
            for fname in os.listdir(core_dir)
            if fname.startswith("expect_")
        }
        core_expectations_not_in_gallery = core_expectations_filename_set - set(
            core_expectations
        )
        if core_expectations_not_in_gallery:
            expectation_tracebacks.write(f"\n\n----------------\n(Not a traceback)\n")
            expectation_tracebacks.write(
                f"Core Expectation files not included in core_expectations:\n"
            )
            for exp_name in sorted(core_expectations_not_in_gallery):
                expectation_tracebacks.write(f"- {exp_name}\n")

    return gallery_info


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
        else:
            if in_code_block:
                # Determine the number of spaces indenting the first line of code so they can be removed from all lines
                # in the code block without wrecking the hierarchical indentation levels of future lines.
                if first_code_indentation == None and line.strip() != "":
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
    context_dir = os.path.join(os.path.sep, "tmp", f"gx-context-{os.getpid()}")
    os.makedirs(context_dir)
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
    help="Name for the generated JSON file",
)
@click.option(
    "--backends",
    "-b",
    "backends",
    help="Backends to consider running tests against (comma-separated)",
)
@click.argument("args", nargs=-1)
def main(**kwargs):
    """Find all Expectations, run their diagnostics methods, and generate expectation_library_v2--staging.json

    - args: snake_name of specific Expectations to include (useful for testing)
    """
    backends = []
    if kwargs["backends"]:
        backends = [name.strip() for name in kwargs["backends"].split(",")]

    context_dir, context = _disable_progress_bars()

    gallery_info = build_gallery(
        include_core=not kwargs["no_core"],
        include_contrib=not kwargs["no_contrib"],
        ignore_suppress=kwargs["ignore_suppress"],
        ignore_only_for=kwargs["ignore_only_for"],
        only_these_expectations=kwargs["args"],
        only_consider_these_backends=backends,
        context=context,
    )
    tracebacks = expectation_tracebacks.getvalue()
    checklists = expectation_checklists.getvalue()
    docstrings = expectation_docstrings.getvalue()
    if tracebacks != "":
        with open("./gallery-tracebacks.txt", "w") as outfile:
            outfile.write(tracebacks)
    if checklists != "":
        with open("./checklists.txt", "w") as outfile:
            outfile.write(checklists)
    if docstrings != "":
        with open("./docstrings.txt", "w") as outfile:
            outfile.write(docstrings)
    with open(f"./{kwargs['outfile_name']}", "w") as outfile:
        json.dump(gallery_info, outfile, indent=4)

    print(f"Deleting {context_dir}")
    shutil.rmtree(context_dir)


if __name__ == "__main__":
    main()
