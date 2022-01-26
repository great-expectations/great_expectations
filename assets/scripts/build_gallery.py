import ast
import importlib
import json
import logging
import os
import sys
import traceback
from io import StringIO
from subprocess import CalledProcessError, CompletedProcess, run
from typing import Dict

import pkg_resources

logger = logging.getLogger(__name__)
chandler = logging.StreamHandler(stream=sys.stdout)
chandler.setLevel(logging.DEBUG)
chandler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%dT%H:%M:%S")
)
logger.addHandler(chandler)
logger.setLevel(logging.DEBUG)


expectation_tracebacks = StringIO()


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
                    requirements_info[current_class] = requirements
                    requirements_info["requirements"] += requirements

    return requirements_info


def build_gallery(
    include_core: bool = True, include_contrib_experimental: bool = True
) -> Dict:
    """
    Build the gallery object by running diagnostics for each Expectation and returning the resulting reports.

    Args:
        include_core: if true, include Expectations defined in the core module
        include_contrib_experimental: if true, include Expectations defined in contrib_experimental:

    Returns:
        None

    """
    gallery_info = dict()
    built_expectations = set()
    logger.info("Loading great_expectations library.")
    installed_packages = pkg_resources.working_set
    installed_packages_txt = sorted(f"{i.key}=={i.version}" for i in installed_packages)
    logger.debug(f"Found the following packages: {installed_packages_txt}")

    import great_expectations

    logger.info("Getting base registered expectations list")
    core_expectations = (
        great_expectations.expectations.registry.list_registered_expectation_implementations()
    )
    if include_core:
        for expectation in core_expectations:
            logger.debug(f"Running diagnostics for expectation: {expectation}")
            impl = great_expectations.expectations.registry.get_expectation_impl(
                expectation
            )
            try:
                diagnostics = impl().run_diagnostics()
                gallery_info[expectation] = diagnostics.to_json_dict()
                built_expectations.add(expectation)
            except Exception:
                logger.error(f"Failed to run diagnostics for: {expectation}")
                print(traceback.format_exc())
                expectation_tracebacks.write(f"\n\n----------------\n{expectation}\n")
                expectation_tracebacks.write(traceback.format_exc())
    else:
        built_expectations = set(core_expectations)

    if include_contrib_experimental:
        logger.info("Finding contrib modules")
        contrib_experimental_dir = os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "contrib",
            "experimental",
            "great_expectations_experimental",
        )
        sys.path.append(contrib_experimental_dir)
        expectations_module = importlib.import_module(
            "expectations", "great_expectations_experimental"
        )
        requirements_dict = {}
        for root, dirs, files in os.walk(contrib_experimental_dir):
            for file in files:
                if file.endswith(".py") and not file == "__init__.py":
                    logger.debug(f"Getting requirements for module {file}")
                    requirements_dict[file[:-3]] = get_contrib_requirements(
                        os.path.join(root, file)
                    )

        # Use a brute-force approach: install all requirements for each module as we import it
        for expectation_module in expectations_module.__all__:
            just_installed = set()
            if expectation_module in requirements_dict:
                logger.info(f"Loading dependencies for module {expectation_module}")
                requirements = requirements_dict[expectation_module].get(
                    "requirements", []
                )
                parsed_requirements = pkg_resources.parse_requirements(requirements)
                for req in parsed_requirements:
                    is_satisfied = any(
                        [installed_pkg in req for installed_pkg in installed_packages]
                    )
                    if is_satisfied:
                        continue
                    just_installed.add(req)
                    logger.debug(f"Executing command: 'pip install \"{req}\"'")
                    execute_shell_command(f'pip install "{req}"')
            logger.debug(f"Importing {expectation_module}")
            importlib.import_module(
                f"expectations.{expectation_module}", "great_expectations_experimental"
            )
            available_expectations = (
                great_expectations.expectations.registry.list_registered_expectation_implementations()
            )
            new_expectations = set(available_expectations) - built_expectations
            for expectation in new_expectations:
                logger.debug(f"Running diagnostics for expectation: {expectation}")
                impl = great_expectations.expectations.registry.get_expectation_impl(
                    expectation
                )
                try:
                    diagnostics = impl().run_diagnostics()
                    gallery_info[expectation] = diagnostics.to_json_dict()
                    built_expectations.add(expectation)
                except Exception:
                    logger.error(f"Failed to run diagnostics for: {expectation}")
                    print(traceback.format_exc())
                    expectation_tracebacks.write(
                        f"\n\n----------------\n{expectation}\n"
                    )
                    expectation_tracebacks.write(traceback.format_exc())

            logger.info(f"Unloading just-installed for module {expectation_module}")
            for req in just_installed:
                logger.debug(f"Executing command: 'pip uninstall -y \"{req}\"'")
                execute_shell_command(f'pip uninstall -y "{req}"')

        metrics_module = importlib.import_module(
            "metrics", "great_expectations_experimental"
        )
        for metrics_module in metrics_module.__all__:
            if metrics_module in requirements_dict:
                logger.warning(
                    f"Independent metrics module {metrics_module} not being processed."
                )

    return gallery_info


if __name__ == "__main__":
    gallery_info = build_gallery(include_core=True, include_contrib_experimental=True)
    tracebacks = expectation_tracebacks.getvalue()
    if tracebacks != "":
        print(tracebacks)
    with open("./expectation_library.json", "w") as outfile:
        json.dump(gallery_info, outfile)
