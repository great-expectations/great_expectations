import logging
import sys
from typing import Dict, List

logger = logging.getLogger(__name__)


def get_imported_packages(log_location="build_init_payload") -> List[Dict]:
    """
    Check for imported packages, get the version info if available.
    Args:
        log_location: Function where this method is called for logging exception e.g. "build_init_payload" -> "build_init_payload: Unable to create pipeline_dag_runners payload field"

    Returns:
        List of strings denoting which dag runners are in use (typically a single item list)
    """

    COMMON_IMPORTS = {
        "pandas",
        "scipy",
        "numpy",
        "jupyter",
    }
    DAG_RUNNER_IMPORTS = {
        "airflow",
        "prefect",
        "dagster",
        "kedro",
        # "argo",  # TODO: Argo is written in Go, how to include?
        "flytekit",
        "ascend",  # ascend python sdk
        # "nifi",  # TODO: How to import?
        # "metaflow",  # TODO: How to import?
    }

    IMPORTS_TO_CHECK = COMMON_IMPORTS + DAG_RUNNER_IMPORTS

    imported_packages = []

    # TODO: This function needs work.

    try:
        imported = [i for i in IMPORTS_TO_CHECK if i in sys.modules.keys()]
        for import_to_check in imported:
            if import_to_check == "pandas":
                try:
                    # TODO: How can I check version without importing??
                    #  use pip? But this is discouraged by pip.
                    import pandas

                    version = pandas.__version__
                    imported_packages.append(
                        {"package": import_to_check, "version": version}
                    )
                except Exception:
                    # Don't add if we have an issue importing
                    pass
            try:
                import_to_add = {
                    "package": import_to_check,
                    # "version": getattr(import_to_check).__version__
                    "version": "0.1.0",
                }
                imported_packages.append(import_to_add)
            except Exception:
                # Don't add if any issues
                pass

    except Exception:
        logger.debug(
            f"{log_location}: Unable to create imported_packages payload field"
        )

    return imported_packages

    # pipeline_dag_runner = ["none_detected"]
    # try:
    #     DAG_RUNNERS = {
    #         "airflow",
    #         "prefect",
    #         "dagster",
    #         "kedro",
    #         # "argo",  # TODO: Argo is written in Go, how to include?
    #         "flytekit",
    #         # "ascend",  # TODO: How to import?
    #         # "nifi",  # TODO: How to import?
    #         # "metaflow",  # TODO: How to import?
    #     }
    #     pipeline_dag_runner_check = [d for d in DAG_RUNNERS if d in sys.modules.keys()]
    #     if pipeline_dag_runner_check != []:
    #         pipeline_dag_runner = pipeline_dag_runner_check
    # except Exception:
    #     logger.debug(
    #         f"{log_location}: Unable to create pipeline_dag_runners payload field"
    #     )
    # return pipeline_dag_runner
