import logging
import sys
from typing import List

logger = logging.getLogger(__name__)


def get_pipeline_dag_runners(log_location="build_init_payload") -> List[str]:
    """
    Determine if any of the common dag runners are imported into the current environment.
    Args:
        log_location: Function where this method is called for logging exception e.g. "build_init_payload" -> "build_init_payload: Unable to create pipeline_dag_runners payload field"

    Returns:
        List of strings denoting which dag runners are in use (typically a single item list)
    """
    pipeline_dag_runner = ["none_detected"]
    try:
        DAG_RUNNERS = {
            "airflow",
            "prefect",
            "dagster",
            "kedro",
            # "argo",  # TODO: Argo is written in Go, how to include?
            # "flytekit",  # TODO: Check import name
            # "ascend",  # TODO: How to import?
            # "nifi",  # TODO: How to import?
            # "metaflow",  # TODO: How to import?
        }
        pipeline_dag_runner_check = [d for d in DAG_RUNNERS if d in sys.modules.keys()]
        if pipeline_dag_runner_check != []:
            pipeline_dag_runner = pipeline_dag_runner_check
    except Exception:
        logger.debug(
            f"{log_location}: Unable to create pipeline_dag_runners payload field"
        )
    return pipeline_dag_runner
