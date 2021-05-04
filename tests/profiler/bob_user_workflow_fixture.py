from typing import List

import pytest


@pytest.fixture
def bob_columnar_table_multi_batch():

    # TODO: this config specifies a class_name and module_name for the Rule class which is currently not configurable
    #  please decide whether Rule class is configurable before considering this a finalized configuration.

    with open("bob_user_workflow_verbose_profiler_config.yml") as f:
        verbose_profiler_config = f.read()

    profiler_configs: List[str] = []
    profiler_configs.append(verbose_profiler_config)

    return {
        "profiler_configs": profiler_configs
    }
