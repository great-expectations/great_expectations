from typing import List

import pytest


@pytest.fixture
def bob_columnar_table_multi_batch():

    with open("bob_user_workflow_verbose_profiler_config.yml") as f:
        verbose_profiler_config = f.read()

    profiler_configs: List[str] = []
    profiler_configs.append(verbose_profiler_config)

    return {
        "profiler_configs": profiler_configs
    }
