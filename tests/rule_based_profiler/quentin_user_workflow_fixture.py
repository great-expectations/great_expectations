import pytest

# TODO: Move these fixtures to integration tests
from great_expectations.data_context.util import file_relative_path


@pytest.fixture
def quentin_columnar_table_multi_batch():
    verbose_profiler_config_file_path: str = file_relative_path(
        __file__, "quentin_user_workflow_verbose_profiler_config.yml"
    )
    verbose_profiler_config: str
    with open(verbose_profiler_config_file_path) as f:
        verbose_profiler_config = f.read()

    expectation_suite_name_bootstrap_sampling_method: str = (
        "quentin_columnar_table_multi_batch"
    )

    return {
        "profiler_config": verbose_profiler_config,
        "test_configuration": {
            "expectation_suite_name": expectation_suite_name_bootstrap_sampling_method,
        },
    }
