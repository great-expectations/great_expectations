import logging
import os
import shutil

import pytest

import great_expectations as gx
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.util import file_relative_path

pytestmark = pytest.mark.filesystem

logger = logging.getLogger(__name__)


def read_config_file_from_disk(config_filepath):
    with open(config_filepath) as infile:
        config_file = infile.read()
        return config_file


@pytest.fixture
def data_context_parameterized_expectation_suite_with_usage_statistics_enabled(
    tmp_path_factory,
):
    """
    This data_context is *manually* created to have the config we want, vs
    created with DataContext.create()
    """
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, FileDataContext.GX_DIR)  # noqa: PTH118
    asset_config_path = os.path.join(context_path, "expectations")  # noqa: PTH118
    fixture_dir = file_relative_path(__file__, "../test_fixtures")
    os.makedirs(  # noqa: PTH103
        os.path.join(asset_config_path, "my_dag_node"),  # noqa: PTH118
        exist_ok=True,
    )
    shutil.copy(
        os.path.join(  # noqa: PTH118
            fixture_dir, "great_expectations_v013_basic_with_usage_stats_enabled.yml"
        ),
        str(os.path.join(context_path, FileDataContext.GX_YML)),  # noqa: PTH118
    )
    shutil.copy(
        os.path.join(  # noqa: PTH118
            fixture_dir,
            "expectation_suites/parameterized_expectation_suite_fixture.json",
        ),
        os.path.join(asset_config_path, "my_dag_node", "default.json"),  # noqa: PTH118
    )
    os.makedirs(  # noqa: PTH103
        os.path.join(context_path, "plugins"), exist_ok=True  # noqa: PTH118
    )
    shutil.copy(
        os.path.join(fixture_dir, "custom_pandas_dataset.py"),  # noqa: PTH118
        str(
            os.path.join(  # noqa: PTH118
                context_path, "plugins", "custom_pandas_dataset.py"
            )
        ),
    )
    shutil.copy(
        os.path.join(fixture_dir, "custom_sparkdf_dataset.py"),  # noqa: PTH118
        str(
            os.path.join(  # noqa: PTH118
                context_path, "plugins", "custom_sparkdf_dataset.py"
            )
        ),
    )
    return gx.get_context(context_root_dir=context_path)
