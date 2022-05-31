import json
import os
import shutil
from collections import OrderedDict

import pandas as pd
import pytest
from freezegun import freeze_time

import great_expectations as ge
import great_expectations.exceptions as ge_exceptions
from great_expectations.checkpoint import Checkpoint, SimpleCheckpoint
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core import ExpectationConfiguration, expectationSuiteSchema
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.config_peer import ConfigOutputModes
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.data_context import (
    BaseDataContext,
    DataContext,
    ExplorerDataContext,
)
from great_expectations.data_context.store import ExpectationsStore
from great_expectations.data_context.types.base import (
    CheckpointConfig,
    DataContextConfig,
    DataContextConfigDefaults,
    DatasourceConfig,
)
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    ExpectationSuiteIdentifier,
)
from great_expectations.data_context.util import PasswordMasker, file_relative_path
from great_expectations.dataset import Dataset
from great_expectations.datasource import (
    Datasource,
    LegacyDatasource,
    SimpleSqlalchemyDatasource,
)
from great_expectations.datasource.types.batch_kwargs import PathBatchKwargs
from great_expectations.util import (
    deep_filter_properties_iterable,
    gen_directory_tree_str,
    is_library_loadable,
)
from tests.test_utils import create_files_in_directory, safe_remove

try:
    from unittest import mock
except ImportError:
    from unittest import mock

from great_expectations.core.yaml_handler import YAMLHandler

yaml: YAMLHandler = YAMLHandler()


@pytest.fixture()
def basic_in_memory_data_context_config_just_stores():
    return DataContextConfig(
        config_version=2,
        plugins_directory=None,
        evaluation_parameter_store_name="evaluation_parameter_store",
        expectations_store_name="expectations_store",
        datasources={},
        stores={
            "expectations_store": {"class_name": "ExpectationsStore"},
            "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
            "validation_result_store": {"class_name": "ValidationsStore"},
            "metrics_store": {"class_name": "MetricStore"},
        },
        validations_store_name="validation_result_store",
        data_docs_sites={},
        validation_operators={},
    )


@pytest.fixture(scope="function")
def titanic_multibatch_data_context(
    tmp_path,
) -> DataContext:
    """
    Based on titanic_data_context, but with 2 identical batches of
    data asset "titanic"
    """
    project_path = tmp_path / "titanic_data_context"
    project_path.mkdir()
    project_path = str(project_path)
    context_path = os.path.join(project_path, "great_expectations")
    os.makedirs(os.path.join(context_path, "expectations"), exist_ok=True)
    data_path = os.path.join(context_path, "..", "data", "titanic")
    os.makedirs(os.path.join(data_path), exist_ok=True)
    shutil.copy(
        file_relative_path(__file__, "../test_fixtures/great_expectations_titanic.yml"),
        str(os.path.join(context_path, "great_expectations.yml")),
    )
    shutil.copy(
        file_relative_path(__file__, "../test_sets/Titanic.csv"),
        str(os.path.join(context_path, "..", "data", "titanic", "Titanic_1911.csv")),
    )
    shutil.copy(
        file_relative_path(__file__, "../test_sets/Titanic.csv"),
        str(os.path.join(context_path, "..", "data", "titanic", "Titanic_1912.csv")),
    )
    return ge.data_context.DataContext(context_path)


def test_instantiation(titanic_multibatch_data_context):
    my_context = titanic_multibatch_data_context


def test_abstract_instantiation(basic_in_memory_data_context_config_just_stores):
    context = BaseDataContext(
        project_config=basic_in_memory_data_context_config_just_stores
    )
