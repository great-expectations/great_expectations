import os
import shutil

import pytest

import great_expectations as ge
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context import BaseDataContext, DataContext
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context.util import PasswordMasker, file_relative_path

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


# def test_instantiation(titanic_multibatch_data_context):
#    my_context = titanic_multibatch_data_context


def test_abstract_instantiation(basic_in_memory_data_context_config_just_stores):
    context = BaseDataContext(
        project_config=basic_in_memory_data_context_config_just_stores
    )
    assert context
