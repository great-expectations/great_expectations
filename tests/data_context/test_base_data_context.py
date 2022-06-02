import os
from typing import List

import pytest

from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig

from great_expectations.data_context.store import (  # isort:skip
    ExpectationsStore,
    ValidationsStore,
    EvaluationParameterStore,
)

yaml: YAMLHandler = YAMLHandler()


@pytest.fixture()
def basic_in_memory_data_context_config_just_stores():
    return DataContextConfig(
        config_version=2.0,
        plugins_directory=None,
        evaluation_parameter_store_name="evaluation_parameter_store",
        expectations_store_name="expectations_store",
        datasources={},
        stores={
            "expectations_store": {"class_name": "ExpectationsStore"},
            "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
            "validation_result_store": {"class_name": "ValidationsStore"},
        },
        validations_store_name="validation_result_store",
        data_docs_sites={},
        validation_operators={},
    )


@pytest.fixture()
def basic_in_memory_data_context_just_stores(
    basic_in_memory_data_context_config_just_stores,
):
    return BaseDataContext(
        project_config=basic_in_memory_data_context_config_just_stores
    )


# @pytest.fixture(scope="function")
# def titanic_multibatch_data_context(
#     tmp_path,
# ) -> DataContext:
#     """
#     Based on titanic_data_context, but with 2 identical batches of
#     data asset "titanic"
#     """
#     project_path = tmp_path / "titanic_data_context"
#     project_path.mkdir()
#     project_path = str(project_path)
#     context_path = os.path.join(project_path, "great_expectations")
#     os.makedirs(os.path.join(context_path, "expectations"), exist_ok=True)
#     data_path = os.path.join(context_path, "..", "data", "titanic")
#     os.makedirs(os.path.join(data_path), exist_ok=True)
#     shutil.copy(
#         file_relative_path(__file__, "../test_fixtures/great_expectations_titanic.yml"),
#         str(os.path.join(context_path, "great_expectations.yml")),
#     )
#     shutil.copy(
#         file_relative_path(__file__, "../test_sets/Titanic.csv"),
#         str(os.path.join(context_path, "..", "data", "titanic", "Titanic_1911.csv")),
#     )
#     shutil.copy(
#         file_relative_path(__file__, "../test_sets/Titanic.csv"),
#         str(os.path.join(context_path, "..", "data", "titanic", "Titanic_1912.csv")),
#     )
#     return ge.data_context.DataContext(context_path)


def test_instantiation_and_basic_stores(
    basic_in_memory_data_context_just_stores,
    basic_in_memory_data_context_config_just_stores,
):
    context: BaseDataContext = basic_in_memory_data_context_just_stores

    assert context.config == basic_in_memory_data_context_config_just_stores
    assert len(context.stores) == 3

    assert context.expectations_store_name == "expectations_store"
    assert isinstance(context.expectations_store, ExpectationsStore)

    assert context.validations_store_name == "validation_result_store"
    assert isinstance(context.validations_store, ValidationsStore)

    assert context.evaluation_parameter_store_name == "evaluation_parameter_store"
    assert isinstance(context.evaluation_parameter_store, EvaluationParameterStore)

    # TODO : validation_store
    # TODO : checkpoint store
    # TODO : metric store?


def test_config_variables(basic_in_memory_data_context_just_stores):
    # nothing instantiated yet
    assert basic_in_memory_data_context_just_stores.config_variables == {}


def test_list_stores(basic_in_memory_data_context_just_stores):
    assert basic_in_memory_data_context_just_stores.list_stores() == [
        {"class_name": "ExpectationsStore", "name": "expectations_store"},
        {
            "class_name": "EvaluationParameterStore",
            "name": "evaluation_parameter_store",
        },
        {"class_name": "ValidationsStore", "name": "validation_result_store"},
    ]


def test_add_store(basic_in_memory_data_context_just_stores):
    store_name: str = "my_new_expectations_store"
    store_config: dict = {"class_name": "ExpectationsStore"}
    basic_in_memory_data_context_just_stores.add_store(
        store_name=store_name, store_config=store_config
    )
    assert basic_in_memory_data_context_just_stores.list_stores() == [
        {"class_name": "ExpectationsStore", "name": "expectations_store"},
        {
            "class_name": "EvaluationParameterStore",
            "name": "evaluation_parameter_store",
        },
        {"class_name": "ValidationsStore", "name": "validation_result_store"},
        {"class_name": "ExpectationsStore", "name": "my_new_expectations_store"},
    ]


def test_list_active_stores(basic_in_memory_data_context_just_stores):
    """
    Active stores are identified by the following keys:
        expectations_store_name,
        validations_store_name,
        evaluation_parameter_store_name,
        checkpoint_store_name
        profiler_store_name

    Therefore the test also test that the list_active_stores() output doesn't change after a store named
    `my_new_expectations_store` is added
    """
    expected_store_list: List[dict] = [
        {"class_name": "ExpectationsStore", "name": "expectations_store"},
        {
            "class_name": "EvaluationParameterStore",
            "name": "evaluation_parameter_store",
        },
        {"class_name": "ValidationsStore", "name": "validation_result_store"},
    ]
    assert (
        basic_in_memory_data_context_just_stores.list_active_stores()
        == expected_store_list
    )

    store_name: str = "my_new_expectations_store"
    store_config: dict = {"class_name": "ExpectationsStore"}
    basic_in_memory_data_context_just_stores.add_store(
        store_name=store_name, store_config=store_config
    )
    assert (
        basic_in_memory_data_context_just_stores.list_active_stores()
        == expected_store_list
    )


def test_get_config_with_variables_substituted(
    basic_in_memory_data_context_just_stores,
):
    """
    Basic test for get_config_with_variables_substituted()

    A more thorough set of tests exist in test_data_context_config_variables.py with
    test_setting_config_variables_is_visible_immediately() testing whether the os.env values take
    precedence over config_file values, which they should.
    """

    context: BaseDataContext = basic_in_memory_data_context_just_stores
    assert isinstance(context.get_config(), DataContextConfig)

    # override the project config to use the $ escaped variable
    context._project_config["validations_store_name"] = "${replace_me}"
    try:
        # which we set from the environment
        os.environ["replace_me"] = "value_from_env_var"
        assert (
            context.get_config_with_variables_substituted().validations_store_name
            == "value_from_env_var"
        )
    finally:
        del os.environ["replace_me"]
