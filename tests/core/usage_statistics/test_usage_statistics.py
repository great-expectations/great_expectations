import configparser
import os
import shutil
from copy import deepcopy
from unittest import mock

import pytest

from great_expectations.core.usage_statistics.usage_statistics import (
    run_validation_operator_usage_statistics,
)
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context.util import file_relative_path
from great_expectations.util import get_context
from tests.integration.usage_statistics.test_integration_usage_statistics import (
    USAGE_STATISTICS_QA_URL,
)


@pytest.fixture
def in_memory_data_context_config_usage_stats_enabled():
    return DataContextConfig(
        **{
            "commented_map": {},
            "config_version": 2,
            "plugins_directory": None,
            "evaluation_parameter_store_name": "evaluation_parameter_store",
            "validations_store_name": "validations_store",
            "expectations_store_name": "expectations_store",
            "config_variables_file_path": None,
            "datasources": {},
            "stores": {
                "expectations_store": {
                    "class_name": "ExpectationsStore",
                },
                "validations_store": {
                    "class_name": "ValidationsStore",
                },
                "evaluation_parameter_store": {
                    "class_name": "EvaluationParameterStore",
                },
            },
            "data_docs_sites": {},
            "validation_operators": {
                "default": {
                    "class_name": "ActionListValidationOperator",
                    "action_list": [],
                }
            },
            "anonymous_usage_statistics": {
                "enabled": True,
                "data_context_id": "00000000-0000-0000-0000-000000000001",
                "usage_statistics_url": USAGE_STATISTICS_QA_URL,
            },
        }
    )


@pytest.mark.unit
def test_consistent_name_anonymization(
    in_memory_data_context_config_usage_stats_enabled, monkeypatch
):
    monkeypatch.delenv(
        "GE_USAGE_STATS", raising=False
    )  # Undo the project-wide test default
    context = get_context(in_memory_data_context_config_usage_stats_enabled)
    assert context.data_context_id == "00000000-0000-0000-0000-000000000001"
    payload = run_validation_operator_usage_statistics(
        context,
        "action_list_operator",
        assets_to_validate=[
            ({"__fake_batch_kwargs": "mydatasource"}, "__fake_expectation_suite_name")
        ],
        run_id="foo",
    )
    # For a *specific* data_context_id, all names will be consistently anonymized
    assert payload["anonymized_operator_name"] == "e079c942d946b823312054118b3b6ef4"


@pytest.mark.unit
def test_global_override_environment_variable_data_context(
    in_memory_data_context_config_usage_stats_enabled, monkeypatch
):
    """Set the env variable GE_USAGE_STATS value to any of the following: FALSE, False, false, 0"""
    monkeypatch.setenv("GE_USAGE_STATS", "False")
    assert (
        in_memory_data_context_config_usage_stats_enabled.anonymous_usage_statistics.enabled
        is True
    )
    context = get_context(in_memory_data_context_config_usage_stats_enabled)
    project_config = context._project_config
    assert project_config.anonymous_usage_statistics.enabled is False


@pytest.mark.filesystem
def test_global_override_from_config_file_in_etc(
    in_memory_data_context_config_usage_stats_enabled, tmp_path_factory, monkeypatch
):
    monkeypatch.delenv(
        "GE_USAGE_STATS", raising=False
    )  # Undo the project-wide test default
    home_config_dir = tmp_path_factory.mktemp("home_dir")
    home_config_dir = str(home_config_dir)
    etc_config_dir = tmp_path_factory.mktemp("etc")
    etc_config_dir = str(etc_config_dir)
    config_dirs = [home_config_dir, etc_config_dir]
    config_dirs = [
        os.path.join(config_dir, "great_expectations.conf")  # noqa: PTH118
        for config_dir in config_dirs
    ]

    for false_string in ["False", "false", "f", "FALSE"]:
        disabled_config = configparser.ConfigParser()
        disabled_config["anonymous_usage_statistics"] = {"enabled": false_string}

        with open(
            os.path.join(etc_config_dir, "great_expectations.conf"), "w"  # noqa: PTH118
        ) as configfile:
            disabled_config.write(configfile)

        with mock.patch(
            "great_expectations.data_context.AbstractDataContext.GLOBAL_CONFIG_PATHS",
            config_dirs,
        ):
            assert (
                in_memory_data_context_config_usage_stats_enabled.anonymous_usage_statistics.enabled
                is True
            )
            context = get_context(
                deepcopy(in_memory_data_context_config_usage_stats_enabled)
            )
            project_config = context._project_config
            assert project_config.anonymous_usage_statistics.enabled is False


@pytest.mark.filesystem
def test_global_override_from_config_file_in_home_folder(
    in_memory_data_context_config_usage_stats_enabled, tmp_path_factory, monkeypatch
):
    monkeypatch.delenv(
        "GE_USAGE_STATS", raising=False
    )  # Undo the project-wide test default
    home_config_dir = tmp_path_factory.mktemp("home_dir")
    home_config_dir = str(home_config_dir)
    etc_config_dir = tmp_path_factory.mktemp("etc")
    etc_config_dir = str(etc_config_dir)
    config_dirs = [home_config_dir, etc_config_dir]
    config_dirs = [
        os.path.join(config_dir, "great_expectations.conf")  # noqa: PTH118
        for config_dir in config_dirs
    ]

    enabled_config = configparser.ConfigParser()
    enabled_config["anonymous_usage_statistics"] = {"enabled": "True"}

    for false_string in ["False", "false", "f", "FALSE"]:
        disabled_config = configparser.ConfigParser()
        disabled_config["anonymous_usage_statistics"] = {"enabled": false_string}

        with open(
            os.path.join(home_config_dir, "great_expectations.conf"),  # noqa: PTH118
            "w",
        ) as configfile:
            disabled_config.write(configfile)

        with mock.patch(
            "great_expectations.data_context.AbstractDataContext.GLOBAL_CONFIG_PATHS",
            config_dirs,
        ):
            assert (
                in_memory_data_context_config_usage_stats_enabled.anonymous_usage_statistics.enabled
                is True
            )
            context = get_context(
                deepcopy(in_memory_data_context_config_usage_stats_enabled)
            )
            project_config = context._project_config
            assert project_config.anonymous_usage_statistics.enabled is False


@pytest.mark.filesystem
def test_global_override_in_yml(tmp_path_factory, monkeypatch):
    monkeypatch.delenv(
        "GE_USAGE_STATS", raising=False
    )  # Undo the project-wide test default
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")  # noqa: PTH118
    os.makedirs(context_path, exist_ok=True)  # noqa: PTH103
    fixture_dir = file_relative_path(__file__, "../../test_fixtures")

    shutil.copy(
        os.path.join(  # noqa: PTH118
            fixture_dir, "great_expectations_basic_with_usage_stats_disabled.yml"
        ),
        str(os.path.join(context_path, "great_expectations.yml")),  # noqa: PTH118
    )

    assert (
        get_context(
            context_root_dir=context_path
        )._project_config.anonymous_usage_statistics.enabled
        is False
    )


@pytest.mark.filesystem
def test_global_override_conf_overrides_yml_and_env_variable(
    tmp_path_factory, monkeypatch
):
    """
    What does this test and why?

    anonymous_usage_stats_enabled can be set in the following 3 places
        - great_expectations.yml (YML)
        - GE_USAGE_STATS environment variable (env)
        - conf file (conf)
    If it is set as `False` in *any* of the 3 places, the global value is set to `False`

    This test tests the following scenario:
        - `True` in YML
        - `True` in env
        - `False` in conf

    Therefore the global value is set to `False`
    """
    monkeypatch.setenv("GE_USAGE_STATS", "True")

    home_config_dir = tmp_path_factory.mktemp("home_dir")
    home_config_dir = str(home_config_dir)
    etc_config_dir = tmp_path_factory.mktemp("etc")
    etc_config_dir = str(etc_config_dir)
    config_dirs = [home_config_dir, etc_config_dir]
    config_dirs = [
        os.path.join(config_dir, "great_expectations.conf")  # noqa: PTH118
        for config_dir in config_dirs
    ]

    disabled_config = configparser.ConfigParser()
    disabled_config["anonymous_usage_statistics"] = {"enabled": "False"}

    with open(
        os.path.join(etc_config_dir, "great_expectations.conf"), "w"  # noqa: PTH118
    ) as configfile:
        disabled_config.write(configfile)

    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")  # noqa: PTH118
    os.makedirs(context_path, exist_ok=True)  # noqa: PTH103
    fixture_dir = file_relative_path(__file__, "../../test_fixtures")

    shutil.copy(
        os.path.join(  # noqa: PTH118
            fixture_dir, "great_expectations_v013_basic_with_usage_stats_enabled.yml"
        ),
        str(os.path.join(context_path, "great_expectations.yml")),  # noqa: PTH118
    )

    assert (
        get_context(
            context_root_dir=context_path
        )._project_config.anonymous_usage_statistics.enabled
        is True
    )

    with mock.patch(
        "great_expectations.data_context.AbstractDataContext.GLOBAL_CONFIG_PATHS",
        config_dirs,
    ):
        context = get_context(context_root_dir=context_path)
        project_config = context._project_config
        assert project_config.anonymous_usage_statistics.enabled is False


@pytest.mark.filesystem
def test_global_override_env_overrides_yml_and_conf(tmp_path_factory, monkeypatch):
    """
    What does this test and why?

    anonymous_usage_stats_enabled can be set in the following 3 places
        - great_expectations.yml (YML)
        - GE_USAGE_STATS environment variable (env)
        - conf file (conf)
    If it is set as `False` in *any* of the 3 places, the global value is set to `False`

    This test tests the following scenario:
        - `True` in YML
        - `False` in env
        - `True` in conf

    Therefore the global value is set to `False`
    """
    monkeypatch.setenv("GE_USAGE_STATS", "False")

    home_config_dir = tmp_path_factory.mktemp("home_dir")
    home_config_dir = str(home_config_dir)
    etc_config_dir = tmp_path_factory.mktemp("etc")
    etc_config_dir = str(etc_config_dir)
    config_dirs = [home_config_dir, etc_config_dir]
    config_dirs = [
        os.path.join(config_dir, "great_expectations.conf")  # noqa: PTH118
        for config_dir in config_dirs
    ]

    disabled_config = configparser.ConfigParser()
    disabled_config["anonymous_usage_statistics"] = {"enabled": "True"}

    with open(
        os.path.join(etc_config_dir, "great_expectations.conf"), "w"  # noqa: PTH118
    ) as configfile:
        disabled_config.write(configfile)

    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")  # noqa: PTH118
    os.makedirs(context_path, exist_ok=True)  # noqa: PTH103
    fixture_dir = file_relative_path(__file__, "../../test_fixtures")

    shutil.copy(
        os.path.join(  # noqa: PTH118
            fixture_dir, "great_expectations_v013_basic_with_usage_stats_enabled.yml"
        ),
        str(os.path.join(context_path, "great_expectations.yml")),  # noqa: PTH118
    )

    assert (
        get_context(
            context_root_dir=context_path
        )._project_config.anonymous_usage_statistics.enabled
        is False
    )

    with mock.patch(
        "great_expectations.data_context.AbstractDataContext.GLOBAL_CONFIG_PATHS",
        config_dirs,
    ):
        context = get_context(context_root_dir=context_path)
        project_config = context._project_config
        assert project_config.anonymous_usage_statistics.enabled is False


@pytest.mark.filesystem
def test_global_override_yml_overrides_env_and_conf(tmp_path_factory, monkeypatch):
    """
    What does this test and why?

    anonymous_usage_stats_enabled can be set in the following 3 places
        - great_expectations.yml (YML)
        - GE_USAGE_STATS environment variable (env)
        - conf file (conf)
    If it is set as `False` in *any* of the 3 places, the global value is set to `False`

    This test tests the following scenario:
        - `False` in YML
        - `True` in env
        - `True` in conf

    Therefore the global value is set to `False`
    """
    monkeypatch.setenv("GE_USAGE_STATS", "True")

    home_config_dir = tmp_path_factory.mktemp("home_dir")
    home_config_dir = str(home_config_dir)
    etc_config_dir = tmp_path_factory.mktemp("etc")
    etc_config_dir = str(etc_config_dir)
    config_dirs = [home_config_dir, etc_config_dir]
    config_dirs = [
        os.path.join(config_dir, "great_expectations.conf")  # noqa: PTH118
        for config_dir in config_dirs
    ]

    disabled_config = configparser.ConfigParser()
    disabled_config["anonymous_usage_statistics"] = {"enabled": "True"}

    with open(
        os.path.join(etc_config_dir, "great_expectations.conf"), "w"  # noqa: PTH118
    ) as configfile:
        disabled_config.write(configfile)

    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")  # noqa: PTH118
    os.makedirs(context_path, exist_ok=True)  # noqa: PTH103
    fixture_dir = file_relative_path(__file__, "../../test_fixtures")

    shutil.copy(
        os.path.join(  # noqa: PTH118
            fixture_dir, "great_expectations_basic_with_usage_stats_disabled.yml"
        ),
        str(os.path.join(context_path, "great_expectations.yml")),  # noqa: PTH118
    )

    assert (
        get_context(
            context_root_dir=context_path
        )._project_config.anonymous_usage_statistics.enabled
        is False
    )

    with mock.patch(
        "great_expectations.data_context.AbstractDataContext.GLOBAL_CONFIG_PATHS",
        config_dirs,
    ):
        context = get_context(context_root_dir=context_path)
        project_config = context._project_config
        assert project_config.anonymous_usage_statistics.enabled is False
