import configparser
import os
import shutil
from copy import deepcopy
from unittest import mock

import pytest

from great_expectations.core.usage_statistics.usage_statistics import (
    run_validation_operator_usage_statistics,
)
from great_expectations.data_context import BaseDataContext, DataContext
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context.util import file_relative_path
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


def test_consistent_name_anonymization(
    in_memory_data_context_config_usage_stats_enabled, monkeypatch
):
    monkeypatch.delenv(
        "GE_USAGE_STATS", raising=False
    )  # Undo the project-wide test default
    context = BaseDataContext(in_memory_data_context_config_usage_stats_enabled)
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


def test_opt_out_environment_variable(
    in_memory_data_context_config_usage_stats_enabled, monkeypatch
):
    """Set the env variable GE_USAGE_STATS value to any of the following: FALSE, False, false, 0"""
    monkeypatch.setenv("GE_USAGE_STATS", "False")
    assert (
        in_memory_data_context_config_usage_stats_enabled.anonymous_usage_statistics.enabled
        is True
    )
    context = BaseDataContext(in_memory_data_context_config_usage_stats_enabled)
    project_config = context._project_config
    assert project_config.anonymous_usage_statistics.enabled is False


def test_opt_out_etc(
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
        os.path.join(config_dir, "great_expectations.conf")
        for config_dir in config_dirs
    ]

    for false_string in ["False", "false", "f", "FALSE"]:
        disabled_config = configparser.ConfigParser()
        disabled_config["anonymous_usage_statistics"] = {"enabled": false_string}

        with open(
            os.path.join(etc_config_dir, "great_expectations.conf"), "w"
        ) as configfile:
            disabled_config.write(configfile)

        with mock.patch(
            "great_expectations.data_context.BaseDataContext.GLOBAL_CONFIG_PATHS",
            config_dirs,
        ):
            assert (
                in_memory_data_context_config_usage_stats_enabled.anonymous_usage_statistics.enabled
                is True
            )
            context = BaseDataContext(
                deepcopy(in_memory_data_context_config_usage_stats_enabled)
            )
            project_config = context._project_config
            assert project_config.anonymous_usage_statistics.enabled is False


def test_opt_out_home_folder(
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
        os.path.join(config_dir, "great_expectations.conf")
        for config_dir in config_dirs
    ]

    enabled_config = configparser.ConfigParser()
    enabled_config["anonymous_usage_statistics"] = {"enabled": "True"}

    for false_string in ["False", "false", "f", "FALSE"]:
        disabled_config = configparser.ConfigParser()
        disabled_config["anonymous_usage_statistics"] = {"enabled": false_string}

        with open(
            os.path.join(home_config_dir, "great_expectations.conf"), "w"
        ) as configfile:
            disabled_config.write(configfile)

        with mock.patch(
            "great_expectations.data_context.BaseDataContext.GLOBAL_CONFIG_PATHS",
            config_dirs,
        ):
            assert (
                in_memory_data_context_config_usage_stats_enabled.anonymous_usage_statistics.enabled
                is True
            )
            context = BaseDataContext(
                deepcopy(in_memory_data_context_config_usage_stats_enabled)
            )
            project_config = context._project_config
            assert project_config.anonymous_usage_statistics.enabled is False


def test_opt_out_yml(tmp_path_factory, monkeypatch):
    monkeypatch.delenv(
        "GE_USAGE_STATS", raising=False
    )  # Undo the project-wide test default
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    os.makedirs(context_path, exist_ok=True)
    fixture_dir = file_relative_path(__file__, "../../test_fixtures")

    shutil.copy(
        os.path.join(
            fixture_dir, "great_expectations_basic_with_usage_stats_disabled.yml"
        ),
        str(os.path.join(context_path, "great_expectations.yml")),
    )

    assert (
        DataContext(
            context_root_dir=context_path
        )._project_config.anonymous_usage_statistics.enabled
        is False
    )


# Test precedence: environment variable > home folder > /etc > yml
def test_opt_out_env_var_overrides_home_folder(
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
        os.path.join(config_dir, "great_expectations.conf")
        for config_dir in config_dirs
    ]

    enabled_config = configparser.ConfigParser()
    enabled_config["anonymous_usage_statistics"] = {"enabled": "True"}

    with open(
        os.path.join(home_config_dir, "great_expectations.conf"), "w"
    ) as configfile:
        enabled_config.write(configfile)

    monkeypatch.setenv("GE_USAGE_STATS", "False")

    with mock.patch(
        "great_expectations.data_context.BaseDataContext.GLOBAL_CONFIG_PATHS",
        config_dirs,
    ):
        assert (
            in_memory_data_context_config_usage_stats_enabled.anonymous_usage_statistics.enabled
            is True
        )
        context = BaseDataContext(in_memory_data_context_config_usage_stats_enabled)
        project_config = context._project_config
        assert project_config.anonymous_usage_statistics.enabled is False


def test_opt_out_env_var_overrides_etc(
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
        os.path.join(config_dir, "great_expectations.conf")
        for config_dir in config_dirs
    ]

    enabled_config = configparser.ConfigParser()
    enabled_config["anonymous_usage_statistics"] = {"enabled": "True"}

    with open(
        os.path.join(etc_config_dir, "great_expectations.conf"), "w"
    ) as configfile:
        enabled_config.write(configfile)

    monkeypatch.setenv("GE_USAGE_STATS", "False")

    with mock.patch(
        "great_expectations.data_context.BaseDataContext.GLOBAL_CONFIG_PATHS",
        config_dirs,
    ):
        assert (
            in_memory_data_context_config_usage_stats_enabled.anonymous_usage_statistics.enabled
            is True
        )
        context = BaseDataContext(in_memory_data_context_config_usage_stats_enabled)
        project_config = context._project_config
        assert project_config.anonymous_usage_statistics.enabled is False


def test_opt_out_env_var_overrides_yml(tmp_path_factory, monkeypatch):
    monkeypatch.delenv(
        "GE_USAGE_STATS", raising=False
    )  # Undo the project-wide test default
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    os.makedirs(context_path, exist_ok=True)
    fixture_dir = file_relative_path(__file__, "../../test_fixtures")

    shutil.copy(
        os.path.join(
            fixture_dir, "great_expectations_basic_with_usage_stats_enabled.yml"
        ),
        str(os.path.join(context_path, "great_expectations.yml")),
    )

    assert (
        DataContext(
            context_root_dir=context_path
        )._project_config.anonymous_usage_statistics.enabled
        is True
    )

    monkeypatch.setenv("GE_USAGE_STATS", "False")
    context = DataContext(context_root_dir=context_path)
    project_config = context._project_config
    assert project_config.anonymous_usage_statistics.enabled is False


def test_opt_out_env_var_overrides_yml_v013(tmp_path_factory, monkeypatch):
    monkeypatch.delenv(
        "GE_USAGE_STATS", raising=False
    )  # Undo the project-wide test default
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    os.makedirs(context_path, exist_ok=True)
    fixture_dir = file_relative_path(__file__, "../../test_fixtures")

    shutil.copy(
        os.path.join(
            fixture_dir, "great_expectations_v013_basic_with_usage_stats_enabled.yml"
        ),
        str(os.path.join(context_path, "great_expectations.yml")),
    )

    assert (
        DataContext(
            context_root_dir=context_path
        )._project_config.anonymous_usage_statistics.enabled
        is True
    )

    monkeypatch.setenv("GE_USAGE_STATS", "False")
    context = DataContext(context_root_dir=context_path)
    project_config = context._project_config
    assert project_config.anonymous_usage_statistics.enabled is False


def test_opt_out_home_folder_overrides_etc(
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
        os.path.join(config_dir, "great_expectations.conf")
        for config_dir in config_dirs
    ]

    enabled_config = configparser.ConfigParser()
    enabled_config["anonymous_usage_statistics"] = {"enabled": "True"}

    disabled_config = configparser.ConfigParser()
    disabled_config["anonymous_usage_statistics"] = {"enabled": "False"}

    with open(
        os.path.join(home_config_dir, "great_expectations.conf"), "w"
    ) as configfile:
        disabled_config.write(configfile)
    with open(
        os.path.join(etc_config_dir, "great_expectations.conf"), "w"
    ) as configfile:
        enabled_config.write(configfile)

    with mock.patch(
        "great_expectations.data_context.BaseDataContext.GLOBAL_CONFIG_PATHS",
        config_dirs,
    ):
        assert (
            in_memory_data_context_config_usage_stats_enabled.anonymous_usage_statistics.enabled
            is True
        )
        context = BaseDataContext(in_memory_data_context_config_usage_stats_enabled)
        project_config = context._project_config
        assert project_config.anonymous_usage_statistics.enabled is False


def test_opt_out_home_folder_overrides_yml(tmp_path_factory, monkeypatch):
    monkeypatch.delenv(
        "GE_USAGE_STATS", raising=False
    )  # Undo the project-wide test default
    home_config_dir = tmp_path_factory.mktemp("home_dir")
    home_config_dir = str(home_config_dir)
    etc_config_dir = tmp_path_factory.mktemp("etc")
    etc_config_dir = str(etc_config_dir)
    config_dirs = [home_config_dir, etc_config_dir]
    config_dirs = [
        os.path.join(config_dir, "great_expectations.conf")
        for config_dir in config_dirs
    ]

    disabled_config = configparser.ConfigParser()
    disabled_config["anonymous_usage_statistics"] = {"enabled": "False"}

    with open(
        os.path.join(home_config_dir, "great_expectations.conf"), "w"
    ) as configfile:
        disabled_config.write(configfile)

    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    os.makedirs(context_path, exist_ok=True)
    fixture_dir = file_relative_path(__file__, "../../test_fixtures")

    shutil.copy(
        os.path.join(
            fixture_dir, "great_expectations_basic_with_usage_stats_enabled.yml"
        ),
        str(os.path.join(context_path, "great_expectations.yml")),
    )

    assert (
        DataContext(
            context_root_dir=context_path
        )._project_config.anonymous_usage_statistics.enabled
        is True
    )

    with mock.patch(
        "great_expectations.data_context.BaseDataContext.GLOBAL_CONFIG_PATHS",
        config_dirs,
    ):
        context = DataContext(context_root_dir=context_path)
        project_config = context._project_config
        assert project_config.anonymous_usage_statistics.enabled is False


def test_opt_out_home_folder_overrides_yml_v013(tmp_path_factory, monkeypatch):
    monkeypatch.delenv(
        "GE_USAGE_STATS", raising=False
    )  # Undo the project-wide test default
    home_config_dir = tmp_path_factory.mktemp("home_dir")
    home_config_dir = str(home_config_dir)
    etc_config_dir = tmp_path_factory.mktemp("etc")
    etc_config_dir = str(etc_config_dir)
    config_dirs = [home_config_dir, etc_config_dir]
    config_dirs = [
        os.path.join(config_dir, "great_expectations.conf")
        for config_dir in config_dirs
    ]

    disabled_config = configparser.ConfigParser()
    disabled_config["anonymous_usage_statistics"] = {"enabled": "False"}

    with open(
        os.path.join(home_config_dir, "great_expectations.conf"), "w"
    ) as configfile:
        disabled_config.write(configfile)

    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    os.makedirs(context_path, exist_ok=True)
    fixture_dir = file_relative_path(__file__, "../../test_fixtures")

    shutil.copy(
        os.path.join(
            fixture_dir, "great_expectations_v013_basic_with_usage_stats_enabled.yml"
        ),
        str(os.path.join(context_path, "great_expectations.yml")),
    )

    assert (
        DataContext(
            context_root_dir=context_path
        )._project_config.anonymous_usage_statistics.enabled
        is True
    )

    with mock.patch(
        "great_expectations.data_context.BaseDataContext.GLOBAL_CONFIG_PATHS",
        config_dirs,
    ):
        context = DataContext(context_root_dir=context_path)
        project_config = context._project_config
        assert project_config.anonymous_usage_statistics.enabled is False


def test_opt_out_etc_overrides_yml(tmp_path_factory, monkeypatch):
    monkeypatch.delenv(
        "GE_USAGE_STATS", raising=False
    )  # Undo the project-wide test default
    home_config_dir = tmp_path_factory.mktemp("home_dir")
    home_config_dir = str(home_config_dir)
    etc_config_dir = tmp_path_factory.mktemp("etc")
    etc_config_dir = str(etc_config_dir)
    config_dirs = [home_config_dir, etc_config_dir]
    config_dirs = [
        os.path.join(config_dir, "great_expectations.conf")
        for config_dir in config_dirs
    ]

    disabled_config = configparser.ConfigParser()
    disabled_config["anonymous_usage_statistics"] = {"enabled": "False"}

    with open(
        os.path.join(etc_config_dir, "great_expectations.conf"), "w"
    ) as configfile:
        disabled_config.write(configfile)

    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    os.makedirs(context_path, exist_ok=True)
    fixture_dir = file_relative_path(__file__, "../../test_fixtures")

    shutil.copy(
        os.path.join(
            fixture_dir, "great_expectations_basic_with_usage_stats_enabled.yml"
        ),
        str(os.path.join(context_path, "great_expectations.yml")),
    )

    assert (
        DataContext(
            context_root_dir=context_path
        )._project_config.anonymous_usage_statistics.enabled
        is True
    )

    with mock.patch(
        "great_expectations.data_context.BaseDataContext.GLOBAL_CONFIG_PATHS",
        config_dirs,
    ):
        context = DataContext(context_root_dir=context_path)
        project_config = context._project_config
        assert project_config.anonymous_usage_statistics.enabled is False


def test_opt_out_etc_overrides_yml_v013(tmp_path_factory, monkeypatch):
    monkeypatch.delenv(
        "GE_USAGE_STATS", raising=False
    )  # Undo the project-wide test default
    home_config_dir = tmp_path_factory.mktemp("home_dir")
    home_config_dir = str(home_config_dir)
    etc_config_dir = tmp_path_factory.mktemp("etc")
    etc_config_dir = str(etc_config_dir)
    config_dirs = [home_config_dir, etc_config_dir]
    config_dirs = [
        os.path.join(config_dir, "great_expectations.conf")
        for config_dir in config_dirs
    ]

    disabled_config = configparser.ConfigParser()
    disabled_config["anonymous_usage_statistics"] = {"enabled": "False"}

    with open(
        os.path.join(etc_config_dir, "great_expectations.conf"), "w"
    ) as configfile:
        disabled_config.write(configfile)

    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    os.makedirs(context_path, exist_ok=True)
    fixture_dir = file_relative_path(__file__, "../../test_fixtures")

    shutil.copy(
        os.path.join(
            fixture_dir, "great_expectations_v013_basic_with_usage_stats_enabled.yml"
        ),
        str(os.path.join(context_path, "great_expectations.yml")),
    )

    assert (
        DataContext(
            context_root_dir=context_path
        )._project_config.anonymous_usage_statistics.enabled
        is True
    )

    with mock.patch(
        "great_expectations.data_context.BaseDataContext.GLOBAL_CONFIG_PATHS",
        config_dirs,
    ):
        context = DataContext(context_root_dir=context_path)
        project_config = context._project_config
        assert project_config.anonymous_usage_statistics.enabled is False
