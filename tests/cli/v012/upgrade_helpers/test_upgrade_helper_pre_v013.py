import json
import os
import shutil

from click.testing import CliRunner
from freezegun import freeze_time
from moto import mock_s3

import great_expectations
from great_expectations import DataContext
from great_expectations.cli.v012 import cli
from great_expectations.data_context.util import file_relative_path
from great_expectations.util import gen_directory_tree_str
from tests.cli.v012.utils import (
    VALIDATION_OPERATORS_DEPRECATION_MESSAGE,
    assert_no_logging_messages_or_tracebacks,
)

try:
    from unittest import mock
except ImportError:
    from unittest import mock


def test_project_upgrade_already_up_to_date(v10_project_directory, caplog):
    # test great_expectations project upgrade command with project with config_version 2

    # copy v2 yml
    shutil.copy(
        file_relative_path(
            __file__, "../../../test_fixtures/upgrade_helper/great_expectations_v2.yml"
        ),
        os.path.join(v10_project_directory, "great_expectations.yml"),
    )

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["project", "upgrade", "-d", v10_project_directory],
        input="\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert "Checking project..." in stdout
    assert "Your project is up-to-date - no further upgrade is necessary." in stdout
    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
        allowed_deprecation_message=VALIDATION_OPERATORS_DEPRECATION_MESSAGE,
    )


def test_upgrade_helper_intervention_on_cli_command(v10_project_directory, caplog):
    # test if cli detects out of date project and asks to run upgrade helper
    # decline upgrade and ensure config version was not modified

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["suite", "list", "-d", v10_project_directory],
        input="n\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert (
        "Your project appears to have an out-of-date config version (1.0) - the version number must be at least 3."
        in stdout
    )
    assert "In order to proceed, your project must be upgraded." in stdout
    assert (
        "Would you like to run the Upgrade Helper to bring your project up-to-date? [Y/n]:"
        in stdout
    )
    assert (
        "Ok, exiting now. To upgrade at a later time, use the following command: [36mgreat_expectations project "
        "upgrade[0m" in stdout
    )
    assert (
        "To learn more about the upgrade process, visit ["
        "36mhttps://docs.greatexpectations.io/en/latest/how_to_guides/migrating_versions.html"
        in stdout
    )
    assert_no_logging_messages_or_tracebacks(caplog, result)

    # make sure config version unchanged
    assert (
        DataContext.get_ge_config_version(context_root_dir=v10_project_directory) == 1
    )

    expected_project_tree_str = """\
great_expectations/
    .gitignore
    great_expectations.yml
    checkpoints/
        .gitkeep
    expectations/
        .gitkeep
    notebooks/
        .gitkeep
    plugins/
        custom_store_backends/
            __init__.py
            my_custom_store_backend.py
    uncommitted/
        config_variables.yml
        data_docs/
            local_site/
                expectations/
                    .gitkeep
                static/
                    .gitkeep
                validations/
                    diabetic_data/
                        warning/
                            20200430T191246.763896Z/
                                c3b4c5df224fef4b1a056a0f3b93aba5.html
        validations/
            diabetic_data/
                warning/
                    20200430T191246.763896Z/
                        c3b4c5df224fef4b1a056a0f3b93aba5.json
"""
    obs_project_tree_str = gen_directory_tree_str(v10_project_directory)
    assert obs_project_tree_str == expected_project_tree_str


@freeze_time("09/26/2019 13:42:41")
def test_basic_project_upgrade(v10_project_directory, caplog):
    # test project upgrade that requires no manual steps

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["project", "upgrade", "-d", v10_project_directory],
        input="\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    with open(
        file_relative_path(
            __file__,
            "../../../test_fixtures/upgrade_helper/test_basic_project_upgrade_expected_v012_stdout.fixture",
        )
    ) as f:
        expected_stdout = f.read()
        expected_stdout = expected_stdout.replace(
            "GE_PROJECT_DIR", v10_project_directory
        )
        assert stdout == expected_stdout

    expected_project_tree_str = """\
great_expectations/
    .gitignore
    great_expectations.yml
    checkpoints/
        .gitkeep
    expectations/
        .ge_store_backend_id
        .gitkeep
    notebooks/
        .gitkeep
    plugins/
        custom_store_backends/
            __init__.py
            my_custom_store_backend.py
    uncommitted/
        config_variables.yml
        data_docs/
            local_site/
                expectations/
                    .gitkeep
                static/
                    .gitkeep
                validations/
                    diabetic_data/
                        warning/
                            20200430T191246.763896Z/
                                20200430T191246.763896Z/
                                    c3b4c5df224fef4b1a056a0f3b93aba5.html
        logs/
            project_upgrades/
                UpgradeHelperV11_20190926T134241.000000Z.json
                UpgradeHelperV13_20190926T134241.000000Z.json
        validations/
            .ge_store_backend_id
            diabetic_data/
                warning/
                    20200430T191246.763896Z/
                        20200430T191246.763896Z/
                            c3b4c5df224fef4b1a056a0f3b93aba5.json
"""
    obs_project_tree_str = gen_directory_tree_str(v10_project_directory)
    assert obs_project_tree_str == expected_project_tree_str
    # make sure config number incremented
    assert (
        DataContext.get_ge_config_version(context_root_dir=v10_project_directory) == 3
    )

    with open(
        file_relative_path(
            __file__,
            "../../../test_fixtures/upgrade_helper/UpgradeHelperV11_basic_upgrade_log.json",
        )
    ) as f:
        expected_upgrade_log_dict = json.load(f)
        expected_upgrade_log_str = json.dumps(expected_upgrade_log_dict)
        expected_upgrade_log_str = expected_upgrade_log_str.replace(
            "GE_PROJECT_DIR", v10_project_directory
        )
        expected_upgrade_log_dict = json.loads(expected_upgrade_log_str)

    with open(
        f"{v10_project_directory}/uncommitted/logs/project_upgrades/UpgradeHelperV11_20190926T134241.000000Z.json"
    ) as f:
        obs_upgrade_log_dict = json.load(f)

    assert obs_upgrade_log_dict == expected_upgrade_log_dict


@freeze_time("09/26/2019 13:42:41")
def test_project_upgrade_with_manual_steps(
    v10_project_directory, caplog, sa, postgresql_engine
):
    # This test requires sqlalchemy because it includes database backends configured
    # test project upgrade that requires manual steps

    # copy v2 yml
    shutil.copy(
        file_relative_path(
            __file__,
            "../../../test_fixtures/upgrade_helper/great_expectations_v1_needs_manual_upgrade.yml",
        ),
        os.path.join(v10_project_directory, "great_expectations.yml"),
    )

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["project", "upgrade", "-d", v10_project_directory],
        input="\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    with open(
        file_relative_path(
            __file__,
            "../../../test_fixtures/upgrade_helper/test_project_upgrade_with_manual_steps_expected_v012_stdout.fixture",
        )
    ) as f:
        expected_stdout = f.read()
        expected_stdout = expected_stdout.replace(
            "GE_PROJECT_DIR", v10_project_directory
        )
        assert stdout == expected_stdout

    pycache_dir_path = os.path.join(
        v10_project_directory, "plugins", "custom_store_backends", "__pycache__"
    )
    try:
        shutil.rmtree(pycache_dir_path)
    except FileNotFoundError:
        pass

    expected_project_tree_str = """\
great_expectations/
    .gitignore
    great_expectations.yml
    checkpoints/
        .gitkeep
    expectations/
        .ge_store_backend_id
        .gitkeep
    notebooks/
        .gitkeep
    plugins/
        custom_store_backends/
            __init__.py
            my_custom_store_backend.py
    uncommitted/
        config_variables.yml
        data_docs/
            local_site/
                expectations/
                    .gitkeep
                static/
                    .gitkeep
                validations/
                    diabetic_data/
                        warning/
                            20200430T191246.763896Z/
                                20200430T191246.763896Z/
                                    c3b4c5df224fef4b1a056a0f3b93aba5.html
        logs/
            project_upgrades/
                UpgradeHelperV11_20190926T134241.000000Z.json
        validations/
            .ge_store_backend_id
            diabetic_data/
                warning/
                    20200430T191246.763896Z/
                        20200430T191246.763896Z/
                            c3b4c5df224fef4b1a056a0f3b93aba5.json
"""
    obs_project_tree_str = gen_directory_tree_str(v10_project_directory)
    assert obs_project_tree_str == expected_project_tree_str
    # make sure config number not incremented
    assert (
        DataContext.get_ge_config_version(context_root_dir=v10_project_directory) == 1
    )

    with open(
        file_relative_path(
            __file__,
            "../../../test_fixtures/upgrade_helper/UpgradeHelperV11_manual_steps_upgrade_log.json",
        )
    ) as f:
        expected_upgrade_log_dict = json.load(f)
        expected_upgrade_log_str = json.dumps(expected_upgrade_log_dict)
        expected_upgrade_log_str = expected_upgrade_log_str.replace(
            "GE_PROJECT_DIR", v10_project_directory
        )
        expected_upgrade_log_dict = json.loads(expected_upgrade_log_str)

    with open(
        f"{v10_project_directory}/uncommitted/logs/project_upgrades/UpgradeHelperV11_20190926T134241.000000Z.json"
    ) as f:
        obs_upgrade_log_dict = json.load(f)

    assert obs_upgrade_log_dict == expected_upgrade_log_dict


@freeze_time("09/26/2019 13:42:41")
@mock_s3
def test_project_upgrade_with_exception(v10_project_directory, caplog):
    # test project upgrade that requires manual steps

    # copy v2 yml
    shutil.copy(
        file_relative_path(
            __file__,
            "../../../test_fixtures/upgrade_helper/great_expectations_v1_basic_with_exception.yml",
        ),
        os.path.join(v10_project_directory, "great_expectations.yml"),
    )

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["project", "upgrade", "-d", v10_project_directory],
        input="\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    with open(
        file_relative_path(
            __file__,
            "../../../test_fixtures/upgrade_helper/test_project_upgrade_with_exception_expected_v012_stdout.fixture",
        )
    ) as f:
        expected_stdout = f.read()
        expected_stdout = expected_stdout.replace(
            "GE_PROJECT_DIR", v10_project_directory
        )
        assert stdout == expected_stdout

    expected_project_tree_str = """\
great_expectations/
    .gitignore
    great_expectations.yml
    checkpoints/
        .gitkeep
    expectations/
        .ge_store_backend_id
        .gitkeep
    notebooks/
        .gitkeep
    plugins/
        custom_store_backends/
            __init__.py
            my_custom_store_backend.py
    uncommitted/
        config_variables.yml
        data_docs/
            local_site/
                expectations/
                    .gitkeep
                static/
                    .gitkeep
                validations/
                    diabetic_data/
                        warning/
                            20200430T191246.763896Z/
                                20200430T191246.763896Z/
                                    c3b4c5df224fef4b1a056a0f3b93aba5.html
        logs/
            project_upgrades/
                UpgradeHelperV11_20190926T134241.000000Z.json
        validations/
            .ge_store_backend_id
            diabetic_data/
                warning/
                    20200430T191246.763896Z/
                        20200430T191246.763896Z/
                            c3b4c5df224fef4b1a056a0f3b93aba5.json
"""
    obs_project_tree_str = gen_directory_tree_str(v10_project_directory)
    assert obs_project_tree_str == expected_project_tree_str
    # make sure config number not incremented
    assert (
        DataContext.get_ge_config_version(context_root_dir=v10_project_directory) == 1
    )

    with open(
        file_relative_path(
            __file__,
            "../../../test_fixtures/upgrade_helper/UpgradeHelperV11_basic_upgrade_with_exception_log.json",
        )
    ) as f:
        expected_upgrade_log_dict = json.load(f)
        expected_upgrade_log_str = json.dumps(expected_upgrade_log_dict)
        expected_upgrade_log_str = expected_upgrade_log_str.replace(
            "GE_PROJECT_DIR", v10_project_directory
        )
        expected_upgrade_log_str = expected_upgrade_log_str.replace(
            "GE_PATH", os.path.split(great_expectations.__file__)[0]
        )
        expected_upgrade_log_dict = json.loads(expected_upgrade_log_str)

    with open(
        f"{v10_project_directory}/uncommitted/logs/project_upgrades/UpgradeHelperV11_20190926T134241.000000Z.json"
    ) as f:
        obs_upgrade_log_dict = json.load(f)
        obs_upgrade_log_dict["exceptions"][0]["exception_message"] = ""

    assert obs_upgrade_log_dict == expected_upgrade_log_dict


@freeze_time("01/19/2021 13:26:39")
def test_v2_to_v3_project_upgrade(v20_project_directory, caplog):
    # test project upgrade that requires no manual steps

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["project", "upgrade", "-d", v20_project_directory],
        input="\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    with open(
        file_relative_path(
            __file__,
            "../../../test_fixtures/upgrade_helper/test_v2_to_v3_project_upgrade_expected_v012_stdout.fixture",
        )
    ) as f:
        expected_stdout = f.read()
        expected_stdout = expected_stdout.replace(
            "GE_PROJECT_DIR", v20_project_directory
        )
        assert stdout == expected_stdout

    expected_project_tree_str = """\
great_expectations/
    .gitignore
    great_expectations.yml
    checkpoints/
        .gitkeep
        my_checkpoint.yml
        titanic_checkpoint_0.yml
        titanic_checkpoint_1.yml
        titanic_checkpoint_2.yml
    expectations/
        .ge_store_backend_id
        .gitkeep
    notebooks/
        .gitkeep
        pandas/
            validation_playground.ipynb
        spark/
            validation_playground.ipynb
        sql/
            validation_playground.ipynb
    plugins/
        custom_data_docs/
            styles/
                data_docs_custom_styles.css
    uncommitted/
        config_variables.yml
        data_docs/
            local_site/
                expectations/
                    .gitkeep
                static/
                    .gitkeep
                validations/
                    diabetic_data/
                        warning/
                            20200430T191246.763896Z/
                                c3b4c5df224fef4b1a056a0f3b93aba5.html
        logs/
            project_upgrades/
                UpgradeHelperV13_20210119T132639.000000Z.json
        validations/
            .ge_store_backend_id
            diabetic_data/
                warning/
                    20200430T191246.763896Z/
                        c3b4c5df224fef4b1a056a0f3b93aba5.json
"""
    obs_project_tree_str = gen_directory_tree_str(v20_project_directory)
    assert obs_project_tree_str == expected_project_tree_str
    # make sure config number incremented
    assert (
        DataContext.get_ge_config_version(context_root_dir=v20_project_directory) == 3
    )

    with open(
        file_relative_path(
            __file__,
            "../../../test_fixtures/upgrade_helper/UpgradeHelperV13_basic_upgrade_log.json",
        )
    ) as f:
        expected_upgrade_log_dict = json.load(f)
        expected_upgrade_log_str = json.dumps(expected_upgrade_log_dict)
        expected_upgrade_log_str = expected_upgrade_log_str.replace(
            "GE_PROJECT_DIR", v20_project_directory
        )
        expected_upgrade_log_dict = json.loads(expected_upgrade_log_str)

    with open(
        f"{v20_project_directory}/uncommitted/logs/project_upgrades/UpgradeHelperV13_20210119T132639.000000Z.json"
    ) as f:
        obs_upgrade_log_dict = json.load(f)

    assert obs_upgrade_log_dict == expected_upgrade_log_dict
