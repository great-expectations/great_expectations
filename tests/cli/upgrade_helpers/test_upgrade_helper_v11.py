# -*- coding: utf-8 -*-
import json
import os
import shutil

import great_expectations
from click.testing import CliRunner
from freezegun import freeze_time
from great_expectations import DataContext
from great_expectations.cli import cli
from great_expectations.data_context.util import file_relative_path
from great_expectations.util import gen_directory_tree_str
from tests.cli.utils import assert_no_logging_messages_or_tracebacks

try:
    from unittest import mock
except ImportError:
    import mock


def test_project_upgrade_already_up_to_date(v10_project_directory, caplog):
    # test great_expectations project upgrade command with project with config_version 2

    # copy v2 yml
    shutil.copy(
        file_relative_path(
            __file__, "../../test_fixtures/upgrade_helper/great_expectations_v2.yml"
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
    assert "Your project is up-to-date - no upgrade is necessary." in stdout
    assert_no_logging_messages_or_tracebacks(caplog, result)


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
        "Your project appears to have an out-of-date config version (1.0) - the version number must be at least 2."
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

    assert "Checking project..." in stdout
    assert (
        "Your project appears to have an out-of-date config version (1.0) - the version number must be at least 2."
        in stdout
    )
    assert (
        "Would you like to run the Upgrade Helper to bring your project up-to-date? [Y/n]:"
        in stdout
    )
    assert (
        """\
UpgradeHelperV11 will upgrade your project to be compatible with Great Expectations 0.11.x.

**WARNING**: Before proceeding, please make sure you have appropriate backups of your project.

Automated Steps
================

The following Stores and/or Data Docs sites will be upgraded:

    - Validation Stores: validations_store
    - Data Docs Sites: local_site

Manual Steps
=============

No manual upgrade steps are required.

Upgrade Confirmation
=====================

Please consult the 0.11.x migration guide for instructions on how to complete any required manual steps or
to learn more about the automated upgrade process:

    https://docs.greatexpectations.io/en/latest/how_to_guides/migrating_versions.html#id1

Would you like to proceed with the project upgrade? [Y/n]:

Upgrading project...\
"""
        in stdout
    )
    assert (
        "Your project was successfully upgraded to be compatible with Great Expectations 0.11.x."
        in stdout
    )
    assert (
        "The config_version of your great_expectations.yml has been automatically incremented to 2.0."
        in stdout
    )
    assert (
        f"""\
A log detailing the upgrade can be found here:

    - {v10_project_directory}/uncommitted/logs/project_upgrades/UpgradeHelperV11_20190926T134241.000000Z.json[0m[0m

================================================================================
[0m
[32mUpgrade complete. Exiting...\
"""
        in stdout
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
                                20200430T191246.763896Z/
                                    c3b4c5df224fef4b1a056a0f3b93aba5.html
        logs/
            project_upgrades/
                UpgradeHelperV11_20190926T134241.000000Z.json
        validations/
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
        DataContext.get_ge_config_version(context_root_dir=v10_project_directory) == 2
    )

    with open(
        file_relative_path(
            __file__,
            "../../test_fixtures/upgrade_helper/UpgradeHelperV11_basic_upgrade_log.json",
        )
    ) as f:
        expected_upgrade_log_dict = json.load(f)
        expected_upgrade_log_str = json.dumps(expected_upgrade_log_dict)
        expected_upgrade_log_str = expected_upgrade_log_str.replace(
            "GE_PROJECT_DIR", v10_project_directory
        )
        expected_upgrade_log_dict = json.loads(expected_upgrade_log_str)

    with open(
        f"{v10_project_directory}/uncommitted/logs/project_upgrades/UpgradeHelperV11_20190926T134241.000000Z"
        f".json"
    ) as f:
        obs_upgrade_log_dict = json.load(f)

    assert obs_upgrade_log_dict == expected_upgrade_log_dict


@freeze_time("09/26/2019 13:42:41")
def test_project_upgrade_with_manual_steps(v10_project_directory, caplog):
    # test project upgrade that requires manual steps

    # copy v2 yml
    shutil.copy(
        file_relative_path(
            __file__,
            "../../test_fixtures/upgrade_helper/great_expectations_v1_needs_manual_upgrade"
            ".yml",
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
    assert (
        "Your project appears to have an out-of-date config version (1.0) - the version number must be at least 2."
        in stdout
    )
    assert (
        "Would you like to run the Upgrade Helper to bring your project up-to-date? [Y/n]:"
        in stdout
    )
    assert (
        """\
++====================================++
|| UpgradeHelperV11: Upgrade Overview ||
++====================================++

UpgradeHelperV11 will upgrade your project to be compatible with Great Expectations 0.11.x.

**WARNING**: Before proceeding, please make sure you have appropriate backups of your project.

Automated Steps
================

The following Stores and/or Data Docs sites will be upgraded:

    - Validation Stores: validations_store
    - Data Docs Sites: local_site

Manual Steps
=============

The following Stores and/or Data Docs sites must be upgraded manually, due to having a database backend, or backend
type that is unsupported or unrecognized:

    - Stores with database backends: validations_db_store, evaluation_parameter_db_store
    - Stores with unsupported/unrecognized backends: validations_unrecognized_store_backend
    - Data Docs sites with unsupported/unrecognized backends: local_site_unrecognized_backend

Upgrade Confirmation
=====================

Please consult the 0.11.x migration guide for instructions on how to complete any required manual steps or
to learn more about the automated upgrade process:

    https://docs.greatexpectations.io/en/latest/how_to_guides/migrating_versions.html#id1

Would you like to proceed with the project upgrade? [Y/n]:

Upgrading project...\
"""
        in stdout
    )
    assert "The Upgrade Helper has completed the automated upgrade steps." in stdout
    assert (
        f"""\
A log detailing the upgrade can be found here:

    - {v10_project_directory}/uncommitted/logs/project_upgrades/UpgradeHelperV11_20190926T134241.000000Z.json[0m[0m\
"""
        in stdout
    )

    assert (
        """
[31mThe Upgrade Helper was unable to perform a complete project upgrade. Next steps:[0m

    - Please perform any manual steps outlined in the Upgrade Overview and/or Upgrade Report above
    - When complete, increment the config_version key in your [36mgreat_expectations.yml[0m to [36m2.0[0m

To learn more about the upgrade process, visit [36mhttps://docs.greatexpectations.io/en/latest/how_to_guides/migrating_versions.html[0m
"""
        in stdout
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
                                20200430T191246.763896Z/
                                    c3b4c5df224fef4b1a056a0f3b93aba5.html
        logs/
            project_upgrades/
                UpgradeHelperV11_20190926T134241.000000Z.json
        validations/
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
            "../../test_fixtures/upgrade_helper/UpgradeHelperV11_manual_steps_upgrade_log.json",
        )
    ) as f:
        expected_upgrade_log_dict = json.load(f)
        expected_upgrade_log_str = json.dumps(expected_upgrade_log_dict)
        expected_upgrade_log_str = expected_upgrade_log_str.replace(
            "GE_PROJECT_DIR", v10_project_directory
        )
        expected_upgrade_log_dict = json.loads(expected_upgrade_log_str)

    with open(
        f"{v10_project_directory}/uncommitted/logs/project_upgrades/UpgradeHelperV11_20190926T134241.000000Z"
        f".json"
    ) as f:
        obs_upgrade_log_dict = json.load(f)

    assert obs_upgrade_log_dict == expected_upgrade_log_dict


@freeze_time("09/26/2019 13:42:41")
def test_project_upgrade_with_exception(v10_project_directory, caplog):
    # test project upgrade that requires manual steps

    # copy v2 yml
    shutil.copy(
        file_relative_path(
            __file__,
            "../../test_fixtures/upgrade_helper/great_expectations_v1_basic_with_exception"
            ".yml",
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
    assert (
        "Your project appears to have an out-of-date config version (1.0) - the version number must be at least 2."
        in stdout
    )
    assert (
        "Would you like to run the Upgrade Helper to bring your project up-to-date? [Y/n]:"
        in stdout
    )
    assert (
        """\
++====================================++
|| UpgradeHelperV11: Upgrade Overview ||
++====================================++

UpgradeHelperV11 will upgrade your project to be compatible with Great Expectations 0.11.x.

**WARNING**: Before proceeding, please make sure you have appropriate backups of your project.

Automated Steps
================

The following Stores and/or Data Docs sites will be upgraded:

    - Validation Stores: validations_store
    - Data Docs Sites: local_site, s3_site_broken

Manual Steps
=============

No manual upgrade steps are required.

Upgrade Confirmation
=====================

Please consult the 0.11.x migration guide for instructions on how to complete any required manual steps or
to learn more about the automated upgrade process:

    https://docs.greatexpectations.io/en/latest/how_to_guides/migrating_versions.html#id1

Would you like to proceed with the project upgrade? [Y/n]:

Upgrading project...\
"""
        in stdout
    )
    assert (
        "The Upgrade Helper encountered some exceptions during the upgrade process."
        in stdout
    )

    assert f"""
Please review the exceptions section of the upgrade log and migrate the affected files manually,
as detailed in the 0.11.x migration guide.

The upgrade log can be found here:

    - {v10_project_directory}/uncommitted/logs/project_upgrades/UpgradeHelperV11_20190926T134241.000000Z.json[0m[0m\
"""
    assert (
        """
[31mThe Upgrade Helper was unable to perform a complete project upgrade. Next steps:[0m

    - Please perform any manual steps outlined in the Upgrade Overview and/or Upgrade Report above
    - When complete, increment the config_version key in your [36mgreat_expectations.yml[0m to [36m2.0[0m

To learn more about the upgrade process, visit [36mhttps://docs.greatexpectations.io/en/latest/how_to_guides/migrating_versions.html[0m
"""
        in stdout
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
                                20200430T191246.763896Z/
                                    c3b4c5df224fef4b1a056a0f3b93aba5.html
        logs/
            project_upgrades/
                UpgradeHelperV11_20190926T134241.000000Z.json
        validations/
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
            "../../test_fixtures/upgrade_helper/UpgradeHelperV11_basic_upgrade_with_exception_log"
            ".json",
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
        f"{v10_project_directory}/uncommitted/logs/project_upgrades/UpgradeHelperV11_20190926T134241.000000Z"
        f".json"
    ) as f:
        obs_upgrade_log_dict = json.load(f)

    assert obs_upgrade_log_dict == expected_upgrade_log_dict
