import os
import sys

import click

from great_expectations import DataContext, exceptions as ge_exceptions
from great_expectations.cli.datasource import (
    create_expectation_suite as create_expectation_suite_impl,
    add_datasource as add_datasource_impl,
)
from great_expectations.cli.init_messages import (
    GREETING,
    PROJECT_IS_COMPLETE,
    BUILD_DOCS_PROMPT,
    LETS_BEGIN_PROMPT,
    RUN_INIT_AGAIN,
    SLACK_SETUP_INTRO,
    SLACK_SETUP_PROMPT,
    SLACK_LATER,
    SLACK_WEBHOOK_PROMPT,
    SLACK_SETUP_COMPLETE,
    COMPLETE_ONBOARDING_PROMPT,
    ONBOARDING_COMPLETE,
)
from great_expectations.cli.logging import logger
from great_expectations.cli.util import cli_message, is_sane_slack_webhook
from great_expectations.core import (
    NamespaceAwareExpectationSuite,
    ExpectationSuiteValidationResult,
)
from great_expectations.render.renderer.notebook_renderer import NotebookRenderer


@click.command()
@click.option(
    "--target_directory",
    "-d",
    default="./",
    help="The root of the project directory where you want to initialize Great Expectations.",
)
@click.option(
    # Note this --no-view option is mostly here for tests
    "--view/--no-view",
    help="By default open in browser unless you specify the --no-view flag",
    default=True,
)
def init(target_directory, view):
    """
    Initialize a new Great Expectations project.

    This guided input walks the user through setting up a new project and also
    onboards a new developer in an existing project.

    It scaffolds directories, sets up notebooks, creates a project file, and
    appends to a `.gitignore` file.
    """
    target_directory = os.path.abspath(target_directory)
    ge_dir = _get_full_path_to_ge_dir(target_directory)
    ge_yml = os.path.join(ge_dir, DataContext.GE_YML)

    cli_message(GREETING)

    # TODO this should be a property
    if os.path.isfile(ge_yml):
        if DataContext.all_uncommitted_directories_exist(
            ge_dir
        ) and DataContext.config_variables_yml_exist(ge_dir):
            # Ensure the context can be instantiated
            try:
                _ = DataContext(ge_dir)
                cli_message(PROJECT_IS_COMPLETE)
            except ge_exceptions.DataContextError as e:
                cli_message("<red>{}</red>".format(e))
                exit(5)
        else:
            _complete_onboarding(target_directory)

        try:
            # if expectations exist, offer to build docs
            context = DataContext(ge_dir)
            if context.list_expectation_suite_keys():
                if click.confirm(BUILD_DOCS_PROMPT, default=True):
                    context.build_data_docs()
                    context.open_data_docs()
        except ge_exceptions.DataContextError as e:
            cli_message("<red>{}</red>".format(e))
    else:
        if not click.confirm(LETS_BEGIN_PROMPT, default=True):
            cli_message(RUN_INIT_AGAIN)
            exit(0)
        try:
            context, datasource_name, data_source_type = _create_new_project(
                target_directory
            )
            if not datasource_name:  # no datasource was created
                return

            # we need only one of the values returned here - profiling_results
            (
                datasource_name,
                generator_name,
                data_asset_name,
                batch_kwargs,
                profiling_results,
            ) = create_expectation_suite_impl(
                context,
                datasource_name=datasource_name,
                show_intro_message=False,
                additional_batch_kwargs={"limit": 1000},
                open_docs=view,
            )

            notebook_renderer = NotebookRenderer()

            for result in profiling_results["results"]:
                # TODO brittle
                suite = result[0]
                assert isinstance(suite, NamespaceAwareExpectationSuite)
                suite_name = suite.expectation_suite_name

                validation_result = result[1]
                assert isinstance(validation_result, ExpectationSuiteValidationResult)
                batch_kwargs = validation_result.meta.get("batch_kwargs")
                data_asset_identifier = validation_result.meta.get("data_asset_name")
                human_data_asset_name = data_asset_identifier.generator_asset

                logger.debug(
                    f"\nRendering a notebook for {human_data_asset_name} and suite {suite_name}"
                )
                logger.debug(f"batch_kwargs: {batch_kwargs}")
                logger.debug(f"datasource_name: {datasource_name}")

                notebook_name = f"{human_data_asset_name}_{suite_name}.ipynb"
                notebook_path = os.path.join(
                    context.root_directory, "notebooks", notebook_name
                )
                notebook_renderer.render_to_disk(suite, batch_kwargs, notebook_path)

                # TODO maybe loop over profiling results since they contain suites and validation results
                # for data_asset in data_assets:
                #     # TODO brittle
                #     # print("\n\n\n")
                #     # print(profiling_results)
                #     # print("\n\n\n")
                #     # sys.exit(1)
                #     # batch_kwargs = profiling_results["results"][0][1]["meta"]["batch_kwargs"]
                #
                #     print(f"data_asset: {data_asset}")
                #     suite = context.get_expectation_suite(
                #         data_asset_name=data_asset,
                #         expectation_suite_name=str(profiler.__name__)
                #     )
                #     suite_name = suite.expectation_suite_name
                #
                #     print(f"\nRendering a notebook for {data_asset} and suite {suite_name}")
                #
                #     # batch_kwargs = context.build_batch_kwargs(data_asset, partition_id="profiler")
                #     batch_kwargs = context.yield_batch_kwargs(data_asset)
                #     batch_kwargs_by_data_asset[data_asset] = batch_kwargs
                #     print(f"batch_kwargs: {batch_kwargs}")
                #     print(f"datasource_name: {datasource_name}")
                #
                #     notebook_name = f"{data_asset}_{suite_name}.ipynb"
                #     notebook_renderer.render_to_disk(suite, batch_kwargs, os.path.join(context.root_directory, notebook_name))
                cli_message("""\n<cyan>Great Expectations is now set up.</cyan>""")
        except ge_exceptions.DataContextError as e:
            cli_message("<red>{}</red>".format(e))


def _slack_setup(context):
    webhook_url = None
    cli_message(SLACK_SETUP_INTRO)
    if not click.confirm(SLACK_SETUP_PROMPT, default=True):
        cli_message(SLACK_LATER)
        return context
    else:
        webhook_url = click.prompt(SLACK_WEBHOOK_PROMPT, default="")

    while not is_sane_slack_webhook(webhook_url):
        cli_message("That URL was not valid.\n")
        if not click.confirm(SLACK_SETUP_PROMPT, default=True):
            cli_message(SLACK_LATER)
            return context
        webhook_url = click.prompt(SLACK_WEBHOOK_PROMPT, default="")

    context.save_config_variable("validation_notification_slack_webhook", webhook_url)
    cli_message(SLACK_SETUP_COMPLETE)

    return context


def _get_full_path_to_ge_dir(target_directory):
    return os.path.abspath(os.path.join(target_directory, DataContext.GE_DIR))


def _create_new_project(target_directory):
    try:
        context = DataContext.create(target_directory)
        datasource_name, data_source_type = add_datasource_impl(context)
        return context, datasource_name, data_source_type
    except ge_exceptions.DataContextError as err:
        cli_message("<red>{}</red>".format(err.message))
        sys.exit(-1)


def _complete_onboarding(target_dir):
    if click.confirm(COMPLETE_ONBOARDING_PROMPT, default=True):
        DataContext.create(target_dir)
        cli_message(ONBOARDING_COMPLETE)
    else:
        cli_message(RUN_INIT_AGAIN)
