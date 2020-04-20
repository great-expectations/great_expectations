import datetime
import os
import sys

import click

from great_expectations import DataContext, exceptions as ge_exceptions
from great_expectations.cli.datasource import select_datasource, \
    get_batch_kwargs
from great_expectations.cli.docs import build_docs
from great_expectations.cli.util import cli_message
from great_expectations.data_context.types.resource_identifiers import \
    ValidationResultIdentifier
from great_expectations.profile import BasicSuiteBuilderProfiler


def create_expectation_suite(
    context,
    datasource_name=None,
    batch_kwargs_generator_name=None,
    generator_asset=None,
    batch_kwargs=None,
    expectation_suite_name=None,
    additional_batch_kwargs=None,
    empty_suite=False,
    show_intro_message=False,
    open_docs=False,
    profiler_configuration="demo"
):
    """
    Create a new expectation suite.

    :return: a tuple: (success, suite name)
    """

    msg_intro = "\n<cyan>========== Create sample Expectations ==========</cyan>\n\n"
    msg_some_data_assets_not_found = """Some of the data assets you specified were not found: {0:s}    
    """
    msg_prompt_what_will_profiler_do = """
Great Expectations will choose a couple of columns and generate expectations about them
to demonstrate some examples of assertions you can make about your data. 
    
Press Enter to continue
"""

    msg_prompt_expectation_suite_name = """
Name the new expectation suite"""

    msg_suite_already_exists = "<red>An expectation suite named `{}` already exists. If you intend to edit the suite please use `great_expectations suite edit {}`.</red>"

    if show_intro_message and not empty_suite:
        cli_message(msg_intro)

    data_source = select_datasource(context, datasource_name=datasource_name)
    if data_source is None:
        # select_datasource takes care of displaying an error message, so all is left here is to exit.
        sys.exit(1)

    datasource_name = data_source.name

    existing_suite_names = [expectation_suite_id.expectation_suite_name for expectation_suite_id in context.list_expectation_suites()]

    if expectation_suite_name in existing_suite_names:
        cli_message(
            msg_suite_already_exists.format(
                expectation_suite_name,
                expectation_suite_name
            )
        )
        sys.exit(1)

    if batch_kwargs_generator_name is None or generator_asset is None or batch_kwargs is None:
        datasource_name, batch_kwargs_generator_name, generator_asset, batch_kwargs = get_batch_kwargs(
            context,
            datasource_name=datasource_name,
            batch_kwargs_generator_name=batch_kwargs_generator_name,
            generator_asset=generator_asset,
            additional_batch_kwargs=additional_batch_kwargs)
        # In this case, we have "consumed" the additional_batch_kwargs
        additional_batch_kwargs = {}

    if expectation_suite_name is None:
        if generator_asset:
            default_expectation_suite_name = "{}.warning".format(generator_asset)
        elif "query" in batch_kwargs:
            default_expectation_suite_name = "query.warning"
        elif "path" in batch_kwargs:
            try:
                # Try guessing a filename
                filename = os.path.split(os.path.normpath(batch_kwargs["path"]))[1]
                # Take all but the last part after the period
                filename = ".".join(filename.split(".")[:-1])
                default_expectation_suite_name = str(filename) + ".warning"
            except (OSError, IndexError):
                default_expectation_suite_name = "warning"
        else:
            default_expectation_suite_name = "warning"
        while True:
            expectation_suite_name = click.prompt(msg_prompt_expectation_suite_name, default=default_expectation_suite_name, show_default=True)
            if expectation_suite_name in existing_suite_names:
                cli_message(
                    msg_suite_already_exists.format(
                        expectation_suite_name,
                        expectation_suite_name
                    )
                )
            else:
                break

    if empty_suite:
        suite = context.create_expectation_suite(expectation_suite_name, overwrite_existing=False)
        suite.add_citation(comment="New suite added via CLI", batch_kwargs=batch_kwargs)
        context.save_expectation_suite(suite, expectation_suite_name)
        return True, expectation_suite_name

    click.prompt(msg_prompt_what_will_profiler_do, default=True, show_default=False)

    cli_message("\nGenerating example Expectation Suite...")
    run_id = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S.%fZ")

    profiling_results = context.profile_data_asset(
        datasource_name,
        batch_kwargs_generator_name=batch_kwargs_generator_name,
        data_asset_name=generator_asset,
        batch_kwargs=batch_kwargs,
        profiler=BasicSuiteBuilderProfiler,
        profiler_configuration=profiler_configuration,
        expectation_suite_name=expectation_suite_name,
        run_id=run_id,
        additional_batch_kwargs=additional_batch_kwargs
    )

    if profiling_results['success']:
        build_docs(context, view=False)
        if open_docs:  # This is mostly to keep tests from spawning windows
            try:
                # TODO this is really brittle and not covered in tests
                validation_result = profiling_results["results"][0][1]
                validation_result_identifier = ValidationResultIdentifier.from_object(validation_result)
                context.open_data_docs(resource_identifier=validation_result_identifier)
            except (KeyError, IndexError):
                context.open_data_docs()

        return True, expectation_suite_name

    if profiling_results['error']['code'] == DataContext.PROFILING_ERROR_CODE_SPECIFIED_DATA_ASSETS_NOT_FOUND:
        raise ge_exceptions.DataContextError(msg_some_data_assets_not_found.format(",".join(profiling_results['error']['not_found_data_assets'])))
    if not profiling_results['success']:  # unknown error
        raise ge_exceptions.DataContextError("Unknown profiling error code: " + profiling_results['error']['code'])