
import copy
import os
import sys
from typing import Any, Dict, List, Optional, Tuple, Union
import click
from great_expectations import DataContext
from great_expectations import exceptions as ge_exceptions
from great_expectations.cli import toolkit
from great_expectations.cli.mark import Mark as mark
from great_expectations.cli.pretty_printing import cli_message, cli_message_list
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import BatchRequest
from great_expectations.core.usage_statistics.anonymizers.types.base import CLISuiteInteractiveFlagCombinations
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.core.usage_statistics.usage_statistics import edit_expectation_suite_usage_statistics
from great_expectations.core.usage_statistics.util import send_usage_message
from great_expectations.render.renderer.v3.suite_edit_notebook_renderer import SuiteEditNotebookRenderer
from great_expectations.render.renderer.v3.suite_profile_notebook_renderer import SuiteProfileNotebookRenderer
try:
    from sqlalchemy.exc import SQLAlchemyError
except ImportError:
    SQLAlchemyError = ge_exceptions.ProfilerError

@click.group()
@click.pass_context
def suite(ctx: click.Context) -> None:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Expectation Suite operations'
    ctx.obj.data_context = ctx.obj.get_data_context_from_config_file()
    cli_event_noun: str = 'suite'
    (begin_event_name, end_event_name) = UsageStatsEvents.get_cli_begin_and_end_event_names(noun=cli_event_noun, verb=ctx.invoked_subcommand)
    send_usage_message(data_context=ctx.obj.data_context, event=begin_event_name, success=True)
    ctx.obj.usage_event_end = end_event_name

@suite.command(name='new')
@click.option('--expectation-suite', '-e', default=None, help='Expectation suite name.')
@click.option('--interactive', '-i', 'interactive_flag', is_flag=True, default=False, help='Use a batch of data to create expectations against (interactive mode).\n')
@click.option('--manual', '-m', 'manual_flag', is_flag=True, default=False, help='Do not use a batch of data to create expectations against (manual mode).\n')
@click.option('--profile', '-p', 'profiler_name', is_flag=False, flag_value='', default=None, help='Generate a starting expectation suite automatically so you can refine it further.\n    Takes in an optional name; if provided, a profiler of that name will be retrieved from your Data Context.\n    Assumes --interactive flag.\n')
@click.option('--batch-request', '-br', help='Arguments to be provided to get_batch when loading the data asset.  Must be a path to a valid JSON file.\nAssumes --interactive flag.\n', default=None)
@click.option('--no-jupyter', '-nj', is_flag=True, default=False, help='By default launch jupyter notebooks, unless you specify --no-jupyter flag.')
@click.pass_context
def suite_new(ctx: click.Context, expectation_suite: Optional[str], interactive_flag: bool, manual_flag: bool, profiler_name: Optional[str], batch_request: Optional[str], no_jupyter: bool) -> None:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    '\n    Create a new Expectation Suite.\n    Edit in jupyter notebooks, or skip with the --no-jupyter flag.\n    '
    context: DataContext = ctx.obj.data_context
    usage_event_end: str = ctx.obj.usage_event_end
    profile: bool = _determine_profile(profiler_name)
    (interactive_mode, profile) = _process_suite_new_flags_and_prompt(context=context, usage_event_end=usage_event_end, interactive_flag=interactive_flag, manual_flag=manual_flag, profile=profile, batch_request=batch_request)
    _suite_new_workflow(context=context, expectation_suite_name=expectation_suite, interactive_mode=interactive_mode, profile=profile, profiler_name=profiler_name, no_jupyter=no_jupyter, usage_event=usage_event_end, batch_request=batch_request)

def _determine_profile(profiler_name: Optional[str]) -> bool:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    profile: bool = (profiler_name is not None)
    if profile:
        if profiler_name:
            msg = 'Since you supplied a profiler name, utilizing the RuleBasedProfiler'
        else:
            msg = 'Since you did not supply a profiler name, defaulting to the UserConfigurableProfiler'
        cli_message(string=f'<yellow>{msg}</yellow>')
    return profile

def _process_suite_new_flags_and_prompt(context: DataContext, usage_event_end: str, interactive_flag: bool, manual_flag: bool, profile: bool, batch_request: Optional[str]=None) -> Tuple[(CLISuiteInteractiveFlagCombinations, bool)]:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    '\n    Process various optional suite new flags and prompt if there is not enough information from the flags.\n    Args:\n        context: Data Context for use in sending error messages if any\n        usage_event_end: event name for ending usage stats message\n        interactive_flag: --interactive from the `suite new` CLI command\n        manual_flag: --manual from the `suite new` CLI command\n        profile: --profile from the `suite new` CLI command\n        batch_request: --batch-request from the `suite new` CLI command\n\n    Returns:\n        Tuple with keys of processed parameters and boolean values\n    '
    interactive_mode: Optional[CLISuiteInteractiveFlagCombinations]
    interactive_mode = _suite_convert_flags_to_interactive_mode(interactive_flag, manual_flag)
    error_message: Optional[str] = None
    if (interactive_mode == CLISuiteInteractiveFlagCombinations.ERROR_INTERACTIVE_TRUE_MANUAL_TRUE):
        error_message = 'Please choose either --interactive or --manual, you may not choose both.'
    _exit_early_if_error(error_message, context, usage_event_end, interactive_mode)
    if _suite_new_user_provided_any_flag(interactive_mode, profile, batch_request):
        interactive_mode = _suite_new_process_profile_and_batch_request_flags(interactive_mode, profile, batch_request)
    else:
        (interactive_mode, profile) = _suite_new_mode_from_prompt(profile)
    return (interactive_mode, profile)

def _suite_new_workflow(context: DataContext, expectation_suite_name: Optional[str], interactive_mode: CLISuiteInteractiveFlagCombinations, profile: bool, profiler_name: Optional[str], no_jupyter: bool, usage_event: str, batch_request: Optional[Union[(str, Dict[(str, Union[(str, int, Dict[(str, Any)])])])]]=None) -> None:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    try:
        datasource_name: Optional[str] = None
        data_asset_name: Optional[str] = None
        additional_batch_request_args: Optional[Dict[(str, Union[(str, int, Dict[(str, Any)])])]] = {'limit': 1000}
        if interactive_mode.value['interactive_flag']:
            if ((batch_request is not None) and isinstance(batch_request, str)):
                batch_request = toolkit.get_batch_request_from_json_file(batch_request_json_file_path=batch_request, data_context=context, usage_event=usage_event, suppress_usage_message=False)
            if (not batch_request):
                batch_request = toolkit.get_batch_request_using_datasource_name(data_context=context, datasource_name=datasource_name, usage_event=usage_event, suppress_usage_message=False, additional_batch_request_args=additional_batch_request_args)
                additional_batch_request_args = {}
            data_asset_name = batch_request.get('data_asset_name')
        else:
            batch_request = None
        suite: ExpectationSuite = toolkit.get_or_create_expectation_suite(expectation_suite_name=expectation_suite_name, data_context=context, data_asset_name=data_asset_name, usage_event=usage_event, suppress_usage_message=False, batch_request=batch_request, create_if_not_exist=True)
        expectation_suite_name = suite.expectation_suite_name
        toolkit.add_citation_with_batch_request(data_context=context, expectation_suite=suite, batch_request=batch_request)
        send_usage_message(data_context=context, event=usage_event, event_payload=interactive_mode.value, success=True)
        if batch_request:
            datasource_name = batch_request.get('datasource_name')
        usage_event = 'cli.suite.edit.begin'
        _suite_edit_workflow(context=context, expectation_suite_name=expectation_suite_name, profile=profile, profiler_name=profiler_name, usage_event=usage_event, interactive_mode=interactive_mode, no_jupyter=no_jupyter, create_if_not_exist=True, datasource_name=datasource_name, batch_request=batch_request, additional_batch_request_args=additional_batch_request_args, suppress_usage_message=True, assume_yes=False)
    except (ge_exceptions.DataContextError, ge_exceptions.ProfilerError, ValueError, OSError, SQLAlchemyError) as e:
        cli_message(string=f'<red>{e}</red>')
        send_usage_message(data_context=context, event=usage_event, event_payload=interactive_mode.value, success=False)
        sys.exit(1)
    except Exception as e:
        send_usage_message(data_context=context, event=usage_event, event_payload=interactive_mode.value, success=False)
        raise e

def _suite_convert_flags_to_interactive_mode(interactive_flag: bool, manual_flag: bool) -> CLISuiteInteractiveFlagCombinations:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    if ((interactive_flag is True) and (manual_flag is True)):
        interactive_mode = CLISuiteInteractiveFlagCombinations.ERROR_INTERACTIVE_TRUE_MANUAL_TRUE
    elif ((interactive_flag is False) and (manual_flag is False)):
        interactive_mode = CLISuiteInteractiveFlagCombinations.UNPROMPTED_INTERACTIVE_FALSE_MANUAL_FALSE
    elif ((interactive_flag is True) and (manual_flag is False)):
        interactive_mode = CLISuiteInteractiveFlagCombinations.UNPROMPTED_INTERACTIVE_TRUE_MANUAL_FALSE
    elif ((interactive_flag is False) and (manual_flag is True)):
        interactive_mode = CLISuiteInteractiveFlagCombinations.UNPROMPTED_INTERACTIVE_FALSE_MANUAL_TRUE
    else:
        interactive_mode = CLISuiteInteractiveFlagCombinations.UNKNOWN
    return interactive_mode

def _suite_new_process_profile_and_batch_request_flags(interactive_mode: CLISuiteInteractiveFlagCombinations, profile: bool, batch_request: Optional[str]) -> CLISuiteInteractiveFlagCombinations:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    interactive_flag = interactive_mode.value['interactive_flag']
    if profile:
        if (interactive_flag is None):
            cli_message('<green>Entering interactive mode since you passed the --profile flag</green>')
            interactive_mode = CLISuiteInteractiveFlagCombinations.UNPROMPTED_OVERRIDE_INTERACTIVE_FALSE_MANUAL_FALSE_PROFILE_TRUE
        elif (interactive_flag is True):
            interactive_mode = CLISuiteInteractiveFlagCombinations.UNPROMPTED_OVERRIDE_INTERACTIVE_TRUE_MANUAL_FALSE_PROFILE_TRUE
        elif (interactive_flag is False):
            cli_message('<yellow>Warning: Ignoring the --manual flag and entering interactive mode since you passed the --profile flag</yellow>')
            interactive_mode = CLISuiteInteractiveFlagCombinations.UNPROMPTED_OVERRIDE_INTERACTIVE_FALSE_MANUAL_TRUE_PROFILE_TRUE
    elif (batch_request is not None):
        if (interactive_flag is None):
            cli_message('<green>Entering interactive mode since you passed the --batch-request flag</green>')
            interactive_mode = CLISuiteInteractiveFlagCombinations.UNPROMPTED_OVERRIDE_INTERACTIVE_FALSE_MANUAL_FALSE_BATCH_REQUEST_SPECIFIED
        elif (interactive_flag is False):
            cli_message('<yellow>Warning: Ignoring the --manual flag and entering interactive mode since you passed the --batch-request flag</yellow>')
            interactive_mode = CLISuiteInteractiveFlagCombinations.UNPROMPTED_OVERRIDE_INTERACTIVE_FALSE_MANUAL_TRUE_BATCH_REQUEST_SPECIFIED
    return interactive_mode

def _exit_early_if_error(error_message: Optional[str], context: DataContext, usage_event_end: str, interactive_mode: CLISuiteInteractiveFlagCombinations) -> None:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    if (error_message is not None):
        cli_message(string=f'<red>{error_message}</red>')
        send_usage_message(data_context=context, event=usage_event_end, event_payload=interactive_mode.value, success=False)
        sys.exit(1)

def _suite_new_user_provided_any_flag(interactive_mode: CLISuiteInteractiveFlagCombinations, profile: bool, batch_request: Optional[str]) -> bool:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    user_provided_any_flag_skip_prompt: bool = any(((interactive_mode.value['interactive_flag'] is not None), (profile is True), (batch_request is not None)))
    return user_provided_any_flag_skip_prompt

def _suite_new_mode_from_prompt(profile: bool) -> Tuple[(CLISuiteInteractiveFlagCombinations, bool)]:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    suite_create_method: str = click.prompt('\nHow would you like to create your Expectation Suite?\n    1. Manually, without interacting with a sample batch of data (default)\n    2. Interactively, with a sample batch of data\n    3. Automatically, using a profiler\n', type=click.Choice(['1', '2', '3']), show_choices=False, default='1', show_default=False)
    if (suite_create_method == ''):
        profile = False
        interactive_mode = CLISuiteInteractiveFlagCombinations.PROMPTED_CHOICE_DEFAULT
    elif (suite_create_method == '1'):
        profile = False
        interactive_mode = CLISuiteInteractiveFlagCombinations.PROMPTED_CHOICE_FALSE
    elif (suite_create_method == '2'):
        profile = False
        interactive_mode = CLISuiteInteractiveFlagCombinations.PROMPTED_CHOICE_TRUE_PROFILE_FALSE
    elif (suite_create_method == '3'):
        profile = True
        interactive_mode = CLISuiteInteractiveFlagCombinations.PROMPTED_CHOICE_TRUE_PROFILE_TRUE
    else:
        interactive_mode = CLISuiteInteractiveFlagCombinations.UNKNOWN
    return (interactive_mode, profile)

@suite.command(name='edit')
@click.argument('expectation_suite')
@click.option('--interactive', '-i', 'interactive_flag', is_flag=True, default=False, help='Allows to specify explicitly whether or not a batch of data is available to reason about using the language\nof expectations; otherwise, best effort is made to determine this automatically (falling back to False).  Assumed with\n--datasource-name option and with --batch-request option.\n')
@click.option('--manual', '-m', 'manual_flag', is_flag=True, default=False, help='Do not use a batch of data to create expectations against(manual mode).\n')
@click.option('--datasource-name', '-ds', default=None, help='The name of the datasource. Assumes --interactive flag.  Incompatible with --batch-request option.\n')
@click.option('--batch-request', '-br', help='Arguments to be provided to get_batch when loading the data asset.  Must be a path to a valid JSON file.\nAssumes --interactive flag.  Incompatible with --datasource-name option.\n', default=None)
@click.option('--no-jupyter', '-nj', is_flag=True, default=False, help='By default launch jupyter notebooks, unless you specify --no-jupyter flag.')
@click.pass_context
def suite_edit(ctx: click.Context, expectation_suite: str, interactive_flag: bool, manual_flag: bool, datasource_name: Optional[str], batch_request: Optional[str], no_jupyter: bool) -> None:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    '\n    Edit an existing Expectation Suite.\n\n    The SUITE argument is required. This is the name you gave to the suite\n    when you created it.\n\n    The edit command will help you specify a batch interactively. Or you can\n    specify them manually by providing --batch-request in valid JSON format.\n\n    Read more about specifying batches of data in the documentation: https://docs.greatexpectations.io/\n    '
    context: DataContext = ctx.obj.data_context
    usage_event_end: str = ctx.obj.usage_event_end
    interactive_mode: CLISuiteInteractiveFlagCombinations = _process_suite_edit_flags_and_prompt(context=context, usage_event_end=usage_event_end, interactive_flag=interactive_flag, manual_flag=manual_flag, datasource_name=datasource_name, batch_request=batch_request)
    additional_batch_request_args: Optional[Dict[(str, Union[(str, int, Dict[(str, Any)])])]] = {'limit': 1000}
    _suite_edit_workflow(context=context, expectation_suite_name=expectation_suite, profile=False, profiler_name=None, usage_event=usage_event_end, interactive_mode=interactive_mode, no_jupyter=no_jupyter, create_if_not_exist=False, datasource_name=datasource_name, batch_request=batch_request, additional_batch_request_args=additional_batch_request_args, suppress_usage_message=False, assume_yes=False)

def _process_suite_edit_flags_and_prompt(context: DataContext, usage_event_end: str, interactive_flag: bool, manual_flag: bool, datasource_name: Optional[str]=None, batch_request: Optional[str]=None) -> CLISuiteInteractiveFlagCombinations:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    '\n    Process various optional suite edit flags and prompt if there is not enough information from the flags.\n    Args:\n        context: Data Context for use in sending error messages if any\n        usage_event_end: event name for ending usage stats message\n        interactive_flag: --interactive from the `suite new` CLI command\n        manual_flag: --manual from the `suite new` CLI command\n        datasource_name: --datasource-name from the `suite new` CLI command\n        batch_request: --batch-request from the `suite new` CLI command\n\n    Returns:\n        boolean of whether to enter interactive mode\n    '
    error_message: Optional[str] = None
    interactive_mode: CLISuiteInteractiveFlagCombinations
    interactive_mode = _suite_convert_flags_to_interactive_mode(interactive_flag, manual_flag)
    if (interactive_mode == CLISuiteInteractiveFlagCombinations.ERROR_INTERACTIVE_TRUE_MANUAL_TRUE):
        error_message = 'Please choose either --interactive or --manual, you may not choose both.'
    if ((datasource_name is not None) and (batch_request is not None)):
        error_message = 'Only one of --datasource-name DATASOURCE_NAME and --batch-request <path to JSON file> options can be used.\n'
        interactive_mode = CLISuiteInteractiveFlagCombinations.ERROR_DATASOURCE_SPECIFIED_BATCH_REQUEST_SPECIFIED
    if (error_message is not None):
        cli_message(string=f'<red>{error_message}</red>')
        send_usage_message(data_context=context, event=usage_event_end, event_payload=interactive_mode.value, success=False)
        sys.exit(1)
    user_provided_any_flag_skip_prompt: bool = any(((interactive_mode.value['interactive_flag'] is not None), (datasource_name is not None), (batch_request is not None)))
    if user_provided_any_flag_skip_prompt:
        if (datasource_name is not None):
            if (interactive_mode.value['interactive_flag'] is None):
                cli_message('<green>Entering interactive mode since you passed the --datasource-name flag</green>')
                interactive_mode = CLISuiteInteractiveFlagCombinations.UNPROMPTED_OVERRIDE_INTERACTIVE_FALSE_MANUAL_FALSE_DATASOURCE_SPECIFIED
            elif (interactive_mode.value['interactive_flag'] is True):
                interactive_mode = CLISuiteInteractiveFlagCombinations.UNPROMPTED_OVERRIDE_INTERACTIVE_TRUE_MANUAL_FALSE_DATASOURCE_SPECIFIED
            elif (interactive_mode.value['interactive_flag'] is False):
                cli_message('<yellow>Warning: Ignoring the --manual flag and entering interactive mode since you passed the --datasource-name flag</yellow>')
                interactive_mode = CLISuiteInteractiveFlagCombinations.UNPROMPTED_OVERRIDE_INTERACTIVE_FALSE_MANUAL_TRUE_DATASOURCE_SPECIFIED
        elif (batch_request is not None):
            if (interactive_mode.value['interactive_flag'] is None):
                cli_message('<green>Entering interactive mode since you passed the --batch-request flag</green>')
                interactive_mode = CLISuiteInteractiveFlagCombinations.UNPROMPTED_OVERRIDE_INTERACTIVE_FALSE_MANUAL_FALSE_BATCH_REQUEST_SPECIFIED
            elif (interactive_mode.value['interactive_flag'] is False):
                cli_message('<yellow>Warning: Ignoring the --manual flag and entering interactive mode since you passed the --batch-request flag</yellow>')
                interactive_mode = CLISuiteInteractiveFlagCombinations.UNPROMPTED_OVERRIDE_INTERACTIVE_FALSE_MANUAL_TRUE_BATCH_REQUEST_SPECIFIED
    else:
        suite_edit_method: str = click.prompt('\nHow would you like to edit your Expectation Suite?\n    1. Manually, without interacting with a sample batch of data (default)\n    2. Interactively, with a sample batch of data\n', type=click.Choice(['1', '2']), show_choices=False, default='1', show_default=False)
        if (suite_edit_method == ''):
            interactive_mode = CLISuiteInteractiveFlagCombinations.PROMPTED_CHOICE_DEFAULT
        if (suite_edit_method == '1'):
            interactive_mode = CLISuiteInteractiveFlagCombinations.PROMPTED_CHOICE_FALSE
        elif (suite_edit_method == '2'):
            interactive_mode = CLISuiteInteractiveFlagCombinations.PROMPTED_CHOICE_TRUE
    return interactive_mode

def _suite_edit_workflow(context: DataContext, expectation_suite_name: str, profile: bool, profiler_name: Optional[str], usage_event: str, interactive_mode: CLISuiteInteractiveFlagCombinations, no_jupyter: bool, create_if_not_exist: bool=False, datasource_name: Optional[str]=None, batch_request: Optional[Union[(str, Dict[(str, Union[(str, int, Dict[(str, Any)])])])]]=None, additional_batch_request_args: Optional[Dict[(str, Union[(str, int, Dict[(str, Any)])])]]=None, suppress_usage_message: bool=False, assume_yes: bool=False) -> None:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    if suppress_usage_message:
        usage_event = None
    suite: ExpectationSuite = toolkit.load_expectation_suite(data_context=context, expectation_suite_name=expectation_suite_name, usage_event=usage_event, create_if_not_exist=create_if_not_exist)
    try:
        if (interactive_mode.value['interactive_flag'] or profile):
            batch_request_from_citation_is_up_to_date: bool = True
            batch_request_from_citation: Optional[Union[(str, Dict[(str, Union[(str, Dict[(str, Any)])])])]] = toolkit.get_batch_request_from_citations(expectation_suite=suite)
            if ((batch_request is not None) and isinstance(batch_request, str)):
                batch_request = toolkit.get_batch_request_from_json_file(batch_request_json_file_path=batch_request, data_context=context, usage_event=usage_event, suppress_usage_message=suppress_usage_message)
                if (batch_request != batch_request_from_citation):
                    batch_request_from_citation_is_up_to_date = False
            if (not (batch_request and isinstance(batch_request, dict) and BatchRequest(**batch_request))):
                if (batch_request_from_citation and isinstance(batch_request_from_citation, dict) and BatchRequest(**batch_request_from_citation)):
                    batch_request = copy.deepcopy(batch_request_from_citation)
                else:
                    batch_request = toolkit.get_batch_request_using_datasource_name(data_context=context, datasource_name=datasource_name, usage_event=usage_event, suppress_usage_message=False, additional_batch_request_args=additional_batch_request_args)
                    if (batch_request != batch_request_from_citation):
                        batch_request_from_citation_is_up_to_date = False
            if (not batch_request_from_citation_is_up_to_date):
                toolkit.add_citation_with_batch_request(data_context=context, expectation_suite=suite, batch_request=batch_request)
        notebook_name: str = f'edit_{expectation_suite_name}.ipynb'
        notebook_path: str = _get_notebook_path(context, notebook_name)
        renderer: SuiteProfileNotebookRenderer
        if profile:
            if (not assume_yes):
                toolkit.prompt_profile_to_create_a_suite(data_context=context, expectation_suite_name=expectation_suite_name)
            renderer = SuiteProfileNotebookRenderer(context=context, expectation_suite_name=expectation_suite_name, profiler_name=profiler_name, batch_request=batch_request)
            renderer.render_to_disk(notebook_file_path=notebook_path)
        else:
            renderer = SuiteEditNotebookRenderer.from_data_context(data_context=context)
            renderer.render_to_disk(suite=suite, notebook_file_path=notebook_path, batch_request=batch_request)
        if no_jupyter:
            cli_message(string=f'To continue editing this suite, run <green>jupyter notebook {notebook_path}</green>')
        else:
            cli_message(string='<green>Opening a notebook for you now to edit your expectation suite!\nIf you wish to avoid this you can add the `--no-jupyter` flag.</green>\n\n')
        payload: dict = edit_expectation_suite_usage_statistics(data_context=context, expectation_suite_name=suite.expectation_suite_name, interactive_mode=interactive_mode)
        if (not suppress_usage_message):
            send_usage_message(data_context=context, event=usage_event, event_payload=payload, success=True)
        if (not no_jupyter):
            toolkit.launch_jupyter_notebook(notebook_path=notebook_path)
    except (ge_exceptions.DataContextError, ge_exceptions.ProfilerError, ValueError, OSError, SQLAlchemyError) as e:
        cli_message(string=f'<red>{e}</red>')
        if (not suppress_usage_message):
            send_usage_message(data_context=context, event=usage_event, event_payload=interactive_mode.value, success=False)
        sys.exit(1)
    except Exception as e:
        if (not suppress_usage_message):
            send_usage_message(data_context=context, event=usage_event, event_payload=interactive_mode.value, success=False)
        raise e

@mark.cli_as_deprecation
@suite.command(name='demo')
@click.pass_context
def suite_demo(ctx: click.Context) -> None:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'This command is not supported in the v3 (Batch Request) API.'
    context: DataContext = ctx.obj.data_context
    usage_event_end: str = ctx.obj.usage_event_end
    send_usage_message(data_context=context, event=usage_event_end, success=True)
    cli_message(string='This command is not supported in the v3 (Batch Request) API. Please use `suite new` instead.')

@suite.command(name='delete')
@click.argument('suite')
@click.pass_context
def suite_delete(ctx: click.Context, suite: str) -> None:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    '\n    Delete an Expectation Suite from the Expectation Store.\n    '
    context: DataContext = ctx.obj.data_context
    usage_event_end: str = ctx.obj.usage_event_end
    try:
        suite_names: List[str] = context.list_expectation_suite_names()
    except Exception as e:
        send_usage_message(data_context=context, event=usage_event_end, success=False)
        raise e
    if (not suite_names):
        toolkit.exit_with_failure_message_and_stats(data_context=context, usage_event=usage_event_end, suppress_usage_message=False, message='<red>No expectation suites found in the project.</red>')
    if (suite not in suite_names):
        toolkit.exit_with_failure_message_and_stats(data_context=context, usage_event=usage_event_end, suppress_usage_message=False, message=f'<red>No expectation suite named {suite} found.</red>')
    if (not (ctx.obj.assume_yes or toolkit.confirm_proceed_or_exit(exit_on_no=False, data_context=context, usage_stats_event=usage_event_end))):
        cli_message(string=f'Suite `{suite}` was not deleted.')
        sys.exit(0)
    context.delete_expectation_suite(suite)
    cli_message(string=f'Deleted the expectation suite named: {suite}')
    send_usage_message(data_context=context, event=usage_event_end, success=True)

@suite.command(name='list')
@click.pass_context
def suite_list(ctx: click.Context) -> None:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'List existing Expectation Suites.'
    context: DataContext = ctx.obj.data_context
    usage_event_end: str = ctx.obj.usage_event_end
    try:
        suite_names: List[str] = context.list_expectation_suite_names()
    except Exception as e:
        send_usage_message(data_context=context, event=usage_event_end, success=False)
        raise e
    suite_names_styled: List[str] = [f' - <cyan>{suite_name}</cyan>' for suite_name in suite_names]
    if (len(suite_names_styled) == 0):
        cli_message(string='No Expectation Suites found')
        send_usage_message(data_context=context, event=usage_event_end, success=True)
        return
    list_intro_string: str
    if (len(suite_names_styled) == 1):
        list_intro_string = '1 Expectation Suite found:'
    else:
        list_intro_string = f'{len(suite_names_styled)} Expectation Suites found:'
    cli_message_list(string_list=suite_names_styled, list_intro_string=list_intro_string)
    send_usage_message(data_context=context, event=usage_event_end, success=True)

def _get_notebook_path(context: DataContext, notebook_name: str) -> str:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    return os.path.abspath(os.path.join(context.root_directory, context.GE_EDIT_NOTEBOOK_DIR, notebook_name))
