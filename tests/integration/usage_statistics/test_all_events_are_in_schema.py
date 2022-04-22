import ast
import itertools
from typing import Dict, List, cast

from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.core.usage_statistics.schemas import (
    anonymized_usage_statistics_record_schema,
)
from scripts.check_docstring_coverage import collect_functions, gather_source_files


def get_all_events_from_anonymized_usage_statistics_record_schema() -> List[str]:
    event_type_defs: List[dict] = anonymized_usage_statistics_record_schema["oneOf"]
    event_lists: List[List[str]] = [
        event_type_def["properties"]["event"]["enum"]
        for event_type_def in event_type_defs
    ]

    unique_events: List[str] = sorted(list(set(list(itertools.chain(*event_lists)))))

    return unique_events


def get_all_events_from_send_usage_stats_invocations() -> List[str]:
    # TODO: 20220421 AJB Implement me:

    all_func_invocations: Dict[str, List[ast.Call]] = collect_function_invocations(
        "../../../great_expectations"
    )
    all_send_usage_message_function_invocations: List[ast.Call] = []
    for filename, functions in all_func_invocations.items():
        for f in functions:
            if isinstance(f.func, ast.Name):
                if f.func.id == "send_usage_message":
                    all_send_usage_message_function_invocations.append(f)

    all_event_names: List[str] = []

    # TODO: AJB 20220421 Note that currently if a function is invoked with a variable, this includes the variable
    #  name instead of the variable value. We likely need to create an event registry instead.
    # for send_usage_message_function_invocation in all_send_usage_message_function_invocations:
    #     event_names: List[str] = [keyword.value.value for keyword in send_usage_message_function_invocation.keywords if keyword.arg == "event"]
    #     all_event_names.extend(event_names)

    return sorted(list(set(all_event_names)))


def collect_function_invocations(directory_path: str) -> Dict[str, List[ast.Call]]:
    """Using AST, iterate through all source files to parse out function call nodes.

    Args:
        directory_path (str): The directory to traverse through.

    Returns:
        A dictionary that maps source file with the function call nodes contained therin.
    """
    all_func_invocations: Dict[str, List[ast.Call]] = {}

    file_paths: List[str] = gather_source_files(directory_path)
    for file_path in file_paths:
        all_func_invocations[file_path] = _collect_function_invocations(file_path)

    return all_func_invocations


def _collect_function_invocations(file_path: str) -> List[ast.Call]:
    with open(file_path) as f:
        root: ast.Module = ast.parse(f.read())

    return cast(
        List[ast.Call], list(filter(lambda n: isinstance(n, ast.Call), ast.walk(root)))
    )


def get_all_events_from_decorated_methods() -> List[str]:
    all_funcs: Dict[str, List[ast.FunctionDef]] = collect_functions(
        "../../../../great_expectations"
    )
    usage_stats_decorated_function_decorators = []
    for filename, functions in all_funcs.items():
        for f in functions:
            if f.decorator_list:
                for deco in f.decorator_list:
                    if isinstance(deco, ast.Call):
                        if isinstance(deco.func, ast.Name):
                            if deco.func.id == "usage_statistics_enabled_method":
                                usage_stats_decorated_function_decorators.append(deco)

    all_event_names: List[str] = []
    for decorated_function in usage_stats_decorated_function_decorators:
        event_names: List[str] = [
            dfk.value.value
            for dfk in decorated_function.keywords
            if dfk.arg == "event_name"
        ]
        all_event_names.extend(event_names)

    return sorted(list(set(all_event_names)))


def test_all_events_are_in_schema():

    # events_from_decorated_methods: List[str] = get_all_events_from_decorated_methods()
    # events_from_send_usage_stats: List[
    #     str
    # ] = get_all_events_from_send_usage_stats_invocations()
    #
    events_in_schema: List[
        str
    ] = get_all_events_from_anonymized_usage_statistics_record_schema()

    # assert set(events_in_schema) == set(
    #     events_from_decorated_methods + events_from_send_usage_stats
    # )

    assert set(events_in_schema) == set(UsageStatsEvents.get_all_event_names())
