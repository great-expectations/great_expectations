import logging
import re
from typing import Any, Dict, List, Optional, Union, cast

from great_expectations.util import get_sqlalchemy_inspector

try:
    from typing import Final
except ImportError:
    from typing_extensions import Final

import click

try:
    from pybigquery.parse_url import parse_url as parse_bigquery_url
except (ImportError, ModuleNotFoundError):
    parse_bigquery_url = None
from great_expectations import exceptions as ge_exceptions
from great_expectations.datasource import (
    BaseDatasource,
    Datasource,
    SimpleSqlalchemyDatasource,
)
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.util import filter_properties_dict

logger = logging.getLogger(__name__)
try:
    import sqlalchemy
    from sqlalchemy.engine.reflection import Inspector
except ImportError:
    logger.debug(
        "Unable to load SqlAlchemy context; install optional sqlalchemy dependency for support"
    )
    sqlalchemy = None
    Inspector = None
DEFAULT_DATA_CONNECTOR_NAMES: Final[List[str]] = [
    "default_runtime_data_connector_name",
    "default_inferred_data_connector_name",
]


def get_batch_request(
    datasource: BaseDatasource,
    additional_batch_request_args: Optional[Dict[(str, Any)]] = None,
) -> Dict[(str, Union[(str, Dict[(str, Any)])])]:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "\n    This method manages the interaction with user necessary to obtain batch_request for a batch of a data asset.\n\n    In order to get batch_request this method needs datasource_name, data_connector_name and data_asset_name\n    to combine them into a batch_request dictionary.\n\n    All three arguments are optional. If they are present, the method uses their values. Otherwise, the method\n    prompts user to enter them interactively. Since it is possible for any of these three components to be\n    passed to this method as empty values and to get their values after interacting with user, this method\n    returns these components' values in case they changed.\n\n    If the datasource has data connectors, the method lets user choose a name from that list (note: if there are\n    multiple data connectors, user has to choose one first).\n\n    # :param datasource:\n    # :param additional_batch_request_args:\n    # :return: batch_request\n    "
    available_data_asset_names_by_data_connector_dict: Dict[
        (str, List[str])
    ] = datasource.get_available_data_asset_names()
    data_connector_name: Optional[str] = select_data_connector_name(
        available_data_asset_names_by_data_connector_dict=available_data_asset_names_by_data_connector_dict
    )
    batch_request: Dict[(str, Union[(str, int, Dict[(str, Any)])])] = {
        "datasource_name": datasource.name,
        "data_connector_name": data_connector_name,
    }
    data_asset_name: Optional[str]
    if isinstance(datasource, Datasource):
        msg_prompt_enter_data_asset_name: str = f"""
Which data asset (accessible by data connector "{data_connector_name}") would you like to use?
"""
        data_asset_name = _get_data_asset_name_from_data_connector(
            datasource=datasource,
            data_connector_name=data_connector_name,
            msg_prompt_enter_data_asset_name=msg_prompt_enter_data_asset_name,
        )
    elif isinstance(datasource, SimpleSqlalchemyDatasource):
        msg_prompt_enter_data_asset_name: str = (
            "\nWhich table would you like to use? (Choose one)\n"
        )
        data_asset_name = _get_data_asset_name_for_simple_sqlalchemy_datasource(
            datasource=datasource,
            data_connector_name=data_connector_name,
            msg_prompt_enter_data_asset_name=msg_prompt_enter_data_asset_name,
        )
    else:
        raise ge_exceptions.DataContextError(
            f"Datasource '{datasource.name}' of unsupported type {type(datasource)} was encountered."
        )
    batch_request.update({"data_asset_name": data_asset_name})
    if additional_batch_request_args and isinstance(
        additional_batch_request_args, dict
    ):
        batch_request.update(additional_batch_request_args)
    batch_spec_passthrough: Dict[
        (str, Union[(str, Dict[(str, Any)])])
    ] = batch_request.get("batch_spec_passthrough")
    if batch_spec_passthrough is None:
        batch_spec_passthrough = {}
    batch_spec_passthrough.update(_get_batch_spec_passthrough(datasource=datasource))
    batch_request["batch_spec_passthrough"] = batch_spec_passthrough
    filter_properties_dict(properties=batch_request, clean_falsy=True, inplace=True)
    return batch_request


def select_data_connector_name(
    available_data_asset_names_by_data_connector_dict: Optional[
        Dict[(str, List[str])]
    ] = None
) -> Optional[str]:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    msg_prompt_select_data_connector_name = "Select data_connector"
    if not available_data_asset_names_by_data_connector_dict:
        available_data_asset_names_by_data_connector_dict = {}
    num_available_data_asset_names_by_data_connector = len(
        available_data_asset_names_by_data_connector_dict
    )
    if num_available_data_asset_names_by_data_connector == 0:
        return None
    if num_available_data_asset_names_by_data_connector == 1:
        return list(available_data_asset_names_by_data_connector_dict.keys())[0]
    elif num_available_data_asset_names_by_data_connector == 2:
        default_data_connector = _check_default_data_connectors(
            available_data_asset_names_by_data_connector_dict
        )
        if default_data_connector:
            return default_data_connector
    data_connector_names: List[str] = list(
        available_data_asset_names_by_data_connector_dict.keys()
    )
    choices: str = "\n".join(
        [
            f"    {i}. {data_connector_name}"
            for (i, data_connector_name) in enumerate(data_connector_names, 1)
        ]
    )
    choices += "\n"
    option_selection: str = click.prompt(
        f"""{msg_prompt_select_data_connector_name}
{choices}""",
        type=click.Choice(
            [str(i) for (i, data_connector_name) in enumerate(data_connector_names, 1)]
        ),
        show_choices=False,
    )
    data_connector_name: str = data_connector_names[(int(option_selection) - 1)]
    return data_connector_name


def _get_data_asset_name_from_data_connector(
    datasource: BaseDatasource,
    data_connector_name: str,
    msg_prompt_enter_data_asset_name: str,
) -> Optional[str]:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Prompts the user to provide a data asset name from a list generated by a data connector\n\n    Args:\n        datasource: The datasource that contains our target data connector\n        data_connector_name: Used to retrieve the target data connector; this connector is then\n                             used to list available data assets\n        msg_prompt_enter_data_asset_name: CLI prompt to request user input\n\n    Returns:\n        The name of the data asset (if provided)\n\n    "
    available_data_asset_names_by_data_connector_dict: Dict[
        (str, List[str])
    ] = datasource.get_available_data_asset_names(
        data_connector_names=data_connector_name
    )
    available_data_asset_names: List[str] = sorted(
        available_data_asset_names_by_data_connector_dict[data_connector_name],
        key=(lambda x: x),
    )
    data_asset_name: Optional[str] = None
    num_data_assets = len(available_data_asset_names)
    if num_data_assets == 0:
        return None
    if num_data_assets > 100:
        prompt = f"""You have a list of {num_data_assets:,} data assets. Would you like to list them [l] or search [s]?
"""
        user_selected_option: Optional[str] = None
        while user_selected_option is None:
            user_selected_option = _get_user_response(prompt)
            if user_selected_option.startswith("l"):
                data_asset_name = _list_available_data_asset_names(
                    available_data_asset_names, msg_prompt_enter_data_asset_name
                )
            elif user_selected_option.startswith("s"):
                data_asset_name = _search_through_available_data_asset_names(
                    available_data_asset_names, msg_prompt_enter_data_asset_name
                )
            else:
                user_selected_option = None
    else:
        data_asset_name = _list_available_data_asset_names(
            available_data_asset_names, msg_prompt_enter_data_asset_name
        )
    return data_asset_name


def _list_available_data_asset_names(
    available_data_asset_names: List[str], msg_prompt_enter_data_asset_name: str
) -> Optional[str]:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    available_data_asset_names_str: List[str] = [
        f"{name}" for name in available_data_asset_names
    ]
    PAGE_SIZE = 50
    data_asset_pages: List[List[str]] = [
        available_data_asset_names_str[i : (i + PAGE_SIZE)]
        for i in range(0, len(available_data_asset_names_str), PAGE_SIZE)
    ]
    if len(data_asset_pages) == 0:
        return None
    display_idx = 0
    data_asset_name: Optional[str] = None
    while data_asset_name is None:
        current_page = data_asset_pages[display_idx]
        choices: str = "\n".join(
            [
                f"    {(i + (display_idx * PAGE_SIZE))}. {name}"
                for (i, name) in enumerate(current_page, 1)
            ]
        )
        instructions = "Type [n] to see the next page or [p] for the previous. When you're ready to select an asset, enter the index."
        prompt = f"""{msg_prompt_enter_data_asset_name}{choices}

{instructions}
"""
        user_response: str = _get_user_response(prompt)
        if user_response.startswith("n"):
            display_idx += 1
        elif user_response.startswith("p"):
            display_idx -= 1
        elif user_response.isdigit():
            data_asset_index: int = int(user_response) - 1
            try:
                data_asset_name = available_data_asset_names[data_asset_index]
            except IndexError:
                break
            except ValueError:
                data_asset_name = user_response
        if display_idx >= len(data_asset_pages):
            display_idx = 0
        elif display_idx < 0:
            display_idx = len(data_asset_pages) - 1
    return data_asset_name


def _search_through_available_data_asset_names(
    available_data_asset_names: List[str], msg_prompt_enter_data_asset_name: str
) -> Optional[str]:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    data_asset_name: Optional[str] = None
    while data_asset_name is None:
        available_data_asset_names_str: List[str] = [
            f"{name}" for name in available_data_asset_names
        ]
        choices: str = "\n".join(
            [
                f"    {i}. {name}"
                for (i, name) in enumerate(available_data_asset_names_str, 1)
            ]
        )
        instructions = "Search by name or regex to filter results. When you're ready to select an asset, enter the index."
        prompt = f"""{msg_prompt_enter_data_asset_name}{choices}

{instructions}
"""
        user_response = _get_user_response(prompt)
        if user_response.isdigit():
            data_asset_index: int = int(user_response) - 1
            try:
                data_asset_name = available_data_asset_names[data_asset_index]
            except IndexError:
                break
            except ValueError:
                data_asset_name = user_response
        else:
            r = re.compile(user_response)
            available_data_asset_names = list(
                filter(r.match, available_data_asset_names)
            )
    return data_asset_name


def _get_data_asset_name_for_simple_sqlalchemy_datasource(
    datasource: SimpleSqlalchemyDatasource,
    data_connector_name: str,
    msg_prompt_enter_data_asset_name: str,
) -> str:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    msg_prompt_how_to_connect_to_data: str = "\nYou have selected a datasource that is a SQL database. How would you like to specify the data?\n1. Enter a table name and schema\n2. List all tables in the database (this may take a very long time)\n"
    data_asset_name: Optional[str] = None
    default_schema: str = _get_default_schema(datasource=datasource)
    if default_schema is None:
        default_schema = ""
    schema_name: str
    table_name: str
    while data_asset_name is None:
        single_or_multiple_data_asset_selection: str = click.prompt(
            msg_prompt_how_to_connect_to_data,
            type=click.Choice(["1", "2", "3"]),
            show_choices=False,
        )
        if single_or_multiple_data_asset_selection == "1":
            schema_name = click.prompt(
                "Please provide the schema name of the table (this is optional)",
                default=default_schema,
            )
            table_name = click.prompt(
                "Please provide the table name (this is required)"
            )
            if schema_name:
                data_asset_name = f"{schema_name}.{table_name}"
            else:
                data_asset_name = table_name
        elif single_or_multiple_data_asset_selection == "2":
            msg_prompt_warning: str = "Warning: If you have a large number of tables in your datasource, this may take a very long time.\nWould you like to continue?"
            confirmation: str = click.prompt(
                msg_prompt_warning, type=click.Choice(["y", "n"]), show_choices=True
            )
            if confirmation == "y":
                data_asset_name = _get_data_asset_name_from_data_connector(
                    datasource=datasource,
                    data_connector_name=data_connector_name,
                    msg_prompt_enter_data_asset_name=msg_prompt_enter_data_asset_name,
                )
    if (datasource.execution_engine.engine.dialect.name.lower() == "bigquery") and (
        parse_bigquery_url is not None
    ):
        if len(data_asset_name.split(".")) < 3:
            (project_id, _, _, _, _, _) = parse_bigquery_url(datasource.engine.url)
            data_asset_name = f"{project_id}.{data_asset_name}"
    return data_asset_name


def _get_default_schema(datasource: SimpleSqlalchemyDatasource) -> str:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    execution_engine: SqlAlchemyExecutionEngine = cast(
        SqlAlchemyExecutionEngine, datasource.execution_engine
    )
    inspector: Inspector = get_sqlalchemy_inspector(execution_engine.engine)
    return inspector.default_schema_name


def _check_default_data_connectors(
    available_data_asset_names_by_data_connector_dict: Dict[(str, List[str])]
) -> Optional[str]:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    if all(
        (data_connector_name in available_data_asset_names_by_data_connector_dict)
        for data_connector_name in DEFAULT_DATA_CONNECTOR_NAMES
    ):
        return DEFAULT_DATA_CONNECTOR_NAMES[1]


def _get_batch_spec_passthrough(
    datasource: BaseDatasource,
) -> Dict[(str, Union[(str, Dict[(str, Any)])])]:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    batch_spec_passthrough: Dict[(str, Union[(str, Dict[(str, Any)])])] = {}
    if isinstance(datasource, Datasource):
        pass
    elif isinstance(datasource, SimpleSqlalchemyDatasource):
        execution_engine: SqlAlchemyExecutionEngine = cast(
            SqlAlchemyExecutionEngine, datasource.execution_engine
        )
    else:
        raise ge_exceptions.DataContextError(
            "Datasource {:s} of unsupported type {:s} was encountered.".format(
                datasource.name, str(type(datasource))
            )
        )
    return batch_spec_passthrough


def _get_user_response(prompt: str) -> str:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    return click.prompt(prompt, show_default=False).strip().lower()
