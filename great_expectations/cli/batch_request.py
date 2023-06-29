from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any, Dict, Final, List, Optional, Type, Union

import click

from great_expectations import exceptions as gx_exceptions
from great_expectations.cli.pretty_printing import cli_message
from great_expectations.compatibility.bigquery import parse_url as parse_bigquery_url
from great_expectations.datasource import (
    BaseDatasource,
    DataConnector,
    Datasource,
    SimpleSqlalchemyDatasource,
)
from great_expectations.datasource.data_connector import ConfiguredAssetSqlDataConnector
from great_expectations.execution_engine import (
    SqlAlchemyExecutionEngine,  # noqa: TCH001
)
from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect
from great_expectations.util import filter_properties_dict, get_sqlalchemy_inspector

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from great_expectations.compatibility import sqlalchemy


DEFAULT_DATA_CONNECTOR_NAMES: Final[List[str]] = [
    "default_runtime_data_connector_name",
    "default_inferred_data_connector_name",
]


def get_batch_request(
    datasource: BaseDatasource,
    additional_batch_request_args: Optional[Dict[str, Any]] = None,
) -> Dict[str, Union[str, int, Dict[str, Any]]]:
    """
    This method manages the interaction with user necessary to obtain batch_request for a batch of a data asset.

    In order to get batch_request this method needs datasource_name, data_connector_name and data_asset_name
    to combine them into a batch_request dictionary.

    All three arguments are optional. If they are present, the method uses their values. Otherwise, the method
    prompts user to enter them interactively. Since it is possible for any of these three components to be
    passed to this method as empty values and to get their values after interacting with user, this method
    returns these components' values in case they changed.

    If the datasource has data connectors, the method lets user choose a name from that list (note: if there are
    multiple data connectors, user has to choose one first).

    # :param datasource:
    # :param additional_batch_request_args:
    # :return: batch_request
    """
    available_data_asset_names_by_data_connector_dict: Dict[
        str, List[str]
    ] = datasource.get_available_data_asset_names()
    data_connector_name: Optional[str] = select_data_connector_name(
        available_data_asset_names_by_data_connector_dict=available_data_asset_names_by_data_connector_dict,
    )

    batch_request: Dict[str, Union[str, int, Dict[str, Any]]] = {
        "datasource_name": datasource.name,
        "data_connector_name": data_connector_name,  # type: ignore[dict-item] # name could be None
    }

    data_asset_name: Optional[str]

    if isinstance(datasource, Datasource):
        msg_prompt_enter_data_asset_name: str = f'\nWhich data asset (accessible by data connector "{data_connector_name}") would you like to use?\n'
        data_asset_name = _get_data_asset_name_from_data_connector(
            datasource=datasource,
            data_connector_name=data_connector_name,  # type: ignore[arg-type] # could be none
            msg_prompt_enter_data_asset_name=msg_prompt_enter_data_asset_name,
        )
    elif isinstance(datasource, SimpleSqlalchemyDatasource):
        msg_prompt_enter_data_asset_name = (
            "\nWhich table would you like to use? (Choose one)\n"
        )
        data_asset_name = _get_data_asset_name_for_simple_sqlalchemy_datasource(
            datasource=datasource,
            data_connector_name=data_connector_name,  # type: ignore[arg-type] # could be none
            msg_prompt_enter_data_asset_name=msg_prompt_enter_data_asset_name,
        )
        _print_configured_asset_sql_data_connector_message(
            datasource=datasource,
            data_connector_name=data_connector_name,  # type: ignore[arg-type] # could be none
        )
    else:
        raise gx_exceptions.DataContextError(
            f"Datasource '{datasource.name}' of unsupported type {type(datasource)} was encountered."
        )

    batch_request.update(
        {
            "data_asset_name": data_asset_name,  # type: ignore[dict-item] # could be none
        }
    )

    if additional_batch_request_args and isinstance(
        additional_batch_request_args, dict
    ):
        batch_request.update(additional_batch_request_args)

    batch_spec_passthrough: Optional[
        Dict[str, Union[str, Dict[str, Any]]]
    ] = batch_request.get(  # type: ignore[assignment] # can't guarantee shape of 'batch_spec_passthrough'
        "batch_spec_passthrough"
    )
    if batch_spec_passthrough is None:
        batch_spec_passthrough = {}

    batch_spec_passthrough.update(_get_batch_spec_passthrough(datasource=datasource))
    batch_request["batch_spec_passthrough"] = batch_spec_passthrough

    filter_properties_dict(properties=batch_request, clean_falsy=True, inplace=True)

    return batch_request


def _print_configured_asset_sql_data_connector_message(
    datasource: BaseDatasource,
    data_connector_name: str,
    data_connector_type: Type = ConfiguredAssetSqlDataConnector,
) -> None:
    """Print a message if the data connector matches data connector type.

    Args:
        datasource: Datasource associated with data connector of interest.
        data_connector_name: Name of the data connector of interest.
        data_connector_type: Type of data connector to check against.
    """
    if _is_data_connector_of_type(datasource, data_connector_name, data_connector_type):
        configured_asset_data_connector_message = f"Need to configure a new Data Asset? See how to add a new DataAsset to your {data_connector_type.__name__} here: https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_sql_datasource/"

        cli_message(configured_asset_data_connector_message)


def _is_data_connector_of_type(
    datasource: BaseDatasource,
    data_connector_name: str,
    data_connector_type: Type = ConfiguredAssetSqlDataConnector,
) -> bool:
    """Determine whether a data connector is a ConfiguredAssetSqlDataConnector.

    Args:
        datasource: Datasource associated with data connector of interest.
        data_connector_name: Name of the data connector of interest.
        data_connector_type: Type of data connector to check against.
    """
    data_connector: DataConnector | None = datasource.data_connectors.get(
        data_connector_name
    )
    return isinstance(data_connector, data_connector_type)


def select_data_connector_name(
    available_data_asset_names_by_data_connector_dict: Optional[
        Dict[str, List[str]]
    ] = None,
) -> Optional[str]:
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

    elif num_available_data_asset_names_by_data_connector == 2:  # noqa: PLR2004
        # if only default data_connectors are configured, select default_inferred_asset_data_connector
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
            for i, data_connector_name in enumerate(data_connector_names, 1)
        ]
    )
    choices += "\n"  # Necessary for consistent spacing between prompts
    option_selection: str = click.prompt(
        f"{msg_prompt_select_data_connector_name}\n{choices}",
        type=click.Choice(
            [str(i) for i, data_connector_name in enumerate(data_connector_names, 1)]
        ),
        show_choices=False,
    )
    data_connector_name: str = data_connector_names[int(option_selection) - 1]

    return data_connector_name


def _get_data_asset_name_from_data_connector(
    datasource: BaseDatasource,
    data_connector_name: str,
    msg_prompt_enter_data_asset_name: str,
) -> Optional[str]:
    """Prompts the user to provide a data asset name from a list generated by a data connector

    Args:
        datasource: The datasource that contains our target data connector
        data_connector_name: Used to retrieve the target data connector; this connector is then
                             used to list available data assets
        msg_prompt_enter_data_asset_name: CLI prompt to request user input

    Returns:
        The name of the data asset (if provided)

    """

    available_data_asset_names_by_data_connector_dict: Dict[
        str, List[str]
    ] = datasource.get_available_data_asset_names(
        data_connector_names=data_connector_name
    )
    available_data_asset_names: List[str] = sorted(
        available_data_asset_names_by_data_connector_dict[data_connector_name],
        key=lambda x: x,
    )

    data_asset_name: Optional[str] = None
    num_data_assets = len(available_data_asset_names)

    if num_data_assets == 0:
        return None

    # If we have a large number of assets, give the user the ability to paginate or search
    if num_data_assets > 100:  # noqa: PLR2004
        prompt = f"You have a list of {num_data_assets:,} data assets. Would you like to list them [l] or search [s]?\n"
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
    # Otherwise, default to pagination
    else:
        data_asset_name = _list_available_data_asset_names(
            available_data_asset_names, msg_prompt_enter_data_asset_name
        )

    return data_asset_name


def _list_available_data_asset_names(
    available_data_asset_names: List[str],
    msg_prompt_enter_data_asset_name: str,
) -> Optional[str]:
    available_data_asset_names_str: List[str] = [
        f"{name}" for name in available_data_asset_names
    ]
    PAGE_SIZE = 50

    # Organize available data assets into pages of 50
    data_asset_pages: List[List[str]] = [
        available_data_asset_names_str[i : i + PAGE_SIZE]
        for i in range(0, len(available_data_asset_names_str), PAGE_SIZE)
    ]

    if len(data_asset_pages) == 0:
        return None

    display_idx = 0  # Used to traverse between pages
    data_asset_name: Optional[str] = None

    while data_asset_name is None:
        current_page = data_asset_pages[display_idx]
        choices: str = "\n".join(
            [
                f"    {i+(display_idx*PAGE_SIZE)}. {name}"
                for i, name in enumerate(current_page, 1)
            ]
        )

        instructions = "Type [n] to see the next page or [p] for the previous. When you're ready to select an asset, enter the index."
        prompt = f"{msg_prompt_enter_data_asset_name}{choices}\n\n{instructions}\n"
        user_response: str = _get_user_response(prompt)

        # Pagination options
        if user_response.startswith("n"):
            display_idx += 1
        elif user_response.startswith("p"):
            display_idx -= 1
        # Selected asset
        elif user_response.isdigit():
            data_asset_index: int = int(user_response) - 1
            try:
                data_asset_name = available_data_asset_names[data_asset_index]
            except IndexError:
                break
            except ValueError:
                data_asset_name = user_response

        # Ensure that our display index is never out of bounds (loops around to the other side)
        if display_idx >= len(data_asset_pages):
            display_idx = 0
        elif display_idx < 0:
            display_idx = len(data_asset_pages) - 1

    return data_asset_name


def _search_through_available_data_asset_names(
    available_data_asset_names: List[str],
    msg_prompt_enter_data_asset_name: str,
) -> Optional[str]:
    data_asset_name: Optional[str] = None
    while data_asset_name is None:
        available_data_asset_names_str: List[str] = [
            f"{name}" for name in available_data_asset_names
        ]
        choices: str = "\n".join(
            [
                f"    {i}. {name}"
                for i, name in enumerate(available_data_asset_names_str, 1)
            ]
        )

        instructions = "Search by name or regex to filter results. When you're ready to select an asset, enter the index."
        prompt = f"{msg_prompt_enter_data_asset_name}{choices}\n\n{instructions}\n"
        user_response = _get_user_response(prompt)

        # Selected asset
        if user_response.isdigit():
            data_asset_index: int = int(user_response) - 1
            try:
                data_asset_name = available_data_asset_names[data_asset_index]
            except IndexError:
                break
            except ValueError:
                data_asset_name = user_response
        # Used search
        else:
            # Narrow the search results down to ones that are close to the user query
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
    msg_prompt_how_to_connect_to_data: str = """
You have selected a datasource that is a SQL database. How would you like to specify the data?
1. Enter a table name and schema
2. List all tables in the database (this may take a very long time)
"""
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
        if single_or_multiple_data_asset_selection == "1":  # name the table and schema
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
        elif single_or_multiple_data_asset_selection == "2":  # list it all
            msg_prompt_warning: str = r"""Warning: If you have a large number of tables in your datasource, this may take a very long time.
Would you like to continue?"""
            confirmation: str = click.prompt(
                msg_prompt_warning, type=click.Choice(["y", "n"]), show_choices=True
            )
            if confirmation == "y":
                data_asset_name = _get_data_asset_name_from_data_connector(
                    datasource=datasource,
                    data_connector_name=data_connector_name,
                    msg_prompt_enter_data_asset_name=msg_prompt_enter_data_asset_name,
                )

    if (
        datasource.execution_engine.engine.dialect.name.lower() == GXSqlDialect.BIGQUERY
        and parse_bigquery_url is not None
    ):
        # bigquery table needs to contain the project id if it differs from the credentials project
        if len(data_asset_name.split(".")) < 3:  # noqa: PLR2004
            project_id, _, _, _, _, _ = parse_bigquery_url(datasource.engine.url)  # type: ignore[attr-defined]
            data_asset_name = f"{project_id}.{data_asset_name}"

    return data_asset_name


def _get_default_schema(datasource: SimpleSqlalchemyDatasource) -> str:
    execution_engine: SqlAlchemyExecutionEngine = datasource.execution_engine
    inspector: sqlalchemy.Inspector = get_sqlalchemy_inspector(execution_engine.engine)
    return inspector.default_schema_name


def _check_default_data_connectors(
    available_data_asset_names_by_data_connector_dict: Dict[str, List[str]]
) -> Optional[str]:
    if all(
        data_connector_name in available_data_asset_names_by_data_connector_dict
        for data_connector_name in DEFAULT_DATA_CONNECTOR_NAMES
    ):
        # return the default_inferred_asset_data_connector
        return DEFAULT_DATA_CONNECTOR_NAMES[1]
    return None


def _get_batch_spec_passthrough(
    datasource: BaseDatasource,
) -> Dict[str, Union[str, Dict[str, Any]]]:
    batch_spec_passthrough: Dict[str, Union[str, Dict[str, Any]]] = {}

    if isinstance(datasource, Datasource):
        pass  # TODO: <Alex>Add parameters for Pandas, Spark, and other SQL as CLI functionality expands.</Alex>
    elif isinstance(datasource, SimpleSqlalchemyDatasource):
        # Some backends require named temporary table parameters.  We specifically elicit those and add them
        # where appropriate.
        _: SqlAlchemyExecutionEngine = datasource.execution_engine
    else:
        raise gx_exceptions.DataContextError(
            "Datasource {:s} of unsupported type {:s} was encountered.".format(
                datasource.name, str(type(datasource))
            )
        )

    return batch_spec_passthrough


def _get_user_response(prompt: str) -> str:
    return click.prompt(prompt, show_default=False).strip().lower()
