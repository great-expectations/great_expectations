from urllib.parse import ParseResult, urlparse

import pytest
from ruamel.yaml import YAML

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context import DataContext
from great_expectations.data_context.types.base import (
    DATA_CONTEXT_ID,
    DataContextConfig,
)
from tests.test_utils import not_raises

yaml = YAML()
yaml.default_flow_style = False


@pytest.fixture()
def blank_in_memory_data_context() -> DataContext:
    context: DataContext = DataContext.create_blank_data_context(
        allow_anonymous_usage_statistics=True, runtime_environment=None
    )
    assert isinstance(context, DataContext)
    return context


def test_blank_context_structure_and_values(blank_in_memory_data_context):
    context: DataContext = blank_in_memory_data_context

    assert not set(context.list_datasources())

    assert not set(context.list_stores())

    with pytest.raises(
        ge_exceptions.InvalidConfigError, match=r"Unable to find configured store: None"
    ):
        # noinspection PyUnusedLocal
        expectation_suite_names: list = context.list_expectation_suite_names()

    assert not set(context.list_validation_operator_names())

    assert not set(context.get_docs_sites_urls())
    # assert(blank_in_memory_data_context.stores.keys() == {"anonymous_usage_statistics"})

    project_yaml: str = context.get_config().to_yaml_str()
    context_config_dict_from_yaml: dict = yaml.load(project_yaml)

    assert len(context_config_dict_from_yaml.keys()) == 2

    data_context_config: DataContextConfig
    with not_raises(ge_exceptions.InvalidDataContextConfigError):
        # noinspection PyUnusedLocal
        data_context_config = DataContextConfig.from_commented_map(
            context_config_dict_from_yaml
        )

    assert data_context_config.config_version == 2

    with not_raises(KeyError):
        # noinspection PyUnusedLocal
        value: str = context_config_dict_from_yaml["anonymous_usage_statistics"]

    with not_raises(KeyError):
        # noinspection PyUnusedLocal
        value: str = context_config_dict_from_yaml["anonymous_usage_statistics"][
            "data_context_id"
        ]

    assert (
        data_context_config.anonymous_usage_statistics.data_context_id
        == DATA_CONTEXT_ID
    )

    with not_raises(KeyError):
        # noinspection PyUnusedLocal
        value: bool = context_config_dict_from_yaml["anonymous_usage_statistics"][
            "enabled"
        ]

    with not_raises(KeyError):
        # noinspection PyUnusedLocal
        value: str = context_config_dict_from_yaml["anonymous_usage_statistics"][
            "usage_statistics_url"
        ]

    with not_raises(IOError):
        usage_statistics_url: str = context_config_dict_from_yaml[
            "anonymous_usage_statistics"
        ]["usage_statistics_url"]
        parse_result: ParseResult = urlparse(usage_statistics_url)
        if not (parse_result.scheme and parse_result.netloc):
            raise IOError(
                f'The stricture of "{usage_statistics_url}" is not recognized as a valid URL format.'
            )
