from urllib.parse import ParseResult, urlparse

import pytest
from ruamel.yaml import YAML

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context import DataContext
from great_expectations.data_context.config_utils import create_minimal_data_context
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.util import filter_properties_dict
from tests.test_utils import not_raises

yaml = YAML()
yaml.default_flow_style = False


@pytest.fixture()
def minimal_in_memory_data_context() -> DataContext:
    context: DataContext = create_minimal_data_context(
        runtime_environment=None, usage_statistics_enabled=True,
    )
    assert isinstance(context, DataContext)
    return context


def test_minimal_context_structure_and_values(minimal_in_memory_data_context):
    context: DataContext = minimal_in_memory_data_context

    assert not set(context.list_datasources())

    assert not set(context.list_stores())

    with pytest.raises(
        ge_exceptions.InvalidConfigError, match=r"Unable to find configured store: None"
    ):
        # noinspection PyUnusedLocal
        expectation_suite_names: list = context.list_expectation_suite_names()

    assert not set(context.list_validation_operator_names())

    assert not set(context.get_docs_sites_urls())

    project_yaml_str: str = context.get_project_config().to_yaml_str()
    project_config_dict_from_yaml: dict = yaml.load(project_yaml_str)

    assert len(project_config_dict_from_yaml.keys()) == 12

    assert (
        len(filter_properties_dict(properties=project_config_dict_from_yaml).keys())
        == 2
    )

    assert set(
        filter_properties_dict(properties=project_config_dict_from_yaml).keys()
    ) == {"config_version", "anonymous_usage_statistics"}

    data_context_config: DataContextConfig
    with not_raises(ge_exceptions.InvalidDataContextConfigError):
        # noinspection PyUnusedLocal
        # noinspection PyTypeChecker
        data_context_config = DataContextConfig.from_commented_map(
            commented_map=project_config_dict_from_yaml
        )

    assert data_context_config.config_version == 2

    with not_raises(KeyError):
        # noinspection PyUnusedLocal
        value: str = project_config_dict_from_yaml["anonymous_usage_statistics"]

    with not_raises(KeyError):
        # noinspection PyUnusedLocal
        value: str = project_config_dict_from_yaml["anonymous_usage_statistics"][
            "data_context_id"
        ]

    with not_raises(KeyError):
        # noinspection PyUnusedLocal
        value: bool = project_config_dict_from_yaml["anonymous_usage_statistics"][
            "enabled"
        ]

    with not_raises(KeyError):
        # noinspection PyUnusedLocal
        value: str = project_config_dict_from_yaml["anonymous_usage_statistics"][
            "usage_statistics_url"
        ]

    with not_raises(IOError):
        usage_statistics_url: str = project_config_dict_from_yaml[
            "anonymous_usage_statistics"
        ]["usage_statistics_url"]
        parse_result: ParseResult = urlparse(usage_statistics_url)
        if not (parse_result.scheme and parse_result.netloc):
            raise IOError(
                f'The stricture of "{usage_statistics_url}" is not recognized as a valid URL format.'
            )
