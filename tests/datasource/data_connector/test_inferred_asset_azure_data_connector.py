from unittest import mock

import pytest

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility import azure
from great_expectations.core import IDDict
from great_expectations.core.batch import (
    BatchRequest,
    BatchRequestBase,
    LegacyBatchDefinition,
)
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.data_connector import InferredAssetAzureDataConnector
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)

# module level markers
pytestmark = pytest.mark.big

yaml = YAMLHandler()


if not (azure.storage and azure.BlobServiceClient):
    pytest.skip(
        'Could not import "azure.storage.blob" from Microsoft Azure cloud',
        allow_module_level=True,
    )


@pytest.fixture
def expected_batch_definitions_unsorted():
    """
    Used to validate `get_batch_definition_list_from_batch_request()` outputs.
    Input and output should maintain the same order (henced "unsorted")
    """
    expected = [
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_azure_data_connector",
            data_asset_name="DEFAULT_ASSET_NAME",
            batch_identifiers=IDDict({"name": "alex", "timestamp": "20200809", "price": "1000"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_azure_data_connector",
            data_asset_name="DEFAULT_ASSET_NAME",
            batch_identifiers=IDDict({"name": "eugene", "timestamp": "20200809", "price": "1500"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_azure_data_connector",
            data_asset_name="DEFAULT_ASSET_NAME",
            batch_identifiers=IDDict({"name": "james", "timestamp": "20200811", "price": "1009"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_azure_data_connector",
            data_asset_name="DEFAULT_ASSET_NAME",
            batch_identifiers=IDDict({"name": "abe", "timestamp": "20200809", "price": "1040"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_azure_data_connector",
            data_asset_name="DEFAULT_ASSET_NAME",
            batch_identifiers=IDDict({"name": "will", "timestamp": "20200809", "price": "1002"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_azure_data_connector",
            data_asset_name="DEFAULT_ASSET_NAME",
            batch_identifiers=IDDict({"name": "james", "timestamp": "20200713", "price": "1567"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_azure_data_connector",
            data_asset_name="DEFAULT_ASSET_NAME",
            batch_identifiers=IDDict({"name": "eugene", "timestamp": "20201129", "price": "1900"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_azure_data_connector",
            data_asset_name="DEFAULT_ASSET_NAME",
            batch_identifiers=IDDict({"name": "will", "timestamp": "20200810", "price": "1001"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_azure_data_connector",
            data_asset_name="DEFAULT_ASSET_NAME",
            batch_identifiers=IDDict({"name": "james", "timestamp": "20200810", "price": "1003"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_azure_data_connector",
            data_asset_name="DEFAULT_ASSET_NAME",
            batch_identifiers=IDDict({"name": "alex", "timestamp": "20200819", "price": "1300"}),
        ),
    ]
    return expected


@pytest.fixture
def expected_batch_definitions_sorted():
    """
    Used to validate `get_batch_definition_list_from_batch_request()` outputs.
    Input should be sorted based on some criteria, resulting in some change
    between input and output.
    """
    expected = [
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_azure_data_connector",
            data_asset_name="DEFAULT_ASSET_NAME",
            batch_identifiers=IDDict({"name": "abe", "timestamp": "20200809", "price": "1040"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_azure_data_connector",
            data_asset_name="DEFAULT_ASSET_NAME",
            batch_identifiers=IDDict({"name": "alex", "timestamp": "20200819", "price": "1300"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_azure_data_connector",
            data_asset_name="DEFAULT_ASSET_NAME",
            batch_identifiers=IDDict({"name": "alex", "timestamp": "20200809", "price": "1000"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_azure_data_connector",
            data_asset_name="DEFAULT_ASSET_NAME",
            batch_identifiers=IDDict({"name": "eugene", "timestamp": "20201129", "price": "1900"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_azure_data_connector",
            data_asset_name="DEFAULT_ASSET_NAME",
            batch_identifiers=IDDict({"name": "eugene", "timestamp": "20200809", "price": "1500"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_azure_data_connector",
            data_asset_name="DEFAULT_ASSET_NAME",
            batch_identifiers=IDDict({"name": "james", "timestamp": "20200811", "price": "1009"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_azure_data_connector",
            data_asset_name="DEFAULT_ASSET_NAME",
            batch_identifiers=IDDict({"name": "james", "timestamp": "20200810", "price": "1003"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_azure_data_connector",
            data_asset_name="DEFAULT_ASSET_NAME",
            batch_identifiers=IDDict({"name": "james", "timestamp": "20200713", "price": "1567"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_azure_data_connector",
            data_asset_name="DEFAULT_ASSET_NAME",
            batch_identifiers=IDDict({"name": "will", "timestamp": "20200810", "price": "1001"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_azure_data_connector",
            data_asset_name="DEFAULT_ASSET_NAME",
            batch_identifiers=IDDict({"name": "will", "timestamp": "20200809", "price": "1002"}),
        ),
    ]
    return expected


# noinspection PyUnusedLocal
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.list_azure_keys",
    return_value=["alpha-1.csv", "alpha-2.csv", "alpha-3.csv"],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.azure.BlobServiceClient"
)
def test_instantiation_with_account_url_and_credential(
    mock_azure_conn,
    mock_list_keys,
):
    my_data_connector = InferredAssetAzureDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=PandasExecutionEngine(),
        default_regex={
            "pattern": r"(alpha)-(.*)\.csv",
            "group_names": ["data_asset_name", "index"],
        },
        container="my_container",
        name_starts_with="",
        azure_options={
            "account_url": "my_account_url.blob.core.windows.net",
            "credential": "my_credential",
        },
    )

    my_data_connector._refresh_data_references_cache()
    assert my_data_connector.get_data_reference_count() == 3
    assert my_data_connector.get_unmatched_data_references() == []


# noinspection PyUnusedLocal
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.list_azure_keys",
    return_value=["alpha-1.csv", "alpha-2.csv", "alpha-3.csv"],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.azure.BlobServiceClient"
)
def test_instantiation_with_conn_str_and_credential(
    mock_azure_conn,
    mock_list_keys,
):
    my_data_connector = InferredAssetAzureDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=PandasExecutionEngine(),
        default_regex={
            "pattern": r"(alpha)-(.*)\.csv",
            "group_names": ["data_asset_name", "index"],
        },
        container="my_container",
        name_starts_with="",
        azure_options={  # Representative of format noted in official docs
            "conn_str": "DefaultEndpointsProtocol=https;AccountName=storagesample;AccountKey=my_account_key",  # noqa: E501
            "credential": "my_credential",
        },
    )

    my_data_connector._refresh_data_references_cache()
    assert my_data_connector.get_data_reference_count() == 3
    assert my_data_connector.get_unmatched_data_references() == []


# noinspection PyUnusedLocal
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.azure.BlobServiceClient"
)
def test_instantiation_with_valid_account_url_assigns_account_name(mock_azure_conn):
    my_data_connector = InferredAssetAzureDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=PandasExecutionEngine(),
        default_regex={
            "pattern": "alpha-(.*)\\.csv",
            "group_names": ["index"],
        },
        container="my_container",
        name_starts_with="",
        azure_options={
            "account_url": "my_account_url.blob.core.windows.net",
            "credential": "my_credential",
        },
    )
    assert my_data_connector._account_name == "my_account_url"


# noinspection PyUnusedLocal
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.azure.BlobServiceClient"
)
def test_instantiation_with_valid_conn_str_assigns_account_name(mock_azure_conn):
    my_data_connector = InferredAssetAzureDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=PandasExecutionEngine(),
        default_regex={
            "pattern": "alpha-(.*)\\.csv",
            "group_names": ["index"],
        },
        container="my_container",
        name_starts_with="",
        azure_options={  # Representative of format noted in official docs
            "conn_str": "DefaultEndpointsProtocol=https;AccountName=storagesample;AccountKey=my_account_key",  # noqa: E501
            "credential": "my_credential",
        },
    )
    assert my_data_connector._account_name == "storagesample"


# noinspection PyUnusedLocal
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.azure.BlobServiceClient"
)
def test_instantiation_with_multiple_auth_methods_raises_error(
    mock_azure_conn,
):
    # Raises error in DataContext's schema validation due to having both `account_url` and `conn_str`  # noqa: E501
    with pytest.raises(AssertionError):
        InferredAssetAzureDataConnector(
            name="my_data_connector",
            datasource_name="FAKE_DATASOURCE_NAME",
            execution_engine=PandasExecutionEngine(),
            default_regex={
                "pattern": "alpha-(.*)\\.csv",
                "group_names": ["index"],
            },
            container="my_container",
            name_starts_with="",
            azure_options={
                "account_url": "account.blob.core.windows.net",
                "conn_str": "DefaultEndpointsProtocol=https;AccountName=storagesample;AccountKey=my_account_key",  # noqa: E501
                "credential": "my_credential",
            },
        )


# noinspection PyUnusedLocal,GrazieInspection
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.azure.BlobServiceClient"
)
def test_instantiation_with_improperly_formatted_auth_keys_in_azure_options_raises_error(
    mock_azure_conn,
):
    # Raises error in InferredAssetAzureDataConnector's constructor due to `account_url` not conforming to the expected format  # noqa: E501
    # Format: <ACCOUNT>.blob.core.windows.net
    with pytest.raises(ImportError):
        InferredAssetAzureDataConnector(
            name="my_data_connector",
            datasource_name="FAKE_DATASOURCE_NAME",
            execution_engine=PandasExecutionEngine(),
            default_regex={
                "pattern": "alpha-(.*)\\.csv",
                "group_names": ["index"],
            },
            container="my_container",
            name_starts_with="",
            azure_options={"account_url": "not_a_valid_url"},
        )

    # Raises error in InferredAssetAzureDataConnector's constructor due to `conn_str` not conforming to the expected format  # noqa: E501
    # Format: Must be a variable length, semicolon-delimited string containing "AccountName=<ACCOUNT>"  # noqa: E501
    with pytest.raises(ImportError):
        InferredAssetAzureDataConnector(
            name="my_data_connector",
            datasource_name="FAKE_DATASOURCE_NAME",
            execution_engine=PandasExecutionEngine(),
            default_regex={
                "pattern": "alpha-(.*)\\.csv",
                "group_names": ["index"],
            },
            container="my_container",
            name_starts_with="",
            azure_options={"conn_str": "not_a_valid_conn_str"},
        )


@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.list_azure_keys",
    return_value=["alpha-1.csv", "alpha-2.csv", "alpha-3.csv"],
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.azure.BlobServiceClient"
)
def test_get_batch_definition_list_from_batch_request_with_nonexistent_datasource_name_raises_error(
    mock_azure_conn, mock_list_keys, empty_data_context_stats_enabled
):
    my_data_connector = InferredAssetAzureDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=PandasExecutionEngine(),
        default_regex={
            "pattern": "alpha-(.*)\\.csv",
            "group_names": ["index"],
        },
        container="my_container",
        name_starts_with="",
        azure_options={
            "account_url": "my_account_url.blob.core.windows.net",
            "credential": "my_credential",
        },
    )

    # Raises error in `DataConnector._validate_batch_request()` due to `datasource_name` in BatchRequest not matching DataConnector `datasource_name`  # noqa: E501
    with pytest.raises(ValueError):
        my_data_connector.get_batch_definition_list_from_batch_request(
            BatchRequest(
                datasource_name="something",
                data_connector_name="my_data_connector",
                data_asset_name="something",
            )
        )


@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.list_azure_keys"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.azure.BlobServiceClient"
)
def test_get_definition_list_from_batch_request_with_empty_args_raises_error(
    mock_azure_conn, mock_list_keys, empty_data_context_stats_enabled
):
    my_data_connector_yaml = yaml.load(
        """
           class_name: InferredAssetAzureDataConnector
           datasource_name: test_environment
           container: my_container
           name_starts_with: ""
           default_regex:
               pattern: (.+)_(.+)_(.+)\\.csv
               group_names:
                   - name
                   - timestamp
                   - price
           azure_options:
               account_url: my_account_url.blob.core.windows.net
               credential: my_credential
       """,
    )

    mock_list_keys.return_value = (
        [
            "alex_20200809_1000.csv",
            "eugene_20200809_1500.csv",
            "james_20200811_1009.csv",
            "abe_20200809_1040.csv",
            "will_20200809_1002.csv",
            "james_20200713_1567.csv",
            "eugene_20201129_1900.csv",
            "will_20200810_1001.csv",
            "james_20200810_1003.csv",
            "alex_20200819_1300.csv",
        ],
    )

    my_data_connector: InferredAssetAzureDataConnector = instantiate_class_from_config(
        config=my_data_connector_yaml,
        runtime_environment={
            "name": "general_azure_data_connector",
            "execution_engine": PandasExecutionEngine(),
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )

    # Raises error in `FilePathDataConnector.get_batch_definition_list_from_batch_request()` due to missing a `batch_request` arg  # noqa: E501
    with pytest.raises(TypeError):
        # noinspection PyArgumentList
        my_data_connector.get_batch_definition_list_from_batch_request()


@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.list_azure_keys"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.azure.BlobServiceClient"
)
def test_get_definition_list_from_batch_request_with_unnamed_data_asset_name_raises_error(
    mock_azure_conn, mock_list_keys, empty_data_context_stats_enabled
):
    my_data_connector_yaml = yaml.load(
        """
           class_name: InferredAssetAzureDataConnector
           datasource_name: test_environment
           container: my_container
           name_starts_with: ""
           default_regex:
               pattern: (.+)_(.+)_(.+)\\.csv
               group_names:
                   - name
                   - timestamp
                   - price
           azure_options:
               account_url: my_account_url.blob.core.windows.net
               credential: my_credential
       """,
    )

    my_data_connector: InferredAssetAzureDataConnector = instantiate_class_from_config(
        config=my_data_connector_yaml,
        runtime_environment={
            "name": "general_azure_data_connector",
            "execution_engine": PandasExecutionEngine(),
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )

    # Raises error in `Batch._validate_init_parameters()` due to `data_asset_name` being `NoneType` and not the required `str`  # noqa: E501
    with pytest.raises(TypeError):
        my_data_connector.get_batch_definition_list_from_batch_request(
            BatchRequest(
                datasource_name="test_environment",
                data_connector_name="general_azure_data_connector",
                data_asset_name="",
            )
        )


@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.list_azure_keys"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.azure.BlobServiceClient"
)
def test_return_all_batch_definitions_unsorted_without_named_data_asset_name(
    mock_azure_conn,
    mock_list_keys,
    empty_data_context_stats_enabled,
    expected_batch_definitions_unsorted,
):
    my_data_connector_yaml = yaml.load(
        """
           class_name: InferredAssetAzureDataConnector
           datasource_name: test_environment
           container: my_container
           name_starts_with: ""
           default_regex:
               pattern: (.+)_(.+)_(.+)\\.csv
               group_names:
                   - name
                   - timestamp
                   - price
           azure_options:
               account_url: my_account_url.blob.core.windows.net
               credential: my_credential
       """,
    )

    mock_list_keys.return_value = [
        "alex_20200809_1000.csv",
        "eugene_20200809_1500.csv",
        "james_20200811_1009.csv",
        "abe_20200809_1040.csv",
        "will_20200809_1002.csv",
        "james_20200713_1567.csv",
        "eugene_20201129_1900.csv",
        "will_20200810_1001.csv",
        "james_20200810_1003.csv",
        "alex_20200819_1300.csv",
    ]

    my_data_connector: InferredAssetAzureDataConnector = instantiate_class_from_config(
        config=my_data_connector_yaml,
        runtime_environment={
            "name": "general_azure_data_connector",
            "execution_engine": PandasExecutionEngine(),
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )

    # In an actual production environment, Azure Blob Storage will automatically sort these blobs by path (alphabetic order).  # noqa: E501
    # Source: https://docs.microsoft.com/en-us/rest/api/storageservices/List-Blobs?redirectedfrom=MSDN
    #
    # The expected behavior is that our `unsorted_batch_definition_list` will maintain the same order it parses through `list_azure_keys()` (hence "unsorted").  # noqa: E501
    # When using an actual `BlobServiceClient` (and not a mock), the output of `list_azure_keys` would be pre-sorted by nature of how the system orders blobs.  # noqa: E501
    # It is important to note that although this is a minor deviation, it is deemed to be immaterial as we still end up testing our desired behavior.  # noqa: E501

    unsorted_batch_definition_list = (
        my_data_connector._get_batch_definition_list_from_batch_request(
            BatchRequestBase(
                datasource_name="test_environment",
                data_connector_name="general_azure_data_connector",
                data_asset_name="",
            )
        )
    )
    assert unsorted_batch_definition_list == expected_batch_definitions_unsorted


@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.list_azure_keys"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.azure.BlobServiceClient"
)
def test_return_all_batch_definitions_unsorted_with_named_data_asset_name(
    mock_azure_conn,
    mock_list_keys,
    empty_data_context_stats_enabled,
    expected_batch_definitions_unsorted,
):
    my_data_connector_yaml = yaml.load(
        """
           class_name: InferredAssetAzureDataConnector
           datasource_name: test_environment
           container: my_container
           name_starts_with: ""
           default_regex:
               pattern: (.+)_(.+)_(.+)\\.csv
               group_names:
                   - name
                   - timestamp
                   - price
           azure_options:
               account_url: my_account_url.blob.core.windows.net
               credential: my_credential
       """,
    )

    mock_list_keys.return_value = [
        "alex_20200809_1000.csv",
        "eugene_20200809_1500.csv",
        "james_20200811_1009.csv",
        "abe_20200809_1040.csv",
        "will_20200809_1002.csv",
        "james_20200713_1567.csv",
        "eugene_20201129_1900.csv",
        "will_20200810_1001.csv",
        "james_20200810_1003.csv",
        "alex_20200819_1300.csv",
    ]

    my_data_connector: InferredAssetAzureDataConnector = instantiate_class_from_config(
        config=my_data_connector_yaml,
        runtime_environment={
            "name": "general_azure_data_connector",
            "execution_engine": PandasExecutionEngine(),
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )

    # In an actual production environment, Azure Blob Storage will automatically sort these blobs by path (alphabetic order).  # noqa: E501
    # Source: https://docs.microsoft.com/en-us/rest/api/storageservices/List-Blobs?redirectedfrom=MSDN
    #
    # The expected behavior is that our `unsorted_batch_definition_list` will maintain the same order it parses through `list_azure_keys()` (hence "unsorted").  # noqa: E501
    # When using an actual `BlobServiceClient` (and not a mock), the output of `list_azure_keys` would be pre-sorted by nature of how the system orders blobs.  # noqa: E501
    # It is important to note that although this is a minor deviation, it is deemed to be immaterial as we still end up testing our desired behavior.  # noqa: E501

    unsorted_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        BatchRequest(
            datasource_name="test_environment",
            data_connector_name="general_azure_data_connector",
            data_asset_name="DEFAULT_ASSET_NAME",
        )
    )
    assert unsorted_batch_definition_list == expected_batch_definitions_unsorted


@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.list_azure_keys"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.azure.BlobServiceClient"
)
def test_return_all_batch_definitions_basic_sorted(
    mock_azure_conn,
    mock_list_keys,
    empty_data_context_stats_enabled,
    expected_batch_definitions_sorted,
):
    my_data_connector_yaml = yaml.load(
        """
       class_name: InferredAssetAzureDataConnector
       datasource_name: test_environment
       container: my_container
       name_starts_with: ""
       default_regex:
           pattern: (.+)_(.+)_(.+)\\.csv
           group_names:
               - name
               - timestamp
               - price
       sorters:
           - orderby: asc
             class_name: LexicographicSorter
             name: name
           - datetime_format: "%Y%m%d"
             orderby: desc
             class_name: DateTimeSorter
             name: timestamp
           - orderby: desc
             class_name: NumericSorter
             name: price
       azure_options:
           account_url: my_account_url.blob.core.windows.net
           credential: my_credential
     """,
    )

    mock_list_keys.return_value = [
        "alex_20200809_1000.csv",
        "eugene_20200809_1500.csv",
        "james_20200811_1009.csv",
        "abe_20200809_1040.csv",
        "will_20200809_1002.csv",
        "james_20200713_1567.csv",
        "eugene_20201129_1900.csv",
        "will_20200810_1001.csv",
        "james_20200810_1003.csv",
        "alex_20200819_1300.csv",
    ]

    my_data_connector: InferredAssetAzureDataConnector = instantiate_class_from_config(
        config=my_data_connector_yaml,
        runtime_environment={
            "name": "general_azure_data_connector",
            "execution_engine": PandasExecutionEngine(),
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )

    sorted_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        BatchRequest(
            datasource_name="test_environment",
            data_connector_name="general_azure_data_connector",
            data_asset_name="DEFAULT_ASSET_NAME",
        )
    )
    assert sorted_batch_definition_list == expected_batch_definitions_sorted


@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.list_azure_keys"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.azure.BlobServiceClient"
)
def test_return_all_batch_definitions_returns_specified_partition(
    mock_azure_conn, mock_list_keys, empty_data_context_stats_enabled
):
    my_data_connector_yaml = yaml.load(
        """
       class_name: InferredAssetAzureDataConnector
       datasource_name: test_environment
       container: my_container
       name_starts_with: ""
       default_regex:
           pattern: (.+)_(.+)_(.+)\\.csv
           group_names:
               - name
               - timestamp
               - price
       sorters:
           - orderby: asc
             class_name: LexicographicSorter
             name: name
           - datetime_format: "%Y%m%d"
             orderby: desc
             class_name: DateTimeSorter
             name: timestamp
           - orderby: desc
             class_name: NumericSorter
             name: price
       azure_options:
           account_url: my_account_url.blob.core.windows.net
           credential: my_credential
     """,
    )

    mock_list_keys.return_value = [
        "alex_20200809_1000.csv",
        "eugene_20200809_1500.csv",
        "james_20200811_1009.csv",
        "abe_20200809_1040.csv",
        "will_20200809_1002.csv",
        "james_20200713_1567.csv",
        "eugene_20201129_1900.csv",
        "will_20200810_1001.csv",
        "james_20200810_1003.csv",
        "alex_20200819_1300.csv",
    ]

    my_data_connector: InferredAssetAzureDataConnector = instantiate_class_from_config(
        config=my_data_connector_yaml,
        runtime_environment={
            "name": "general_azure_data_connector",
            "execution_engine": PandasExecutionEngine(),
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )

    my_batch_request: BatchRequest = BatchRequest(
        datasource_name="test_environment",
        data_connector_name="general_azure_data_connector",
        data_asset_name="DEFAULT_ASSET_NAME",
        data_connector_query=IDDict(
            **{
                "batch_filter_parameters": {
                    "name": "james",
                    "timestamp": "20200713",
                    "price": "1567",
                }
            }
        ),
    )

    my_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=my_batch_request
    )

    assert len(my_batch_definition_list) == 1
    my_batch_definition = my_batch_definition_list[0]
    expected_batch_definition = LegacyBatchDefinition(
        datasource_name="test_environment",
        data_connector_name="general_azure_data_connector",
        data_asset_name="DEFAULT_ASSET_NAME",
        batch_identifiers=IDDict(
            **{
                "name": "james",
                "timestamp": "20200713",
                "price": "1567",
            }
        ),
    )
    assert my_batch_definition == expected_batch_definition


# noinspection PyUnusedLocal
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.azure.BlobServiceClient"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.list_azure_keys"
)
def test_return_all_batch_definitions_sorted_without_data_connector_query(
    mock_azure_conn,
    mock_list_keys,
    empty_data_context_stats_enabled,
    expected_batch_definitions_sorted,
):
    my_data_connector_yaml = yaml.load(
        """
       class_name: InferredAssetAzureDataConnector
       datasource_name: test_environment
       container: my_container
       name_starts_with: ""
       default_regex:
           pattern: (.+)_(.+)_(.+)\\.csv
           group_names:
               - name
               - timestamp
               - price
       sorters:
           - orderby: asc
             class_name: LexicographicSorter
             name: name
           - datetime_format: "%Y%m%d"
             orderby: desc
             class_name: DateTimeSorter
             name: timestamp
           - orderby: desc
             class_name: NumericSorter
             name: price
       azure_options:
           account_url: my_account_url.blob.core.windows.net
           credential: my_credential
     """,
    )

    mock_list_keys.return_value = [
        "alex_20200809_1000.csv",
        "eugene_20200809_1500.csv",
        "james_20200811_1009.csv",
        "abe_20200809_1040.csv",
        "will_20200809_1002.csv",
        "james_20200713_1567.csv",
        "eugene_20201129_1900.csv",
        "will_20200810_1001.csv",
        "james_20200810_1003.csv",
        "alex_20200819_1300.csv",
    ]

    my_data_connector: InferredAssetAzureDataConnector = instantiate_class_from_config(
        config=my_data_connector_yaml,
        runtime_environment={
            "name": "general_azure_data_connector",
            "execution_engine": PandasExecutionEngine(),
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )

    sorted_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        BatchRequest(
            datasource_name="test_environment",
            data_connector_name="general_azure_data_connector",
            data_asset_name="DEFAULT_ASSET_NAME",
        )
    )
    assert sorted_batch_definition_list == expected_batch_definitions_sorted


# noinspection PyUnusedLocal
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.azure.BlobServiceClient"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.list_azure_keys"
)
def test_return_all_batch_definitions_raises_error_due_to_sorter_that_does_not_match_group(
    mock_azure_conn, mock_list_keys, empty_data_context_stats_enabled
):
    my_data_connector_yaml = yaml.load(
        """
       class_name: InferredAssetAzureDataConnector
       datasource_name: test_environment
       container: my_container
       default_regex:
           pattern: (.+)_.+_.+\\.csv
           group_names:
               - name
       sorters:
           - orderby: asc
             class_name: LexicographicSorter
             name: name
           - datetime_format: "%Y%m%d"
             orderby: desc
             class_name: DateTimeSorter
             name: timestamp
           - orderby: desc
             class_name: NumericSorter
             name: for_me_Me_Me

       azure_options:
           account_url: my_account_url.blob.core.windows.net
           credential: my_credential
   """,
    )

    mock_list_keys.return_value = [
        "alex_20200809_1000.csv",
        "eugene_20200809_1500.csv",
        "james_20200811_1009.csv",
        "abe_20200809_1040.csv",
        "will_20200809_1002.csv",
        "james_20200713_1567.csv",
        "eugene_20201129_1900.csv",
        "will_20200810_1001.csv",
        "james_20200810_1003.csv",
        "alex_20200819_1300.csv",
    ]

    # Raises error due to a sorter (for_me_Me_me) not matching a group_name in `FilePathDataConnector._validate_sorters_configuration()`  # noqa: E501
    with pytest.raises(gx_exceptions.DataConnectorError):
        instantiate_class_from_config(
            config=my_data_connector_yaml,
            runtime_environment={
                "name": "general_azure_data_connector",
                "execution_engine": PandasExecutionEngine(),
            },
            config_defaults={"module_name": "great_expectations.datasource.data_connector"},
        )


# noinspection PyUnusedLocal
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.azure.BlobServiceClient"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.list_azure_keys"
)
def test_return_all_batch_definitions_too_many_sorters(
    mock_azure_conn, mock_list_keys, empty_data_context_stats_enabled
):
    my_data_connector_yaml = yaml.load(
        """
       class_name: InferredAssetAzureDataConnector
       datasource_name: test_environment
       container: my_container
       name_starts_with: ""
       default_regex:
           pattern: (.+)_.+_.+\\.csv
           group_names:
               - name
       sorters:
           - orderby: asc
             class_name: LexicographicSorter
             name: name
           - datetime_format: "%Y%m%d"
             orderby: desc
             class_name: DateTimeSorter
             name: timestamp
           - orderby: desc
             class_name: NumericSorter
             name: price

       azure_options:
           account_url: my_account_url.blob.core.windows.net
           credential: my_credential
   """,
    )

    mock_list_keys.return_value = [
        "alex_20200809_1000.csv",
        "eugene_20200809_1500.csv",
        "james_20200811_1009.csv",
        "abe_20200809_1040.csv",
        "will_20200809_1002.csv",
        "james_20200713_1567.csv",
        "eugene_20201129_1900.csv",
        "will_20200810_1001.csv",
        "james_20200810_1003.csv",
        "alex_20200819_1300.csv",
    ]

    # Raises error due to a non-existent sorter being specified in `FilePathDataConnector._validate_sorters_configuration()`  # noqa: E501
    with pytest.raises(gx_exceptions.DataConnectorError):
        instantiate_class_from_config(
            config=my_data_connector_yaml,
            runtime_environment={
                "name": "general_azure_data_connector",
                "execution_engine": PandasExecutionEngine(),
            },
            config_defaults={"module_name": "great_expectations.datasource.data_connector"},
        )


# noinspection PyUnusedLocal
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.azure.BlobServiceClient"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.list_azure_keys",
)
def test_get_full_file_path_pandas(
    mock_azure_conn, mock_list_keys, empty_data_context_stats_enabled
):
    yaml_string = """
class_name: InferredAssetAzureDataConnector
datasource_name: FAKE_DATASOURCE_NAME
container: my_container
name_starts_with: my_base_directory/
default_regex:
    pattern: ^(.+)-(\\d{{4}})(\\d{{2}})\\.(csv|txt)$
    group_names:
        - data_asset_name
        - year_dir
        - month_dir
azure_options:
    account_url: my_account_url.blob.core.windows.net
    credential: my_credential
    """
    config = yaml.load(yaml_string)

    my_data_connector: InferredAssetAzureDataConnector = instantiate_class_from_config(
        config,
        runtime_environment={
            "name": "my_data_connector",
            "execution_engine": PandasExecutionEngine(),
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )

    mock_list_keys.return_value = [
        "my_base_directory/alpha/files/go/here/alpha-202001.csv",
        "my_base_directory/alpha/files/go/here/alpha-202002.csv",
        "my_base_directory/alpha/files/go/here/alpha-202003.csv",
        "my_base_directory/beta_here/beta-202001.txt",
        "my_base_directory/beta_here/beta-202002.txt",
        "my_base_directory/beta_here/beta-202003.txt",
        "my_base_directory/beta_here/beta-202004.txt",
        "my_base_directory/gamma-202001.csv",
        "my_base_directory/gamma-202002.csv",
        "my_base_directory/gamma-202003.csv",
        "my_base_directory/gamma-202004.csv",
        "my_base_directory/gamma-202005.csv",
    ]

    assert (
        my_data_connector._get_full_file_path(
            path="my_base_directory/alpha/files/go/here/alpha-202001.csv",
            data_asset_name="alpha",
        )
        == "my_account_url.blob.core.windows.net/my_container/my_base_directory/alpha/files/go/here/alpha-202001.csv"  # noqa: E501
    )
    assert (
        my_data_connector._get_full_file_path(
            path="my_base_directory/beta_here/beta-202002.txt", data_asset_name="beta"
        )
        == "my_account_url.blob.core.windows.net/my_container/my_base_directory/beta_here/beta-202002.txt"  # noqa: E501
    )
    assert (
        my_data_connector._get_full_file_path(
            path="my_base_directory/gamma-202005.csv", data_asset_name="gamma"
        )
        == "my_account_url.blob.core.windows.net/my_container/my_base_directory/gamma-202005.csv"
    )


# noinspection PyUnusedLocal
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.azure.BlobServiceClient"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.list_azure_keys",
)
def test_get_full_file_path_spark(
    mock_azure_conn,
    mock_list_keys,
    empty_data_context_stats_enabled,
    spark_session,
):
    yaml_string = """
class_name: InferredAssetAzureDataConnector
datasource_name: FAKE_DATASOURCE_NAME
container: my_container
name_starts_with: my_base_directory/
default_regex:
    pattern: ^(.+)-(\\d{{4}})(\\d{{2}})\\.(csv|txt)$
    group_names:
        - data_asset_name
        - year_dir
        - month_dir
azure_options:
    account_url: my_account_url.blob.core.windows.net
    credential: my_credential
    """
    config = yaml.load(yaml_string)

    my_data_connector: InferredAssetAzureDataConnector = instantiate_class_from_config(
        config,
        runtime_environment={
            "name": "my_data_connector",
            "execution_engine": SparkDFExecutionEngine(),
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )

    mock_list_keys.return_value = [
        "my_base_directory/alpha/files/go/here/alpha-202001.csv",
        "my_base_directory/alpha/files/go/here/alpha-202002.csv",
        "my_base_directory/alpha/files/go/here/alpha-202003.csv",
        "my_base_directory/beta_here/beta-202001.txt",
        "my_base_directory/beta_here/beta-202002.txt",
        "my_base_directory/beta_here/beta-202003.txt",
        "my_base_directory/beta_here/beta-202004.txt",
        "my_base_directory/gamma-202001.csv",
        "my_base_directory/gamma-202002.csv",
        "my_base_directory/gamma-202003.csv",
        "my_base_directory/gamma-202004.csv",
        "my_base_directory/gamma-202005.csv",
    ]

    assert (
        my_data_connector._get_full_file_path(
            path="my_base_directory/alpha/files/go/here/alpha-202001.csv",
            data_asset_name="alpha",
        )
        == "wasbs://my_container@my_account_url.blob.core.windows.net/my_base_directory/alpha/files/go/here/alpha-202001.csv"
    )
    assert (
        my_data_connector._get_full_file_path(
            path="my_base_directory/beta_here/beta-202002.txt", data_asset_name="beta"
        )
        == "wasbs://my_container@my_account_url.blob.core.windows.net/my_base_directory/beta_here/beta-202002.txt"
    )
    assert (
        my_data_connector._get_full_file_path(
            path="my_base_directory/gamma-202005.csv", data_asset_name="gamma"
        )
        == "wasbs://my_container@my_account_url.blob.core.windows.net/my_base_directory/gamma-202005.csv"
    )


# noinspection PyUnusedLocal
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.azure.BlobServiceClient"
)
@mock.patch(
    "great_expectations.datasource.data_connector.inferred_asset_azure_data_connector.list_azure_keys",
)
def test_get_full_file_path_bad_execution_engine(
    mock_azure_conn, mock_list_keys, empty_data_context_stats_enabled
):
    yaml_string = """
class_name: InferredAssetAzureDataConnector
datasource_name: FAKE_DATASOURCE_NAME
container: my_container
name_starts_with: my_base_directory/
default_regex:
    pattern: ^(.+)-(\\d{{4}})(\\d{{2}})\\.(csv|txt)$
    group_names:
        - data_asset_name
        - year_dir
        - month_dir
azure_options:
    account_url: my_account_url.blob.core.windows.net
    credential: my_credential
    """
    config = yaml.load(yaml_string)

    # Raises error due to a non-existent/unknown ExecutionEngine instance.
    with pytest.raises(gx_exceptions.DataConnectorError):
        # noinspection PyUnusedLocal
        my_data_connector: InferredAssetAzureDataConnector = (  # noqa: F841
            instantiate_class_from_config(
                config,
                runtime_environment={
                    "name": "my_data_connector",
                },
                config_defaults={"module_name": "great_expectations.datasource.data_connector"},
            )
        )
