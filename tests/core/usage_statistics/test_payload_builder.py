from unittest import mock

import pytest

from great_expectations import __version__ as gx_version
from great_expectations.core.usage_statistics.payload_builder import (
    UsageStatisticsPayloadBuilder,
)
from great_expectations.core.usage_statistics.usage_statistics import (
    UsageStatisticsHandler,
)
from great_expectations.util import get_context


def test_build_init_payload(
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    """This test is for a happy path only but will fail if there is an exception thrown in init_payload"""

    context = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    usage_statistics_handler = context._usage_statistics_handler
    builder = usage_statistics_handler._builder
    init_payload = builder.build_init_payload()
    assert list(init_payload.keys()) == [
        "platform.system",
        "platform.release",
        "version_info",
        "datasources",
        "stores",
        "validation_operators",
        "data_docs_sites",
        "expectation_suites",
        "dependencies",
    ]

    datasources = init_payload["datasources"]
    assert len(datasources) == 1 and "my_datasource" in datasources

    assert init_payload["expectation_suites"] == []


def test_usage_statistics_handler_build_envelope(
    in_memory_data_context_config_usage_stats_enabled, sample_partial_message
):
    """This test is for a happy path only but will fail if there is an exception thrown in build_envelope"""

    context = get_context(in_memory_data_context_config_usage_stats_enabled)

    usage_statistics_handler = UsageStatisticsHandler(
        data_context=context,
        data_context_id=in_memory_data_context_config_usage_stats_enabled.anonymous_usage_statistics.data_context_id,
        usage_statistics_url=in_memory_data_context_config_usage_stats_enabled.anonymous_usage_statistics.usage_statistics_url,
    )

    assert (
        usage_statistics_handler._data_context_id
        == "00000000-0000-0000-0000-000000000001"
    )

    builder = usage_statistics_handler._builder
    envelope = builder.build_envelope(sample_partial_message)

    expected_keys = [
        "data_context_id",
        "data_context_instance_id",
        "event",
        "event_payload",
        "event_time",
        "ge_version",
        "mac_address",
        "success",
        "version",
        "x-forwarded-for",
    ]
    assert sorted(envelope.keys()) == expected_keys

    assert envelope["version"] == "2"
    assert envelope["data_context_id"] == "00000000-0000-0000-0000-000000000001"


@pytest.mark.unit
def test_determine_hashed_mac_address():
    builder = UsageStatisticsPayloadBuilder(
        data_context=mock.Mock(),
        data_context_id="00000000-0000-0000-0000-000000000001",
        gx_version=gx_version,
    )

    # Picking an arbitrary 48-bit positive integer as a mock MAC addr
    with mock.patch("uuid.getnode", return_value=170040650683345) as mock_node:
        hashed_mac_address = builder._determine_hashed_mac_address()

    mock_node.assert_called_once()
    assert (
        hashed_mac_address
        == "8422aebe6c3db9612f79d14c6b9280e65e53ef969db3aff4281e3035fb3ce86f"
    )
