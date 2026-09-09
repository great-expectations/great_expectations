from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from great_expectations.data_context.store.data_asset_store import DataAssetStore

if TYPE_CHECKING:
    from great_expectations.data_context.store.data_asset_store import (
        CloudResponsePayloadTD,
    )


@pytest.mark.cloud
@pytest.mark.parametrize(
    "response_json, expected, error_type",
    [
        pytest.param(
            {
                "data": {
                    "id": "03d61d4e-003f-48e7-a3b2-f9f842384da3",
                    "attributes": {
                        "data_asset_config": {
                            "name": "my_asset",
                            "type": "pandas",
                        },
                    },
                }
            },
            {
                "id": "03d61d4e-003f-48e7-a3b2-f9f842384da3",
                "name": "my_asset",
                "type": "pandas",
            },
            None,
            id="single_config",
        ),
        pytest.param(
            {
                "data": [
                    {
                        "id": "03d61d4e-003f-48e7-a3b2-f9f842384da3",
                        "attributes": {
                            "data_asset_config": {
                                "name": "my_asset",
                                "type": "pandas",
                            },
                        },
                    }
                ]
            },
            {
                "id": "03d61d4e-003f-48e7-a3b2-f9f842384da3",
                "name": "my_asset",
                "type": "pandas",
            },
            None,
            id="single_config_in_list",
        ),
        pytest.param(
            {
                "data": [
                    {
                        "data": [
                            {
                                "id": "03d61d4e-003f-48e7-a3b2-f9f842384da3",
                                "attributes": {
                                    "data_asset_config": {
                                        "name": "my_asset",
                                        "type": "pandas",
                                    },
                                },
                            }
                        ]
                    },
                    {
                        "data": [
                            {
                                "id": "ffg61d4e-003f-48e7-a3b2-f9f842384da3",
                                "attributes": {
                                    "data_asset_config": {
                                        "name": "my_other_asset",
                                        "type": "pandas",
                                    },
                                },
                            }
                        ]
                    },
                ]
            },
            None,
            TypeError,
            id="multiple_config_in_list",
        ),
    ],
)
def test_gx_cloud_response_json_to_object_dict(
    response_json: CloudResponsePayloadTD, expected: dict | None, error_type: type[Exception] | None
) -> None:
    if error_type:
        with pytest.raises(error_type):
            _ = DataAssetStore.gx_cloud_response_json_to_object_dict(response_json)
    else:
        actual = DataAssetStore.gx_cloud_response_json_to_object_dict(response_json)
        assert actual == expected
