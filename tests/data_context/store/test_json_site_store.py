import pytest

from great_expectations.data_context.store.json_site_store import JsonSiteStore


@pytest.mark.cloud
def test_gx_cloud_response_json_to_object_dict():
    response_json = {
        "data": {
            "id": "683df05f-efec-48d4-a45a-02e8318043b8",
            "attributes": {
                "rendered_data_doc": {
                    "sections": [],
                    "data_asset_name": "my_asset",
                }
            },
        }
    }

    actual = JsonSiteStore.gx_cloud_response_json_to_object_dict(response_json)
    expected = {
        "id": "683df05f-efec-48d4-a45a-02e8318043b8",
        "sections": [],
        "data_asset_name": "my_asset",
    }

    assert actual == expected
