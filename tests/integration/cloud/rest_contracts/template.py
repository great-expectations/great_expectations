import pytest
from pact import Like


@pytest.mark.cloud
def test_get_datasource(pact, cloud_data_context):
    datasource_name = "pandas_datasource"
    expected = {
        "name": datasource_name,
        "type": "pandas",
    }

    (
        pact.given(f"{datasource_name} exists")
        .upon_receiving(f"a request for {datasource_name}")
        .with_request(
            "get", "/organizations/uuid:organization_id/datasources/uuid:datasource_id"
        )
        .will_respond_with(200, body=Like(expected))
    )

    with pact:
        cloud_data_context.get_datasource(datasource_name=datasource_name)

    # assert something with the result, for ex, did I process 'result' properly?
    # or was I able to deserialize correctly? etc.
    assert False
