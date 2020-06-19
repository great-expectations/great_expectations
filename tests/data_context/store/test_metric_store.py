import pytest

from great_expectations.data_context.util import instantiate_class_from_config


@pytest.fixture(
    params=[
        {
            "class_name": "MetricStore",
            "store_backend": {
                "class_name": "DatabaseStoreBackend",
                "credentials": {
                    "drivername": "postgresql",
                    "username": "postgres",
                    "password": "",
                    "host": "localhost",
                    "port": "5432",
                    "database": "test_ci",
                },
            },
        },
        {
            "class_name": "MetricStore",
            "module_name": "great_expectations.data_context.store",
        },
    ]
)
def param_store(request, test_backends):
    # If we have a backend configuration but we do not have postgres configured, skip
    backend_config = request.param.get("store_backend", None)
    if backend_config:
        if (
            backend_config.get("credentials", {}).get("drivername", None)
            == "postgresql"
        ):
            if "postgresql" not in test_backends:
                pytest.skip("skipping fixture because postgresql not selected")

    return instantiate_class_from_config(
        config=request.param,
        config_defaults={"module_name": "great_expectations.data_context.store",},
        runtime_environment={},
    )
