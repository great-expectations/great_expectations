from unittest.mock import Mock

import pytest

from great_expectations.data_context.types.base import (
    ExecutionEngineConfigSchema,
)


@pytest.mark.unit
@pytest.mark.parametrize(
    "connect_args",
    [
        {"connection_factory": Mock()},
        {"ssl_verify_cert": False},
        {"timeout": 30},
        {
            "ssl": {
                "ssl_ca": "ca.pem",
                "ssl_cert": "client-cert.pem",
                "ssl_key": "client-key.pem",
            }
        },
    ],
)
def test_execution_engine_config_conect_args(connect_args):
    """
    this is part of a test-driven fix for: https://github.com/great-expectations/great_expectations/issues/6226
    connect_args examples are here: https://docs.sqlalchemy.org/en/20/core/engines.html#use-the-connect-args-dictionary-parameter
    """
    cfg = ExecutionEngineConfigSchema().load(
        {
            "class_name": "SqlAlchemyExecutionEngine",
            "module_name": "great_expectations.execution_engine",
            "connection_string": "sqlite://",
            "connect_args": connect_args,
        }
    )

    assert cfg.connect_args == connect_args
