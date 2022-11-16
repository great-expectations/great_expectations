from contextlib import contextmanager
from unittest import mock

import pytest

from great_expectations.core.config_substitutor import (
    ConfigurationSubstitutor,
    secretmanager,
)


@pytest.fixture
def config_substitutor() -> ConfigurationSubstitutor:
    return ConfigurationSubstitutor()


@contextmanager
def does_not_raise():
    yield


@pytest.mark.parametrize(
    "input_value,method_to_patch,return_value",
    [
        ("any_value", None, "any_value"),
        ("secret|any_value", None, "secret|any_value"),
        (
            "secret|arn:aws:secretsmanager:region-name-1:123456789012:secret:my-secret",
            "great_expectations.core.config_substitutor.ConfigurationSubstitutor.substitute_value_from_aws_secrets_manager",
            "success",
        ),
        (
            "secret|projects/project_id/secrets/my_secret",
            "great_expectations.core.config_substitutor.ConfigurationSubstitutor.substitute_value_from_gcp_secret_manager",
            "success",
        ),
        (
            "secret|https://my-vault-name.vault.azure.net/secrets/my_secret",
            "great_expectations.core.config_substitutor.ConfigurationSubstitutor.substitute_value_from_azure_keyvault",
            "success",
        ),
    ],
)
@pytest.mark.unit
def test_substitute_value_from_secret_store(
    config_substitutor, input_value, method_to_patch, return_value
):
    if method_to_patch:
        with mock.patch(method_to_patch, return_value=return_value):
            assert (
                config_substitutor.substitute_value_from_secret_store(value=input_value)
                == return_value
            )
    else:
        assert (
            config_substitutor.substitute_value_from_secret_store(value=input_value)
            == return_value
        )


class MockedBoto3Client:
    def __init__(self, secret_response):
        self.secret_response = secret_response

    def get_secret_value(self, *args, **kwargs):
        return self.secret_response


class MockedBoto3Session:
    def __init__(self, secret_response):
        self.secret_response = secret_response

    def __call__(self):
        return self

    def client(self, *args, **kwargs):
        return MockedBoto3Client(self.secret_response)


@pytest.mark.parametrize(
    "input_value,secret_response,raises,expected",
    [
        (
            "secret|arn:aws:secretsmanager:region-name-1:123456789012:secret:my-secret",
            {"SecretString": "value"},
            does_not_raise(),
            "value",
        ),
        (
            "secret|arn:aws:secretsmanager:region-name-1:123456789012:secret:my-secret",
            {"SecretBinary": b"dmFsdWU="},
            does_not_raise(),
            "value",
        ),
        (
            "secret|arn:aws:secretsmanager:region-name-1:123456789012:secret:my-secret|key",
            {"SecretString": '{"key": "value"}'},
            does_not_raise(),
            "value",
        ),
        (
            "secret|arn:aws:secretsmanager:region-name-1:123456789012:secret:my-secret|key",
            {"SecretBinary": b"eyJrZXkiOiAidmFsdWUifQ=="},
            does_not_raise(),
            "value",
        ),
        (
            "secret|arn:aws:secretsmanager:region-name-1:123456789012:secret:my-se%&et|key",
            None,
            pytest.raises(ValueError),
            None,
        ),
        (
            "secret|arn:aws:secretsmanager:region-name-1:123456789012:secret:my-secret:000000000-0000-0000-0000-00000000000|key",
            None,
            pytest.raises(ValueError),
            None,
        ),
    ],
)
@pytest.mark.unit
def test_substitute_value_from_aws_secrets_manager(
    config_substitutor, input_value, secret_response, raises, expected
):
    with raises:
        with mock.patch(
            "great_expectations.core.config_substitutor.boto3.session.Session",
            return_value=MockedBoto3Session(secret_response),
        ):
            assert (
                config_substitutor.substitute_value_from_aws_secrets_manager(
                    input_value
                )
                == expected
            )


class MockedSecretManagerServiceClient:
    def __init__(self, secret_response):
        self.secret_response = secret_response

    def __call__(self):
        return self

    def access_secret_version(self, *args, **kwargs):
        class Response:
            pass

        response = Response()
        response._pb = Response()
        response._pb.payload = Response()
        response._pb.payload.data = self.secret_response

        return response


@pytest.mark.skipif(
    secretmanager is None,
    reason="Could not import 'secretmanager' from google.cloud in data_context.util",
)
@pytest.mark.parametrize(
    "input_value,secret_response,raises,expected",
    [
        (
            "secret|projects/project_id/secrets/my_secret",
            b"value",
            does_not_raise(),
            "value",
        ),
        (
            "secret|projects/project_id/secrets/my_secret|key",
            b'{"key": "value"}',
            does_not_raise(),
            "value",
        ),
        (
            "secret|projects/project_id/secrets/my_se%&et|key",
            None,
            pytest.raises(ValueError),
            None,
        ),
        (
            "secret|projects/project_id/secrets/my_secret/version/A|key",
            None,
            pytest.raises(ValueError),
            None,
        ),
    ],
)
@pytest.mark.unit
def test_substitute_value_from_gcp_secret_manager(
    config_substitutor, input_value, secret_response, raises, expected
):
    with raises:
        with mock.patch(
            "great_expectations.core.config_substitutor.secretmanager.SecretManagerServiceClient",
            return_value=MockedSecretManagerServiceClient(secret_response),
        ):
            assert (
                config_substitutor.substitute_value_from_gcp_secret_manager(input_value)
                == expected
            )


class MockedSecretClient:
    def __init__(self, secret_response):
        self.secret_response = secret_response

    def __call__(self, *args, **kwargs):
        return self

    def get_secret(self, *args, **kwargs):
        class Response:
            pass

        response = Response()
        response.value = self.secret_response
        return response


@mock.patch(
    "great_expectations.core.config_substitutor.DefaultAzureCredential", new=object
)
@pytest.mark.parametrize(
    "input_value,secret_response,raises,expected",
    [
        (
            "secret|https://my-vault-name.vault.azure.net/secrets/my-secret",
            "value",
            does_not_raise(),
            "value",
        ),
        (
            "secret|https://my-vault-name.vault.azure.net/secrets/my-secret|key",
            '{"key": "value"}',
            does_not_raise(),
            "value",
        ),
        (
            "secret|https://my-vault-name.vault.azure.net/secrets/my-se%&et|key",
            None,
            pytest.raises(ValueError),
            None,
        ),
        (
            "secret|https://my_vault_name.vault.azure.net/secrets/my-secret/A0000000000000000000000000000000|key",
            None,
            pytest.raises(ValueError),
            None,
        ),
    ],
)
@pytest.mark.unit
def test_substitute_value_from_azure_keyvault(
    config_substitutor, input_value, secret_response, raises, expected
):
    with raises:
        with mock.patch(
            "great_expectations.core.config_substitutor.SecretClient",
            return_value=MockedSecretClient(secret_response),
        ):
            assert (
                config_substitutor.substitute_value_from_azure_keyvault(input_value)
                == expected
            )
