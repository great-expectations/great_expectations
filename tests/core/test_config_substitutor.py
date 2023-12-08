from contextlib import contextmanager
from unittest import mock

import pytest

from great_expectations.compatibility import azure, google
from great_expectations.core.config_substitutor import (
    _ConfigurationSubstitutor,
)


@pytest.fixture
def config_substitutor() -> _ConfigurationSubstitutor:
    return _ConfigurationSubstitutor()


@contextmanager
def does_not_raise():
    yield


class MockedBoto3Client:
    def __init__(self, secret_response):
        self.secret_response = secret_response

    def get_secret_value(self, *args, **kwargs):
        return self.secret_response

    def get_parameter(self, *args, **kwargs):
        return self.get_secret_value(*args, **kwargs)


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
            "great_expectations.core.config_substitutor.aws.boto3.session.Session",
            return_value=MockedBoto3Session(secret_response),
        ):
            # As we're testing the secret store and not the actual substitution logic,
            # we deem the use of an empty config_variables_dict appropriate.
            assert (
                config_substitutor.substitute_config_variable(
                    template_str=input_value, config_variables_dict={}
                )
                == expected
            )


@pytest.mark.parametrize(
    "input_value,secret_response,raises,expected",
    [
        (
            "secret|arn:aws:ssm:region-name-1:123456789012:parameter/my-parameter",
            {
                "Parameter": {
                    "Type": "String",
                    "Value": "value",
                }
            },
            does_not_raise(),
            "value",
        ),
        (
            "secret|arn:aws:ssm:region-name-1:123456789012:parameter/my-parameter",
            {
                "Parameter": {
                    "Type": "SecureString",
                    "Value": "value",
                }
            },
            does_not_raise(),
            "value",
        ),
        (
            "secret|arn:aws:ssm:region-name-1:123456789012:parameter/my-parameter|key",
            {
                "Parameter": {
                    "Type": "SecureString",
                    "Value": '{"key": "value"}',
                }
            },
            does_not_raise(),
            "value",
        ),
        (
            "secret|arn:aws:ssm:region-name-1:123456789012:parameter/secure/my-parameter",
            {
                "Parameter": {
                    "Type": "SecureString",
                    "Value": "value",
                }
            },
            does_not_raise(),
            "value",
        ),
        (
            "secret|arn:aws:ssm:region-name-1:123456789012:parameter/secure/my-param%&eter",
            None,
            pytest.raises(ValueError),
            None,
        ),
    ],
)
@pytest.mark.unit
def test_substitute_value_from_aws_ssm(
    config_substitutor, input_value, secret_response, raises, expected
):
    with raises:
        with mock.patch(
            "great_expectations.core.config_substitutor.aws.boto3.session.Session",
            return_value=MockedBoto3Session(secret_response),
        ):
            assert (
                config_substitutor.substitute_config_variable(
                    template_str=input_value, config_variables_dict={}
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


# This test requires this import but monkeypatches external calls made to google.
@pytest.mark.skipif(
    not google.secretmanager,
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
            "great_expectations.core.config_substitutor.google.secretmanager.SecretManagerServiceClient",
            return_value=MockedSecretManagerServiceClient(secret_response),
        ):
            # As we're testing the secret store and not the actual substitution logic,
            # we deem the use of an empty config_variables_dict appropriate.
            assert (
                config_substitutor.substitute_config_variable(
                    template_str=input_value, config_variables_dict={}
                )
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
    "great_expectations.core.config_substitutor.azure.DefaultAzureCredential",
    new=object,
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
        # If the regex is not matched, we return the value as-is
        (
            "secret|https://my_vault_name.vault.azure.net/secrets/my-secret/A0000000000000000000000000000000|key",
            None,
            does_not_raise(),
            "secret|https://my_vault_name.vault.azure.net/secrets/my-secret/A0000000000000000000000000000000|key",
        ),
    ],
)
@pytest.mark.unit
# This test requires this import but monkeypatches external calls made to azure.
@pytest.mark.skipif(
    not (azure.storage and azure.SecretClient),
    reason='Could not import "azure.storage.blob" from Microsoft Azure cloud',
)
def test_substitute_value_from_azure_keyvault(
    config_substitutor, input_value, secret_response, raises, expected
):
    with raises:
        with mock.patch(
            "great_expectations.core.config_substitutor.azure.SecretClient",
            return_value=MockedSecretClient(secret_response),
        ):
            # As we're testing the secret store and not the actual substitution logic,
            # we deem the use of an empty config_variables_dict appropriate.
            assert (
                config_substitutor.substitute_config_variable(
                    template_str=input_value, config_variables_dict={}
                )
                == expected
            )
