from __future__ import annotations

import base64
import errno
import json
import logging
import os
import re
from abc import ABC, abstractmethod
from collections import OrderedDict
from functools import lru_cache
from typing import Any, Dict, Optional, Type, cast

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.types.base import BaseYamlConfig, GeCloudConfig

try:
    from azure.identity import DefaultAzureCredential
    from azure.keyvault.secrets import SecretClient
except ImportError:
    SecretClient = None
    DefaultAzureCredential = None

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    boto3 = None
    ClientError = None

try:
    from google.cloud import secretmanager
except ImportError:
    secretmanager = None

logger = logging.getLogger(__name__)
yaml = YAMLHandler()


class ConfigurationSubstitutor:
    def substitute_all_config_variables(
        self, data, replace_variables_dict, dollar_sign_escape_string: str = r"\$"
    ):
        """
        Substitute all config variables of the form ${SOME_VARIABLE} in a dictionary-like
        config object for their values.

        The method traverses the dictionary recursively.

        :param data:
        :param replace_variables_dict:
        :param dollar_sign_escape_string: a reserved character for specifying parameters
        :return: a dictionary with all the variables replaced with their values
        """
        if isinstance(data, BaseYamlConfig):
            data = (data.__class__.get_schema_class())().dump(data)

        if isinstance(data, dict) or isinstance(data, OrderedDict):
            return {
                k: self.substitute_all_config_variables(v, replace_variables_dict)
                for k, v in data.items()
            }
        elif isinstance(data, list):
            return [
                self.substitute_all_config_variables(v, replace_variables_dict)
                for v in data
            ]
        return self.substitute_config_variable(
            data, replace_variables_dict, dollar_sign_escape_string
        )

    def substitute_config_variable(
        self,
        template_str,
        config_variables_dict,
        dollar_sign_escape_string: str = r"\$",
    ) -> Optional[str]:
        """
        This method takes a string, and if it contains a pattern ${SOME_VARIABLE} or $SOME_VARIABLE,
        returns a string where the pattern is replaced with the value of SOME_VARIABLE,
        otherwise returns the string unchanged. These patterns are case sensitive. There can be multiple
        patterns in a string, e.g. all 3 will be substituted in the following:
        $SOME_VARIABLE${some_OTHER_variable}$another_variable

        If the environment variable SOME_VARIABLE is set, the method uses its value for substitution.
        If it is not set, the value of SOME_VARIABLE is looked up in the config variables store (file).
        If it is not found there, the input string is returned as is.

        If the value to substitute is not a string, it is returned as-is.

        If the value to substitute begins with dollar_sign_escape_string it is not substituted.

        If the value starts with the keyword `secret|`, it tries to apply secret store substitution.

        :param template_str: a string that might or might not be of the form ${SOME_VARIABLE}
                or $SOME_VARIABLE
        :param config_variables_dict: a dictionary of config variables. It is loaded from the
                config variables store (by default, "uncommitted/config_variables.yml file)
        :param dollar_sign_escape_string: a string that will be used in place of a `$` when substitution
                is not desired.

        :return: a string with values substituted, or the same object if template_str is not a string.
        """

        if template_str is None:
            return template_str

        # 1. Make substitutions for non-escaped patterns
        try:
            match = re.finditer(
                r"(?<!\\)\$\{(.*?)\}|(?<!\\)\$([_a-zA-Z][_a-zA-Z0-9]*)", template_str
            )
        except TypeError:
            # If the value is not a string (e.g., a boolean), we should return it as is
            return template_str

        for m in match:
            # Match either the first group e.g. ${Variable} or the second e.g. $Variable
            config_variable_name = m.group(1) or m.group(2)
            config_variable_value = config_variables_dict.get(config_variable_name)

            if config_variable_value is not None:
                if not isinstance(config_variable_value, str):
                    return config_variable_value
                template_str = template_str.replace(m.group(), config_variable_value)
            else:
                raise ge_exceptions.MissingConfigVariableError(
                    f"""\n\nUnable to find a match for config substitution variable: `{config_variable_name}`.
    Please add this missing variable to your `uncommitted/config_variables.yml` file or your environment variables.
    See https://docs.greatexpectations.io/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials""",
                    missing_config_variable=config_variable_name,
                )

        # 2. Replace the "$"'s that had been escaped
        template_str = template_str.replace(dollar_sign_escape_string, "$")
        template_str = self._substitute_value_from_secret_store(template_str)
        return template_str

    @lru_cache(maxsize=None)
    def _substitute_value_from_secret_store(self, value):
        """
        This method takes a value, tries to parse the value to fetch a secret from a secret manager
        and returns the secret's value only if the input value is a string and contains one of the following patterns:

        - AWS Secrets Manager: the input value starts with ``secret|arn:aws:secretsmanager``

        - GCP Secret Manager: the input value matches the following regex ``^secret\\|projects\\/[a-z0-9\\_\\-]{6,30}\\/secrets``

        - Azure Key Vault: the input value matches the following regex ``^secret\\|https:\\/\\/[a-zA-Z0-9\\-]{3,24}\\.vault\\.azure\\.net``

        Input value examples:

        - AWS Secrets Manager: ``secret|arn:aws:secretsmanager:eu-west-3:123456789012:secret:my_secret``

        - GCP Secret Manager: ``secret|projects/gcp_project_id/secrets/my_secret``

        - Azure Key Vault: ``secret|https://vault-name.vault.azure.net/secrets/my-secret``

        :param value: a string that might or might not start with `secret|`

        :return: a string with the value substituted by the secret from the secret store,
                or the same object if value is not a string.
        """
        if isinstance(value, str) and value.startswith("secret|"):
            if value.startswith("secret|arn:aws:secretsmanager"):
                return self._substitute_value_from_aws_secrets_manager(value)
            elif re.compile(r"^secret\|projects\/[a-z0-9\_\-]{6,30}\/secrets").match(
                value
            ):
                return self._substitute_value_from_gcp_secret_manager(value)
            elif re.compile(
                r"^secret\|https:\/\/[a-zA-Z0-9\-]{3,24}\.vault\.azure\.net"
            ).match(value):
                return self._substitute_value_from_azure_keyvault(value)
        return value

    def _substitute_value_from_aws_secrets_manager(self, value):
        """
        This methods uses a boto3 client and the secretsmanager service to try to retrieve the secret value
        from the elements it is able to parse from the input value.

        - value: string with pattern ``secret|arn:aws:secretsmanager:${region_name}:${account_id}:secret:${secret_name}``

            optional : after the value above, a secret version can be added ``:${secret_version}``

            optional : after the value above, a secret key can be added ``|${secret_key}``

        - region_name: `AWS region used by the secrets manager <https://docs.aws.amazon.com/general/latest/gr/rande.html>`_
        - account_id: `Account ID for the AWS account used by the secrets manager <https://docs.aws.amazon.com/en_us/IAM/latest/UserGuide/console_account-alias.html>`_

                This value is currently not used.
        - secret_name: Name of the secret
        - secret_version: UUID of the version of the secret
        - secret_key: Only if the secret's data is a JSON string, which key of the dict should be retrieve

        :param value: a string that starts with ``secret|arn:aws:secretsmanager``

        :return: a string with the value substituted by the secret from the AWS Secrets Manager store

        :raises: ImportError, ValueError
        """
        regex = re.compile(
            r"^secret\|arn:aws:secretsmanager:([a-z\-0-9]+):([0-9]{12}):secret:([a-zA-Z0-9\/_\+=\.@\-]+)"
            r"(?:\:([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}))?(?:\|([^\|]+))?$"
        )
        if not boto3:
            logger.error(
                "boto3 is not installed, please install great_expectations with aws_secrets extra > "
                "pip install great_expectations[aws_secrets]"
            )
            raise ImportError("Could not import boto3")

        matches = regex.match(value)

        if not matches:
            raise ValueError(f"Could not match the value with regex {regex}")

        region_name = matches.group(1)
        secret_name = matches.group(3)
        secret_version = matches.group(4)
        secret_key = matches.group(5)

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=region_name)

        if secret_version:
            secret_response = client.get_secret_value(
                SecretId=secret_name, VersionId=secret_version
            )
        else:
            secret_response = client.get_secret_value(SecretId=secret_name)
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if "SecretString" in secret_response:
            secret = secret_response["SecretString"]
        else:
            secret = base64.b64decode(secret_response["SecretBinary"]).decode("utf-8")
        if secret_key:
            secret = json.loads(secret)[secret_key]
        return secret

    def _substitute_value_from_gcp_secret_manager(self, value):
        """
        This methods uses a google.cloud.secretmanager.SecretManagerServiceClient to try to retrieve the secret value
        from the elements it is able to parse from the input value.

        value: string with pattern ``secret|projects/${project_id}/secrets/${secret_name}``

            optional : after the value above, a secret version can be added ``/versions/${secret_version}``

            optional : after the value above, a secret key can be added ``|${secret_key}``

        - project_id: `Project ID of the GCP project on which the secret manager is implemented <https://cloud.google.com/resource-manager/docs/creating-managing-projects#before_you_begin>`_
        - secret_name: Name of the secret
        - secret_version: ID of the version of the secret
        - secret_key: Only if the secret's data is a JSON string, which key of the dict should be retrieve

        :param value: a string that matches the following regex ``^secret|projects/[a-z0-9_-]{6,30}/secrets``

        :return: a string with the value substituted by the secret from the GCP Secret Manager store
        :raises: ImportError, ValueError
        """
        regex = re.compile(
            r"^secret\|projects\/([a-z0-9\_\-]{6,30})\/secrets/([a-zA-Z\_\-]{1,255})"
            r"(?:\/versions\/([a-z0-9]+))?(?:\|([^\|]+))?$"
        )
        if not secretmanager:
            logger.error(
                "secretmanager is not installed, please install great_expectations with gcp extra > "
                "pip install great_expectations[gcp]"
            )
            raise ImportError("Could not import secretmanager from google.cloud")

        client = secretmanager.SecretManagerServiceClient()
        matches = regex.match(value)

        if not matches:
            raise ValueError(f"Could not match the value with regex {regex}")

        project_id = matches.group(1)
        secret_id = matches.group(2)
        secret_version = matches.group(3)
        secret_key = matches.group(4)
        if not secret_version:
            secret_version = "latest"
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{secret_version}"
        try:
            secret = client.access_secret_version(name=name)._pb.payload.data.decode(
                "utf-8"
            )
        except AttributeError:
            secret = client.access_secret_version(name=name).payload.data.decode(
                "utf-8"
            )  # for google-cloud-secret-manager < 2.0.0
        if secret_key:
            secret = json.loads(secret)[secret_key]
        return secret

    def _substitute_value_from_azure_keyvault(self, value):
        """
        This methods uses a azure.identity.DefaultAzureCredential to authenticate to the Azure SDK for Python
        and a azure.keyvault.secrets.SecretClient to try to retrieve the secret value from the elements
        it is able to parse from the input value.

        - value: string with pattern ``secret|https://${vault_name}.vault.azure.net/secrets/${secret_name}``

            optional : after the value above, a secret version can be added ``/${secret_version}``

            optional : after the value above, a secret key can be added ``|${secret_key}``

        - vault_name: `Vault name of the secret manager <https://docs.microsoft.com/en-us/azure/key-vault/general/about-keys-secrets-certificates#objects-identifiers-and-versioning>`_
        - secret_name: Name of the secret
        - secret_version: ID of the version of the secret
        - secret_key: Only if the secret's data is a JSON string, which key of the dict should be retrieve

        :param value: a string that matches the following regex ``^secret|https://[a-zA-Z0-9-]{3,24}.vault.azure.net``

        :return: a string with the value substituted by the secret from the Azure Key Vault store
        :raises: ImportError, ValueError
        """
        regex = re.compile(
            r"^secret\|(https:\/\/[a-zA-Z0-9\-]{3,24}\.vault\.azure\.net)\/secrets\/([0-9a-zA-Z-]+)"
            r"(?:\/([a-f0-9]{32}))?(?:\|([^\|]+))?$"
        )
        if not SecretClient:
            logger.error(
                "SecretClient is not installed, please install great_expectations with azure_secrets extra > "
                "pip install great_expectations[azure_secrets]"
            )
            raise ImportError(
                "Could not import SecretClient from azure.keyvault.secrets"
            )
        matches = regex.match(value)

        if not matches:
            raise ValueError(f"Could not match the value with regex {regex}")

        keyvault_uri = matches.group(1)
        secret_name = matches.group(2)
        secret_version = matches.group(3)
        secret_key = matches.group(4)
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=keyvault_uri, credential=credential)
        secret = client.get_secret(name=secret_name, version=secret_version).value
        if secret_key:
            secret = json.loads(secret)[secret_key]
        return secret


class AbstractConfigurationProvider(ABC):
    def __init__(self) -> None:
        self._substitutor = ConfigurationSubstitutor()

    @abstractmethod
    def get_values(self) -> Dict[str, str]:
        """
        Retrieve any configuration variables relevant to the provider's environment.
        """
        pass


class ConfigurationProvider(AbstractConfigurationProvider):
    """
    Wrapper class around the other environment-specific configuraiton provider classes.

    Based on relevance, specific providers are registered to this object and are invoked
    using the API defined by the AbstractConfigurationProvider.

    In short, this class' purpose is to aggregate all configuration variables that may
    be present for a given user environment (config variables, env vars, runtime environment, etc.)
    """

    def __init__(self) -> None:
        self._providers: OrderedDict[
            Type[AbstractConfigurationProvider], AbstractConfigurationProvider
        ] = OrderedDict()

    def register_provider(self, provider: AbstractConfigurationProvider) -> None:
        """
        Saves a configuration provider to the object's state for downstream usage.
        See `get_values()` for more information.

        Args:
            provider: An instance of a provider to register.
        """
        type_ = type(provider)
        if type_ in self._providers:
            raise ValueError(f"Provider of type {type_} has already been registered!")
        self._providers[type_] = provider

    def get_provider(
        self, type_: Type[AbstractConfigurationProvider]
    ) -> Optional[AbstractConfigurationProvider]:
        """
        Retrieves a registered configuration provider (if available).

        Args:
            type_: The class of the configuration provider to retrieve.

        Returns:
            A registered provider if available.
            If not, None is returned.
        """
        return self._providers.get(type_)

    def get_values(self) -> Dict[str, str]:
        """
        Iterates through all registered providers to aggregate a list of configuration values.

        Values are generated based on the order of registration; if there is a conflict,
        subsequent providers will overwrite existing values.
        """
        values: Dict[str, str] = {}
        for provider in self._providers.values():
            values.update(provider.get_values())
        return values

    def substitute_config(self, config: Any) -> Any:
        config_values = self.get_values()
        return self._substitutor.substitute_all_config_variables(config, config_values)


class RuntimeEnvironmentConfigurationProvider(AbstractConfigurationProvider):
    """
    Responsible for the management of the runtime_environment dictionary provided at runtime.
    """

    def __init__(self, runtime_environment: Dict[str, str]) -> None:
        self._runtime_environment = runtime_environment

    def get_values(self) -> Dict[str, str]:
        return self._runtime_environment


class EnvironmentConfigurationProvider(AbstractConfigurationProvider):
    """
    Responsible for the management of environment variables.
    """

    def get_values(self) -> Dict[str, str]:
        return dict(os.environ)


class ConfigurationVariablesConfigurationProvider(AbstractConfigurationProvider):
    """
    Responsible for the management of user-defined configuration variables.

    These can be found in the user's /uncommitted/config_variables.yml file.
    """

    def __init__(
        self, config_variables_file_path: str, root_directory: Optional[str] = None
    ) -> None:
        self._config_variables_file_path = config_variables_file_path
        self._root_directory = root_directory

    def get_values(self) -> Dict[str, str]:
        env_vars = dict(os.environ)
        try:
            # If the user specifies the config variable path with an environment variable, we want to substitute it
            defined_path: str = self._substitutor.substitute_config_variable(  # type: ignore[assignment]
                self._config_variables_file_path, env_vars
            )
            if not os.path.isabs(defined_path):
                root_directory: str = self._root_directory or os.curdir
            else:
                root_directory = ""

            var_path = os.path.join(root_directory, defined_path)
            with open(var_path) as config_variables_file:
                contents = config_variables_file.read()

            variables = dict(yaml.load(contents)) or {}
            return cast(
                Dict[str, str],
                self._substitutor.substitute_all_config_variables(variables, env_vars),
            )

        except OSError as e:
            if e.errno != errno.ENOENT:
                raise
            return {}


class CloudConfigurationProvider(AbstractConfigurationProvider):
    """
    Responsible for the management of a user's GX Cloud credentials.

    See `GeCloudConfig` for more information. Note that this is only registered on the primary
    config provider when in a Cloud-backend environment.
    """

    def __init__(self, cloud_config: GeCloudConfig) -> None:
        self._cloud_config = cloud_config

    def get_values(self) -> Dict[str, str]:
        from great_expectations.data_context.data_context.cloud_data_context import (
            GECloudEnvironmentVariable,
        )

        return {
            GECloudEnvironmentVariable.BASE_URL: self._cloud_config.base_url,
            GECloudEnvironmentVariable.ACCESS_TOKEN: self._cloud_config.access_token,
            GECloudEnvironmentVariable.ORGANIZATION_ID: self._cloud_config.organization_id,  # type: ignore[dict-item]
        }
