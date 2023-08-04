import base64
import json
import logging
import re
from collections import OrderedDict
from functools import lru_cache
from typing import Any, Dict, Final, Optional

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility import aws, azure, google
from great_expectations.data_context.types.base import BaseYamlConfig

logger = logging.getLogger(__name__)

TEMPLATE_STR_REGEX: Final[re.Pattern] = re.compile(
    r"(?<!\\)\$\{(.*?)\}|(?<!\\)\$([_a-zA-Z][_a-zA-Z0-9]*)"
)


class _ConfigurationSubstitutor:
    """
    Responsible for encapsulating all logic around $VARIABLE (or ${VARIABLE}) substitution.

    While the config variables utilized for substitution are provided at runtime, all the
    behavior necessary to actually update config objects with their appropriate runtime values
    should be defined herein.
    """

    AWS_PATTERN = r"^secret\|arn:aws:secretsmanager:([a-z\-0-9]+):([0-9]{12}):secret:([a-zA-Z0-9\/_\+=\.@\-]+)"
    AWS_SSM_PATTERN = r"^secret\|arn:aws:ssm:([a-z\-0-9]+):([0-9]{12}):parameter\/([a-zA-Z0-9\/_\+=\.@\-]+)"

    GCP_PATTERN = (
        r"^secret\|projects\/([a-z0-9\_\-]{6,30})\/secrets/([a-zA-Z\_\-]{1,255})"
    )
    AZURE_PATTERN = r"^secret\|(https:\/\/[a-zA-Z0-9\-]{3,24}\.vault\.azure\.net)\/secrets\/([0-9a-zA-Z-]+)"

    def __init__(self) -> None:
        # Using the @lru_cache decorator on method calls can create memory leaks - an attr is preferred here.
        # Ref: https://stackoverflow.com/a/68550238
        self._secret_store_cache = lru_cache(maxsize=None)(
            self._substitute_value_from_secret_store
        )

    def substitute_all_config_variables(
        self,
        data: Any,
        replace_variables_dict: Dict[str, str],
        dollar_sign_escape_string: str = r"\$",
    ) -> Any:
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

        if isinstance(data, dict) or isinstance(data, OrderedDict):  # noqa: PLR1701
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
        template_str: str,
        config_variables_dict: Dict[str, str],
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
            match = re.finditer(TEMPLATE_STR_REGEX, template_str)
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
                raise gx_exceptions.MissingConfigVariableError(
                    f"""\n\nUnable to find a match for config substitution variable: `{config_variable_name}`.
    Please add this missing variable to your `uncommitted/config_variables.yml` file or your environment variables.
    See https://docs.greatexpectations.io/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials""",
                    missing_config_variable=config_variable_name,
                )

        # 2. Replace the "$"'s that had been escaped
        template_str = template_str.replace(dollar_sign_escape_string, "$")
        template_str = self._substitute_value_from_secret_store(template_str)
        return template_str

    def _substitute_value_from_secret_store(self, value: str) -> str:
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
        if isinstance(value, str):
            if re.match(self.AWS_PATTERN, value):
                return self._substitute_value_from_aws_secrets_manager(value)
            elif re.match(self.AWS_SSM_PATTERN, value):
                return self._substitute_value_from_aws_ssm(value)
            elif re.match(self.GCP_PATTERN, value):
                return self._substitute_value_from_gcp_secret_manager(value)
            elif re.match(self.AZURE_PATTERN, value):
                return self._substitute_value_from_azure_keyvault(value)
        return value

    def _substitute_value_from_aws_secrets_manager(self, value: str) -> str:
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
            rf"{self.AWS_PATTERN}(?:\:([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}))?(?:\|([^\|]+))?$"
        )
        if not aws.boto3:
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
        session = aws.boto3.session.Session()
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

    def _substitute_value_from_aws_ssm(self, value: str) -> str:
        """
        This methods uses a boto3 client and the systemmanager service to try to retrieve the secret value
        from the elements it is able to parse from the input value.

        - value: string with pattern ``secret|arn:aws:ssm:${region_name}:${account_id}:parameter:${secret_name}``

            optional : after the value above, a secret version can be added ``:${secret_version}``

            optional : after the value above, a secret key can be added ``|${secret_key}``

        - region_name: `AWS region used by the System Manager Parameter Store <https://docs.aws.amazon.com/general/latest/gr/rande.html>`_
        - account_id: `Account ID for the AWS account used by the parameter store <https://docs.aws.amazon.com/en_us/IAM/latest/UserGuide/console_account-alias.html>`_

                This value is currently not used.
        - secret_name: Name of the secret
        - secret_version: UUID of the version of the secret
        - secret_key: Only if the secret's data is a JSON string, which key of the dict should be retrieve

        :param value: a string that starts with ``secret|arn:aws:ssm``

        :return: a string with the value substituted by the secret from the AWS Secrets Manager store

        :raises: ImportError, ValueError
        """
        regex = re.compile(
            rf"{self.AWS_SSM_PATTERN}(?:\:([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}))?(?:\|([^\|]+))?$"
        )
        if not aws.boto3:
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
        session = aws.boto3.session.Session()

        client = session.client(service_name="ssm", region_name=region_name)

        if secret_version:
            secret_response = client.get_parameter(
                Name=secret_name, WithDecryption=True, Version=secret_version
            )
        else:
            secret_response = client.get_parameter(
                Name=secret_name, WithDecryption=True
            )
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        secret = secret_response["Parameter"]["Value"]

        if secret_key:
            secret = json.loads(secret_response["Parameter"]["Value"])[secret_key]

        return secret

    def _substitute_value_from_gcp_secret_manager(self, value: str) -> str:
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
            rf"{self.GCP_PATTERN}(?:\/versions\/([a-z0-9]+))?(?:\|([^\|]+))?$"
        )
        if not google.secretmanager:
            logger.error(
                "secretmanager is not installed, please install great_expectations with gcp extra > "
                "pip install great_expectations[gcp]"
            )
            raise ImportError("Could not import secretmanager from google.cloud")

        client = google.secretmanager.SecretManagerServiceClient()
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

    def _substitute_value_from_azure_keyvault(self, value: str) -> str:
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
            rf"{self.AZURE_PATTERN}(?:\/([a-f0-9]{32}))?(?:\|([^\|]+))?$"
        )
        if not azure.SecretClient:  # type: ignore[truthy-function] # False if NotImported
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
        credential = azure.DefaultAzureCredential()
        client = azure.SecretClient(vault_url=keyvault_uri, credential=credential)
        secret = client.get_secret(name=secret_name, version=secret_version).value
        if secret_key:
            secret = json.loads(secret)[secret_key]  # type: ignore[arg-type] # secret could be None
        return secret  # type: ignore[return-value] # secret could be None
