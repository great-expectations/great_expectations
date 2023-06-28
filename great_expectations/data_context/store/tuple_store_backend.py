# PYTHON 2 - py2 - update to ABC direct use rather than __metaclass__ once we drop py2 support
import functools
import logging
import os
import random
import re
import shutil
from abc import ABCMeta
from typing import Any, List, Tuple

from great_expectations.compatibility import aws
from great_expectations.data_context.store.store_backend import StoreBackend
from great_expectations.exceptions import InvalidKeyError, StoreBackendError
from great_expectations.util import filter_properties_dict

logger = logging.getLogger(__name__)


class TupleStoreBackend(StoreBackend, metaclass=ABCMeta):
    r"""
    If filepath_template is provided, the key to this StoreBackend abstract class must be a tuple with
    fixed length equal to the number of unique components matching the regex r"{\d+}"

    For example, in the following template path: expectations/{0}/{1}/{2}/prefix-{2}.json, keys must have
    three components.
    """

    def __init__(  # noqa: PLR0913
        self,
        filepath_template=None,
        filepath_prefix=None,
        filepath_suffix=None,
        forbidden_substrings=None,
        platform_specific_separator=True,
        fixed_length_key=False,
        suppress_store_backend_id=False,
        manually_initialize_store_backend_id: str = "",
        base_public_path=None,
        store_name=None,
    ) -> None:
        super().__init__(
            fixed_length_key=fixed_length_key,
            suppress_store_backend_id=suppress_store_backend_id,
            manually_initialize_store_backend_id=manually_initialize_store_backend_id,
            store_name=store_name,
        )
        if forbidden_substrings is None:
            forbidden_substrings = ["/", "\\"]
        self.forbidden_substrings = forbidden_substrings
        self.platform_specific_separator = platform_specific_separator

        if filepath_template is not None and filepath_suffix is not None:
            raise ValueError(
                "filepath_suffix may only be used when filepath_template is None"
            )

        self.filepath_template = filepath_template
        if filepath_prefix and len(filepath_prefix) > 0:
            # Validate that the filepath prefix does not end with a forbidden substring
            if filepath_prefix[-1] in self.forbidden_substrings:
                raise StoreBackendError(
                    "Unable to initialize TupleStoreBackend: filepath_prefix may not end with a "
                    "forbidden substring. Current forbidden substrings are "
                    + str(forbidden_substrings)
                )
        self.filepath_prefix = filepath_prefix
        self.filepath_suffix = filepath_suffix
        self.base_public_path = base_public_path

        if filepath_template is not None:
            # key length is the number of unique values to be substituted in the filepath_template
            self.key_length = len(set(re.findall(r"{\d+}", filepath_template)))

            self.verify_that_key_to_filepath_operation_is_reversible()
            self._fixed_length_key = True

    def _validate_key(self, key) -> None:
        super()._validate_key(key)

        for key_element in key:
            for substring in self.forbidden_substrings:
                if substring in key_element:
                    raise ValueError(
                        "Keys in {} must not contain substrings in {} : {}".format(
                            self.__class__.__name__,
                            self.forbidden_substrings,
                            key,
                        )
                    )

    def _validate_value(self, value) -> None:
        if not isinstance(value, str) and not isinstance(value, bytes):
            raise TypeError(
                "Values in {} must be instances of {} or {}, not {}".format(
                    self.__class__.__name__,
                    str,
                    bytes,
                    type(value),
                )
            )

    def _convert_key_to_filepath(self, key):
        # NOTE: This method uses a hard-coded forward slash as a separator,
        # and then replaces that with a platform-specific separator if requested (the default)
        self._validate_key(key)
        # Handle store_backend_id separately
        if key == self.STORE_BACKEND_ID_KEY:
            filepath = f"{self.filepath_prefix or ''}{'/' if self.filepath_prefix else ''}{key[0]}"
            return (
                filepath
                if not self.platform_specific_separator
                else os.path.normpath(filepath)
            )
        if self.filepath_template:
            converted_string = self.filepath_template.format(*list(key))
        else:
            converted_string = "/".join(key)

        if self.filepath_prefix:
            converted_string = f"{self.filepath_prefix}/{converted_string}"
        if self.filepath_suffix:
            converted_string += self.filepath_suffix
        if self.platform_specific_separator:
            converted_string = os.path.normpath(converted_string)

        return converted_string

    def _convert_filepath_to_key(self, filepath):  # noqa: PLR0912
        if filepath == self.STORE_BACKEND_ID_KEY[0]:
            return self.STORE_BACKEND_ID_KEY
        if self.platform_specific_separator:
            filepath = os.path.normpath(filepath)

        if self.filepath_prefix:
            if (
                not filepath.startswith(self.filepath_prefix)
                and len(filepath) >= len(self.filepath_prefix) + 1
            ):
                # If filepath_prefix is set, we expect that it is the first component of a valid filepath.
                raise ValueError(
                    "filepath must start with the filepath_prefix when one is set by the store_backend"
                )
            else:
                # Remove the prefix before processing
                # Also remove the separator that was added, which may have been platform-dependent
                filepath = filepath[len(self.filepath_prefix) + 1 :]

        if self.filepath_suffix:
            if not filepath.endswith(self.filepath_suffix):
                # If filepath_suffix is set, we expect that it is the last component of a valid filepath.
                raise ValueError(
                    "filepath must end with the filepath_suffix when one is set by the store_backend"
                )
            else:
                # Remove the suffix before processing
                filepath = filepath[: -len(self.filepath_suffix)]

        if self.filepath_template:
            # filepath_template is always specified with forward slashes, but it is then
            # used to (1) dynamically construct and evaluate a regex, and (2) split the provided (observed) filepath
            if self.platform_specific_separator:
                filepath_template = os.path.join(  # noqa: PTH118
                    *self.filepath_template.split("/")
                )
                filepath_template = filepath_template.replace("\\", "\\\\")
            else:
                filepath_template = self.filepath_template

            # Convert the template to a regex
            indexed_string_substitutions = re.findall(r"{\d+}", filepath_template)
            tuple_index_list = [
                f"(?P<tuple_index_{i}>.*)"
                for i in range(len(indexed_string_substitutions))
            ]
            intermediate_filepath_regex = re.sub(
                r"{\d+}",
                lambda m, r=iter(  # noqa: B008 # function-call-in-default-argument
                    tuple_index_list
                ): next(r),
                filepath_template,
            )
            filepath_regex = intermediate_filepath_regex.format(*tuple_index_list)

            # Apply the regex to the filepath
            matches = re.compile(filepath_regex).match(filepath)
            if matches is None:
                return None

            # Map key elements into the appropriate parts of the tuple
            new_key = [None] * self.key_length
            for i in range(len(tuple_index_list)):
                tuple_index = int(
                    re.search(r"\d+", indexed_string_substitutions[i]).group(0)
                )
                key_element = matches.group(f"tuple_index_{str(i)}")
                new_key[tuple_index] = key_element

            new_key = tuple(new_key)
        else:
            filepath = os.path.normpath(filepath)
            new_key = tuple(filepath.split(os.sep))
        return new_key

    def verify_that_key_to_filepath_operation_is_reversible(self):
        def get_random_hex(size=4):
            return "".join(
                [random.choice(list("ABCDEF0123456789")) for _ in range(size)]
            )

        key = tuple(get_random_hex() for _ in range(self.key_length))
        filepath = self._convert_key_to_filepath(key)
        new_key = self._convert_filepath_to_key(filepath)
        if key != new_key:
            raise ValueError(
                "filepath template {} for class {} is not reversible for a tuple of length {}. "
                "Have you included all elements in the key tuple?".format(
                    self.filepath_template,
                    self.__class__.__name__,
                    self.key_length,
                )
            )

    @property
    def config(self) -> dict:
        return self._config  # type: ignore[attr-defined]


class TupleFilesystemStoreBackend(TupleStoreBackend):
    """Uses a local filepath as a store.

    The key to this StoreBackend must be a tuple with fixed length based on the filepath_template,
    or a variable-length tuple may be used and returned with an optional filepath_suffix (to be) added.
    The filepath_template is a string template used to convert the key to a filepath.
    """

    def __init__(  # noqa: PLR0913
        self,
        base_directory,
        filepath_template=None,
        filepath_prefix=None,
        filepath_suffix=None,
        forbidden_substrings=None,
        platform_specific_separator=True,
        root_directory=None,
        fixed_length_key=False,
        suppress_store_backend_id=False,
        manually_initialize_store_backend_id: str = "",
        base_public_path=None,
        store_name=None,
    ) -> None:
        super().__init__(
            filepath_template=filepath_template,
            filepath_prefix=filepath_prefix,
            filepath_suffix=filepath_suffix,
            forbidden_substrings=forbidden_substrings,
            platform_specific_separator=platform_specific_separator,
            fixed_length_key=fixed_length_key,
            suppress_store_backend_id=suppress_store_backend_id,
            manually_initialize_store_backend_id=manually_initialize_store_backend_id,
            base_public_path=base_public_path,
            store_name=store_name,
        )
        if os.path.isabs(base_directory):  # noqa: PTH117
            self.full_base_directory = base_directory
        else:
            if root_directory is None:  # noqa: PLR5501
                raise ValueError(
                    "base_directory must be an absolute path if root_directory is not provided"
                )
            elif not os.path.isabs(root_directory):  # noqa: PTH117
                raise ValueError(
                    "root_directory must be an absolute path. Got {} instead.".format(
                        root_directory
                    )
                )
            else:
                self.full_base_directory = os.path.join(  # noqa: PTH118
                    root_directory, base_directory
                )

        os.makedirs(  # noqa: PTH103
            str(os.path.dirname(self.full_base_directory)),  # noqa: PTH120
            exist_ok=True,
        )
        # Initialize with store_backend_id if not part of an HTMLSiteStore
        if not self._suppress_store_backend_id:
            _ = self.store_backend_id

        # Gather the call arguments of the present function (include the "module_name" and add the "class_name"), filter
        # out the Falsy values, and set the instance "_config" variable equal to the resulting dictionary.
        self._config = {
            "base_directory": base_directory,
            "filepath_template": filepath_template,
            "filepath_prefix": filepath_prefix,
            "filepath_suffix": filepath_suffix,
            "forbidden_substrings": forbidden_substrings,
            "platform_specific_separator": platform_specific_separator,
            "root_directory": root_directory,
            "fixed_length_key": fixed_length_key,
            "suppress_store_backend_id": suppress_store_backend_id,
            "manually_initialize_store_backend_id": manually_initialize_store_backend_id,
            "base_public_path": base_public_path,
            "store_name": store_name,
            "module_name": self.__class__.__module__,
            "class_name": self.__class__.__name__,
        }
        filter_properties_dict(properties=self._config, clean_falsy=True, inplace=True)

    def _get(self, key):
        filepath: str = os.path.join(  # noqa: PTH118
            self.full_base_directory, self._convert_key_to_filepath(key)
        )
        try:
            with open(filepath) as infile:
                contents: str = infile.read().rstrip("\n")
        except FileNotFoundError:
            raise InvalidKeyError(
                f"Unable to retrieve object from TupleFilesystemStoreBackend with the following Key: {str(filepath)}"
            )

        return contents

    def _set(self, key, value, **kwargs):
        if not isinstance(key, tuple):
            key = key.to_tuple()
        filepath = os.path.join(  # noqa: PTH118
            self.full_base_directory, self._convert_key_to_filepath(key)
        )
        path, filename = os.path.split(filepath)

        os.makedirs(str(path), exist_ok=True)  # noqa: PTH103
        with open(filepath, "wb") as outfile:
            if isinstance(value, str):
                outfile.write(value.encode("utf-8"))
            else:
                outfile.write(value)
        return filepath

    def _move(self, source_key, dest_key, **kwargs):
        source_path = os.path.join(  # noqa: PTH118
            self.full_base_directory, self._convert_key_to_filepath(source_key)
        )

        dest_path = os.path.join(  # noqa: PTH118
            self.full_base_directory, self._convert_key_to_filepath(dest_key)
        )
        dest_dir, dest_filename = os.path.split(dest_path)

        if os.path.exists(source_path):  # noqa: PTH110
            os.makedirs(dest_dir, exist_ok=True)  # noqa: PTH103
            shutil.move(source_path, dest_path)
            return dest_key

        return False

    def list_keys(self, prefix: Tuple = ()) -> List[Tuple]:
        key_list = []
        for root, dirs, files in os.walk(
            os.path.join(self.full_base_directory, *prefix)  # noqa: PTH118
        ):
            for file_ in files:
                full_path, file_name = os.path.split(
                    os.path.join(root, file_)  # noqa: PTH118
                )
                relative_path = os.path.relpath(
                    full_path,
                    self.full_base_directory,
                )
                if relative_path == ".":
                    filepath = file_name
                else:
                    filepath = os.path.join(relative_path, file_name)  # noqa: PTH118

                if self.filepath_prefix and not filepath.startswith(
                    self.filepath_prefix
                ):
                    continue
                elif self.filepath_suffix and not filepath.endswith(
                    self.filepath_suffix
                ):
                    continue
                key = self._convert_filepath_to_key(filepath)
                if key and not self.is_ignored_key(key):
                    key_list.append(key)

        return key_list

    def rrmdir(self, mroot, curpath) -> None:
        """
        recursively removes empty dirs between curpath and mroot inclusive
        """
        try:
            while (
                not os.listdir(curpath)
                and os.path.exists(curpath)  # noqa: PTH110
                and mroot != curpath
            ):
                f2 = os.path.dirname(curpath)  # noqa: PTH120
                os.rmdir(curpath)  # noqa: PTH106
                curpath = f2
        except (NotADirectoryError, FileNotFoundError):
            pass

    def remove_key(self, key):
        if not isinstance(key, tuple):
            key = key.to_tuple()

        filepath = os.path.join(  # noqa: PTH118
            self.full_base_directory, self._convert_key_to_filepath(key)
        )

        if os.path.exists(filepath):  # noqa: PTH110
            d_path = os.path.dirname(filepath)  # noqa: PTH120
            os.remove(filepath)  # noqa: PTH107
            self.rrmdir(self.full_base_directory, d_path)
            return True
        return False

    def get_url_for_key(self, key, protocol=None):
        path = self._convert_key_to_filepath(key)
        escaped_path = self._url_path_escape_special_characters(path=path)
        full_path = os.path.join(self.full_base_directory, escaped_path)  # noqa: PTH118

        if protocol is None:
            protocol = "file:"
        url = f"{protocol}//{full_path}"
        return url

    def get_public_url_for_key(self, key, protocol=None):
        if not self.base_public_path:
            raise StoreBackendError(
                """Error: No base_public_path was configured!
                    - A public URL was requested base_public_path was not configured for the TupleFilesystemStoreBackend
                """
            )
        path = self._convert_key_to_filepath(key)
        public_url = self.base_public_path + path
        return public_url

    def _has_key(self, key):
        return os.path.isfile(  # noqa: PTH113
            os.path.join(  # noqa: PTH118
                self.full_base_directory, self._convert_key_to_filepath(key)
            )
        )

    @property
    def config(self) -> dict:
        return self._config


class TupleS3StoreBackend(TupleStoreBackend):
    """
    Uses an S3 bucket as a store.

    The key to this StoreBackend must be a tuple with fixed length based on the filepath_template,
    or a variable-length tuple may be used and returned with an optional filepath_suffix (to be) added.
    The filepath_template is a string template used to convert the key to a filepath.
    """

    def __init__(  # noqa: PLR0913
        self,
        bucket,
        prefix="",
        boto3_options=None,
        s3_put_options=None,
        filepath_template=None,
        filepath_prefix=None,
        filepath_suffix=None,
        forbidden_substrings=None,
        platform_specific_separator=False,
        fixed_length_key=False,
        suppress_store_backend_id=False,
        manually_initialize_store_backend_id: str = "",
        base_public_path=None,
        endpoint_url=None,
        store_name=None,
    ) -> None:
        super().__init__(
            filepath_template=filepath_template,
            filepath_prefix=filepath_prefix,
            filepath_suffix=filepath_suffix,
            forbidden_substrings=forbidden_substrings,
            platform_specific_separator=platform_specific_separator,
            fixed_length_key=fixed_length_key,
            suppress_store_backend_id=suppress_store_backend_id,
            manually_initialize_store_backend_id=manually_initialize_store_backend_id,
            base_public_path=base_public_path,
            store_name=store_name,
        )
        self.bucket = bucket
        if prefix:
            if self.platform_specific_separator:
                prefix = prefix.strip(os.sep)

            # we *always* strip "/" from the prefix based on the norms of s3
            # whether the rest of the key is built with platform-specific separators or not
            prefix = prefix.strip("/")
        self.prefix = prefix
        if boto3_options is None:
            boto3_options = {}
        self._boto3_options = boto3_options
        if s3_put_options is None:
            s3_put_options = {}
        self.s3_put_options = s3_put_options
        self.endpoint_url = endpoint_url
        # Initialize with store_backend_id if not part of an HTMLSiteStore
        if not self._suppress_store_backend_id:
            _ = self.store_backend_id

        # Gather the call arguments of the present function (include the "module_name" and add the "class_name"), filter
        # out the Falsy values, and set the instance "_config" variable equal to the resulting dictionary.
        self._config = {
            "bucket": bucket,
            "prefix": prefix,
            "boto3_options": boto3_options,
            "s3_put_options": s3_put_options,
            "filepath_template": filepath_template,
            "filepath_prefix": filepath_prefix,
            "filepath_suffix": filepath_suffix,
            "forbidden_substrings": forbidden_substrings,
            "platform_specific_separator": platform_specific_separator,
            "fixed_length_key": fixed_length_key,
            "suppress_store_backend_id": suppress_store_backend_id,
            "manually_initialize_store_backend_id": manually_initialize_store_backend_id,
            "base_public_path = None": base_public_path,
            "endpoint_url": endpoint_url,
            "store_name": store_name,
            "module_name": self.__class__.__module__,
            "class_name": self.__class__.__name__,
        }
        filter_properties_dict(properties=self._config, clean_falsy=True, inplace=True)

    def _build_s3_object_key(self, key):
        if self.platform_specific_separator:
            if self.prefix:
                s3_object_key = os.path.join(  # noqa: PTH118
                    self.prefix, self._convert_key_to_filepath(key)
                )
            else:
                s3_object_key = self._convert_key_to_filepath(key)
        else:
            if self.prefix:  # noqa: PLR5501
                s3_object_key = "/".join(
                    (self.prefix, self._convert_key_to_filepath(key))
                )
            else:
                s3_object_key = self._convert_key_to_filepath(key)
        return s3_object_key

    def _get(self, key):
        s3_object_key = self._build_s3_object_key(key)

        s3 = self._create_client()

        try:
            s3_response_object = s3.get_object(Bucket=self.bucket, Key=s3_object_key)
        except (s3.exceptions.NoSuchKey, s3.exceptions.NoSuchBucket):
            raise InvalidKeyError(
                f"Unable to retrieve object from TupleS3StoreBackend with the following Key: {str(s3_object_key)}"
            )

        return (
            s3_response_object["Body"]
            .read()
            .decode(s3_response_object.get("ContentEncoding", "utf-8"))
        )

    def _set(
        self,
        key,
        value,
        content_encoding="utf-8",
        content_type="application/json",
        **kwargs,
    ):
        s3_object_key = self._build_s3_object_key(key)

        s3 = self._create_resource()

        try:
            result_s3 = s3.Object(self.bucket, s3_object_key)
            if isinstance(value, str):
                result_s3.put(
                    Body=value.encode(content_encoding),
                    ContentEncoding=content_encoding,
                    ContentType=content_type,
                    **self.s3_put_options,
                )
            else:
                result_s3.put(
                    Body=value, ContentType=content_type, **self.s3_put_options
                )
        except s3.meta.client.exceptions.ClientError as e:
            logger.debug(str(e))
            raise StoreBackendError("Unable to set object in s3.")

        return s3_object_key

    def _move(self, source_key, dest_key, **kwargs) -> None:
        s3 = self._create_resource()

        source_filepath = self._convert_key_to_filepath(source_key)
        if not source_filepath.startswith(self.prefix):
            source_filepath = os.path.join(self.prefix, source_filepath)  # noqa: PTH118
        dest_filepath = self._convert_key_to_filepath(dest_key)
        if not dest_filepath.startswith(self.prefix):
            dest_filepath = os.path.join(self.prefix, dest_filepath)  # noqa: PTH118

        s3.Bucket(self.bucket).copy(
            {"Bucket": self.bucket, "Key": source_filepath}, dest_filepath
        )

        s3.Object(self.bucket, source_filepath).delete()

    def list_keys(self, prefix: Tuple = ()) -> List[Tuple]:
        # Note that the prefix arg is only included to maintain consistency with the parent class signature
        s3r = self._create_resource()
        bucket = s3r.Bucket(self.bucket)
        key_list = []
        if self.prefix:
            objects_list = bucket.objects.filter(Prefix=self.prefix)
        else:
            objects_list = bucket.objects.all()
        for s3_object_info in objects_list:
            s3_object_key = s3_object_info.key
            if self.platform_specific_separator:
                s3_object_key = os.path.relpath(s3_object_key, self.prefix)
            else:
                if self.prefix is None:  # noqa: PLR5501
                    if s3_object_key.startswith("/"):
                        s3_object_key = s3_object_key[1:]
                else:
                    if s3_object_key.startswith(f"{self.prefix}/"):  # noqa: PLR5501
                        s3_object_key = s3_object_key[len(self.prefix) + 1 :]
            if self.filepath_prefix and not s3_object_key.startswith(
                self.filepath_prefix
            ):
                continue
            elif self.filepath_suffix and not s3_object_key.endswith(
                self.filepath_suffix
            ):
                continue
            key = self._convert_filepath_to_key(s3_object_key)
            if key:
                key_list.append(key)

        return key_list

    def get_url_for_key(self, key, protocol=None):
        location = None
        if self.boto3_options.get("endpoint_url"):
            location = self.boto3_options.get("endpoint_url")
        else:
            # build s3 endpoint when no endpoint_url is configured

            location = self._create_client().get_bucket_location(Bucket=self.bucket)[
                "LocationConstraint"
            ]

            if location is None:
                location = "https://s3.amazonaws.com"
            else:
                location = f"https://s3-{location}.amazonaws.com"

        s3_key = self._convert_key_to_filepath(key)

        if not self.prefix:
            return f"{location}/{self.bucket}/{s3_key}"
        return f"{location}/{self.bucket}/{self.prefix}/{s3_key}"

    def get_public_url_for_key(self, key, protocol=None):
        if not self.base_public_path:
            raise StoreBackendError(
                """Error: No base_public_path was configured!
                    - A public URL was requested base_public_path was not configured for the
                """
            )
        s3_key = self._convert_key_to_filepath(key)
        # <WILL> What happens if there is a prefix?
        if self.base_public_path[-1] != "/":
            public_url = f"{self.base_public_path}/{s3_key}"
        else:
            public_url = self.base_public_path + s3_key
        return public_url

    def remove_key(self, key):
        if not isinstance(key, tuple):
            key = key.to_tuple()

        s3 = self._create_resource()
        s3_object_key = self._build_s3_object_key(key)

        # Check if the object exists
        if self.has_key(key):
            # This implementation deletes the object if non-versioned or adds a delete marker if versioned
            s3.Object(self.bucket, s3_object_key).delete()
            return True
        else:
            return False

    def _has_key(self, key):
        all_keys = self.list_keys()
        return key in all_keys

    def _assume_role_and_get_secret_credentials(self):
        role_session_name = "GXAssumeRoleSession"
        client = aws.boto3.client("sts", self._boto3_options.get("region_name"))
        role_arn = self._boto3_options.pop("assume_role_arn")
        assume_role_duration = self._boto3_options.pop("assume_role_duration")
        response = client.assume_role(
            RoleArn=role_arn,
            RoleSessionName=role_session_name,
            DurationSeconds=assume_role_duration,
        )
        self._boto3_options["aws_access_key_id"] = response["Credentials"][
            "AccessKeyId"
        ]
        self._boto3_options["aws_secret_access_key"] = response["Credentials"][
            "SecretAccessKey"
        ]
        self._boto3_options["aws_session_token"] = response["Credentials"][
            "SessionToken"
        ]

    @property
    def boto3_options(self):
        result = {}
        if self._boto3_options.get("signature_version"):
            signature_version = self._boto3_options.pop("signature_version")
            result["config"] = aws.Config(signature_version=signature_version)
        if self._boto3_options.get("assume_role_arn"):
            self._assume_role_and_get_secret_credentials()
        result.update(self._boto3_options)

        return result

    def _create_client(self):
        return aws.boto3.client("s3", **self.boto3_options)

    def _create_resource(self):
        return aws.boto3.resource("s3", **self.boto3_options)

    @property
    def config(self) -> dict:
        return self._config


class TupleGCSStoreBackend(TupleStoreBackend):
    """
    Uses a GCS bucket as a store.

    The key to this StoreBackend must be a tuple with fixed length based on the filepath_template,
    or a variable-length tuple may be used and returned with an optional filepath_suffix (to be) added.

    The filepath_template is a string template used to convert the key to a filepath.
    """

    def __init__(  # noqa: PLR0913
        self,
        bucket,
        project,
        prefix="",
        filepath_template=None,
        filepath_prefix=None,
        filepath_suffix=None,
        forbidden_substrings=None,
        platform_specific_separator=False,
        fixed_length_key=False,
        suppress_store_backend_id=False,
        manually_initialize_store_backend_id: str = "",
        public_urls=True,
        base_public_path=None,
        store_name=None,
    ) -> None:
        super().__init__(
            filepath_template=filepath_template,
            filepath_prefix=filepath_prefix,
            filepath_suffix=filepath_suffix,
            forbidden_substrings=forbidden_substrings,
            platform_specific_separator=platform_specific_separator,
            fixed_length_key=fixed_length_key,
            suppress_store_backend_id=suppress_store_backend_id,
            manually_initialize_store_backend_id=manually_initialize_store_backend_id,
            base_public_path=base_public_path,
            store_name=store_name,
        )
        self.bucket = bucket
        self.prefix = prefix
        self.project = project
        self._public_urls = public_urls
        # Initialize with store_backend_id if not part of an HTMLSiteStore
        if not self._suppress_store_backend_id:
            _ = self.store_backend_id

        # Gather the call arguments of the present function (include the "module_name" and add the "class_name"), filter
        # out the Falsy values, and set the instance "_config" variable equal to the resulting dictionary.
        self._config = {
            "bucket": bucket,
            "project": project,
            "prefix": prefix,
            "filepath_template": filepath_template,
            "filepath_prefix": filepath_prefix,
            "filepath_suffix": filepath_suffix,
            "forbidden_substrings": forbidden_substrings,
            "platform_specific_separator": platform_specific_separator,
            "fixed_length_key": fixed_length_key,
            "suppress_store_backend_id": suppress_store_backend_id,
            "manually_initialize_store_backend_id": manually_initialize_store_backend_id,
            "public_urls": public_urls,
            "base_public_path": base_public_path,
            "store_name": store_name,
            "module_name": self.__class__.__module__,
            "class_name": self.__class__.__name__,
        }
        filter_properties_dict(properties=self._config, clean_falsy=True, inplace=True)

    def _build_gcs_object_key(self, key):
        if self.platform_specific_separator:
            if self.prefix:
                gcs_object_key = os.path.join(  # noqa: PTH118
                    self.prefix, self._convert_key_to_filepath(key)
                )
            else:
                gcs_object_key = self._convert_key_to_filepath(key)
        else:
            if self.prefix:  # noqa: PLR5501
                gcs_object_key = "/".join(
                    (self.prefix, self._convert_key_to_filepath(key))
                )
            else:
                gcs_object_key = self._convert_key_to_filepath(key)
        return gcs_object_key

    def _get(self, key):
        gcs_object_key = self._build_gcs_object_key(key)

        from great_expectations.compatibility import google

        gcs = google.storage.Client(project=self.project)
        bucket = gcs.bucket(self.bucket)
        gcs_response_object = bucket.get_blob(gcs_object_key)
        if not gcs_response_object:
            raise InvalidKeyError(
                f"Unable to retrieve object from TupleGCSStoreBackend with the following Key: {str(key)}"
            )
        else:
            return gcs_response_object.download_as_bytes().decode("utf-8")

    def _set(
        self,
        key,
        value,
        content_encoding="utf-8",
        content_type="application/json",
        **kwargs,
    ):
        gcs_object_key = self._build_gcs_object_key(key)

        from great_expectations.compatibility import google

        gcs = google.storage.Client(project=self.project)
        bucket = gcs.bucket(self.bucket)
        blob = bucket.blob(gcs_object_key)

        if isinstance(value, str):
            blob.content_encoding = content_encoding
            blob.upload_from_string(
                value.encode(content_encoding), content_type=content_type
            )
        else:
            blob.upload_from_string(value, content_type=content_type)
        return gcs_object_key

    def _move(self, source_key, dest_key, **kwargs) -> None:
        from great_expectations.compatibility import google

        gcs = google.storage.Client(project=self.project)
        bucket = gcs.bucket(self.bucket)

        source_filepath = self._convert_key_to_filepath(source_key)
        if not source_filepath.startswith(self.prefix):
            source_filepath = os.path.join(self.prefix, source_filepath)  # noqa: PTH118
        dest_filepath = self._convert_key_to_filepath(dest_key)
        if not dest_filepath.startswith(self.prefix):
            dest_filepath = os.path.join(self.prefix, dest_filepath)  # noqa: PTH118

        blob = bucket.blob(source_filepath)
        _ = bucket.rename_blob(blob, dest_filepath)

    def list_keys(self, prefix: Tuple = ()) -> List[Tuple]:
        # Note that the prefix arg is only included to maintain consistency with the parent class signature
        key_list = []

        from great_expectations.compatibility import google

        gcs = google.storage.Client(self.project)

        for blob in gcs.list_blobs(self.bucket, prefix=self.prefix):
            gcs_object_name = blob.name
            gcs_object_key = os.path.relpath(
                gcs_object_name,
                self.prefix,
            )
            if self.filepath_prefix and not gcs_object_key.startswith(
                self.filepath_prefix
            ):
                continue
            elif self.filepath_suffix and not gcs_object_key.endswith(
                self.filepath_suffix
            ):
                continue
            key = self._convert_filepath_to_key(gcs_object_key)
            if key:
                key_list.append(key)
        return key_list

    def get_url_for_key(self, key, protocol=None):
        path = self._convert_key_to_filepath(key)

        if self._public_urls:
            base_url = "https://storage.googleapis.com/"
        else:
            base_url = "https://storage.cloud.google.com/"

        path_url = self._get_path_url(path)

        return base_url + path_url

    def get_public_url_for_key(self, key, protocol=None):
        if not self.base_public_path:
            raise StoreBackendError(
                """Error: No base_public_path was configured!
                    - A public URL was requested base_public_path was not configured for the
                """
            )
        path = self._convert_key_to_filepath(key)
        path_url = self._get_path_url(path)
        public_url = self.base_public_path + path_url
        return public_url

    def _get_path_url(self, path):
        if self.prefix:
            path_url = "/".join((self.bucket, self.prefix, path))
        else:
            if self.base_public_path:  # noqa: PLR5501
                if self.base_public_path[-1] != "/":
                    path_url = f"/{path}"
                else:
                    path_url = path
            else:
                path_url = "/".join((self.bucket, path))
        return path_url

    def remove_key(self, key):
        from great_expectations.compatibility import google

        gcs = google.storage.Client(project=self.project)
        bucket = gcs.bucket(self.bucket)
        try:
            bucket.delete_blobs(blobs=list(bucket.list_blobs(prefix=self.prefix)))
        except google.NotFound:
            return False
        return True

    def _has_key(self, key):
        all_keys = self.list_keys()
        return key in all_keys


class TupleAzureBlobStoreBackend(TupleStoreBackend):
    """
    Uses an Azure Blob as a store.

    The key to this StoreBackend must be a tuple with fixed length based on the filepath_template,
    or a variable-length tuple may be used and returned with an optional filepath_suffix (to be) added.
    The filepath_template is a string template used to convert the key to a filepath.

    You need to setup the connection string environment variable
    https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python
    """

    # We will use blobclient here
    def __init__(  # noqa: PLR0913
        self,
        container,
        connection_string=None,
        account_url=None,
        prefix=None,
        filepath_template=None,
        filepath_prefix=None,
        filepath_suffix=None,
        forbidden_substrings=None,
        platform_specific_separator=False,
        fixed_length_key=False,
        suppress_store_backend_id=False,
        manually_initialize_store_backend_id: str = "",
        store_name=None,
    ) -> None:
        super().__init__(
            filepath_template=filepath_template,
            filepath_prefix=filepath_prefix,
            filepath_suffix=filepath_suffix,
            forbidden_substrings=forbidden_substrings,
            platform_specific_separator=platform_specific_separator,
            fixed_length_key=fixed_length_key,
            suppress_store_backend_id=suppress_store_backend_id,
            manually_initialize_store_backend_id=manually_initialize_store_backend_id,
            store_name=store_name,
        )
        self.connection_string = connection_string or os.environ.get(
            "AZURE_STORAGE_CONNECTION_STRING"
        )
        self.prefix = prefix or ""
        self.container = container
        self.account_url = account_url or os.environ.get("AZURE_STORAGE_ACCOUNT_URL")

    @property
    @functools.lru_cache  # noqa: B019 # lru_cache on method
    def _container_client(self) -> Any:
        from great_expectations.compatibility import azure

        # Validate that "azure" libararies were successfully imported and attempt to create "azure_client" handle.
        if azure.BlobServiceClient:  # type: ignore[truthy-function] # False if NotImported
            try:
                if self.connection_string:
                    blob_service_client: azure.BlobServiceClient = (
                        azure.BlobServiceClient.from_connection_string(
                            self.connection_string
                        )
                    )
                elif self.account_url:
                    blob_service_client = azure.BlobServiceClient(
                        account_url=self.account_url,
                        credential=azure.DefaultAzureCredential(),
                    )
                else:
                    raise StoreBackendError(
                        "Unable to initialize ServiceClient, AZURE_STORAGE_CONNECTION_STRING should be set"
                    )
            except Exception as e:
                # Failure to create "azure_client" is most likely due invalid "azure_options" dictionary.
                raise StoreBackendError(
                    f'Due to exception: "{str(e)}", "azure_client" could not be created.'
                ) from e
        else:
            raise StoreBackendError(
                'Unable to create azure "BlobServiceClient" due to missing azure.storage.blob dependency.'
            )

        return blob_service_client.get_container_client(self.container)

    def _get(self, key):
        az_blob_key = os.path.join(  # noqa: PTH118
            self.prefix, self._convert_key_to_filepath(key)
        )
        return (
            self._container_client.download_blob(az_blob_key).readall().decode("utf-8")
        )

    def _set(self, key, value, content_encoding="utf-8", **kwargs):
        from great_expectations.compatibility.azure import ContentSettings

        az_blob_key = os.path.join(  # noqa: PTH118
            self.prefix, self._convert_key_to_filepath(key)
        )

        if isinstance(value, str):
            if az_blob_key.endswith(".html"):
                my_content_settings = ContentSettings(content_type="text/html")
                self._container_client.upload_blob(
                    name=az_blob_key,
                    data=value,
                    encoding=content_encoding,
                    overwrite=True,
                    content_settings=my_content_settings,
                )
            else:
                self._container_client.upload_blob(
                    name=az_blob_key,
                    data=value,
                    encoding=content_encoding,
                    overwrite=True,
                )
        else:
            self._container_client.upload_blob(
                name=az_blob_key, data=value, overwrite=True
            )
        return az_blob_key

    def list_keys(self, prefix: Tuple = ()) -> List[Tuple]:
        # Note that the prefix arg is only included to maintain consistency with the parent class signature
        key_list = []

        for obj in self._container_client.list_blobs(name_starts_with=self.prefix):  # type: ignore[attr-defined]
            az_blob_key = os.path.relpath(obj.name)
            if az_blob_key.startswith(f"{self.prefix}{os.path.sep}"):
                az_blob_key = az_blob_key[len(self.prefix) + 1 :]
            if self.filepath_prefix and not az_blob_key.startswith(
                self.filepath_prefix
            ):
                continue
            elif self.filepath_suffix and not az_blob_key.endswith(
                self.filepath_suffix
            ):
                continue
            key = self._convert_filepath_to_key(az_blob_key)

            key_list.append(key)
        return key_list

    def get_url_for_key(self, key, protocol=None):
        az_blob_key = self._convert_key_to_filepath(key)
        az_blob_path = os.path.join(  # noqa: PTH118
            self.container, self.prefix, az_blob_key
        )

        return "https://{}.blob.core.windows.net/{}".format(
            self._container_client.account_name,
            az_blob_path,
        )

    def _has_key(self, key):
        all_keys = self.list_keys()
        return key in all_keys

    def _move(self, source_key, dest_key, **kwargs) -> None:
        source_blob_path = self._convert_key_to_filepath(source_key)
        if not source_blob_path.startswith(self.prefix):
            source_blob_path = os.path.join(  # noqa: PTH118
                self.prefix, source_blob_path
            )
        dest_blob_path = self._convert_key_to_filepath(dest_key)
        if not dest_blob_path.startswith(self.prefix):
            dest_blob_path = os.path.join(self.prefix, dest_blob_path)  # noqa: PTH118

        # azure storage sdk does not have _move method
        source_blob = self._container_client.get_blob_client(source_blob_path)  # type: ignore[attr-defined]
        dest_blob = self._container_client.get_blob_client(dest_blob_path)  # type: ignore[attr-defined]

        dest_blob.start_copy_from_url(source_blob.url, requires_sync=True)
        copy_properties = dest_blob.get_blob_properties().copy

        if copy_properties.status != "success":
            dest_blob.abort_copy(copy_properties.id)
            raise StoreBackendError(
                f"Unable to copy blob {source_blob_path} with status {copy_properties.status}"
            )
        source_blob.delete_blob()

    def remove_key(self, key):
        if not isinstance(key, tuple):
            key = key.to_tuple()

        az_blob_path = self._convert_key_to_filepath(key)
        if not az_blob_path.startswith(self.prefix):
            az_blob_path = os.path.join(self.prefix, az_blob_path)  # noqa: PTH118

        blob = self._container_client.get_blob_client(az_blob_path)
        blob.delete_blob()
        return True

    @property
    def config(self) -> dict:
        return self._config  # type: ignore[attr-defined]
