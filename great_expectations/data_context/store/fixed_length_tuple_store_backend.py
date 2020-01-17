import os
import random
import re
# PYTHON 2 - py2 - update to ABC direct use rather than __metaclass__ once we drop py2 support
from abc import ABCMeta

from six import string_types

from great_expectations.data_context.store.store_backend import StoreBackend
from great_expectations.data_context.util import safe_mmkdir


class FixedLengthTupleStoreBackend(StoreBackend):
    __metaclass__ = ABCMeta
    """
    If key_length is provided, the key to this StoreBackend abstract class must be a tuple with fixed length equal
    to key_length. The filepath_template is a string template used to convert the key to a filepath.
    There's a bit of regex magic in _convert_filepath_to_key that reverses this process,
    so that we can write AND read using filenames as keys.

    Another class should get this logic through multiple inheritance.
    """

    def __init__(
        self,
        filepath_template,
        key_length,
        forbidden_substrings=None,
        platform_specific_separator=True
    ):
        assert isinstance(key_length, int) or key_length is None
        self.key_length = key_length
        if forbidden_substrings is None:
            forbidden_substrings = ["/", "\\"]
        self.forbidden_substrings = forbidden_substrings
        self.platform_specific_separator = platform_specific_separator

        self.filepath_template = filepath_template
        if key_length:
            self.verify_that_key_to_filepath_operation_is_reversible()

    def _validate_key(self, key):
        super(FixedLengthTupleStoreBackend, self)._validate_key(key)

        for key_element in key:
            for substring in self.forbidden_substrings:
                if substring in key_element:
                    raise ValueError("Keys in {0} must not contain substrings in {1} : {2}".format(
                        self.__class__.__name__,
                        self.forbidden_substrings,
                        key,
                    ))

    def _validate_value(self, value):
        if not isinstance(value, string_types) and not isinstance(value, bytes):
            raise TypeError("Values in {0} must be instances of {1} or {2}, not {3}".format(
                self.__class__.__name__,
                string_types,
                bytes,
                type(value),
            ))

    def _convert_key_to_filepath(self, key):
        self._validate_key(key)
        if self.filepath_template:
            converted_string = self.filepath_template.format(*list(key))
        else:
            converted_string = '/'.join(key)
        if self.platform_specific_separator:
            converted_string = os.path.join(*converted_string.split('/'))
        return converted_string

    def _convert_filepath_to_key(self, filepath):
        # filepath_template (for now) is always specified with forward slashes, but it is then
        # used to (1) dynamically construct and evaluate a regex, and (2) split the provided (observed) filepath
        if not self.filepath_template:
            return tuple(filepath.split(os.sep))

        if self.platform_specific_separator:
            filepath_template = os.path.join(*self.filepath_template.split('/'))
            filepath_template = filepath_template.replace('\\', '\\\\')
        else:
            filepath_template = self.filepath_template

        # Convert the template to a regex
        indexed_string_substitutions = re.findall(r"{\d+}", filepath_template)
        tuple_index_list = ["(?P<tuple_index_{0}>.*)".format(i, ) for i in range(len(indexed_string_substitutions))]
        intermediate_filepath_regex = re.sub(
            r"{\d+}",
            lambda m, r=iter(tuple_index_list): next(r),
            filepath_template
        )
        filepath_regex = intermediate_filepath_regex.format(*tuple_index_list)

        # Apply the regex to the filepath
        matches = re.compile(filepath_regex).match(filepath)
        if matches is None:
            return None

        # Map key elements into the appropriate parts of the tuple
        new_key = list([None for _ in range(self.key_length)])
        for i in range(len(tuple_index_list)):
            tuple_index = int(re.search(r'\d+', indexed_string_substitutions[i]).group(0))
            key_element = matches.group('tuple_index_' + str(i))
            new_key[tuple_index] = key_element

        new_key = tuple(new_key)
        return new_key

    def verify_that_key_to_filepath_operation_is_reversible(self):
        def get_random_hex(size=4):
            return "".join([random.choice(list("ABCDEF0123456789")) for _ in range(size)])

        key = tuple([get_random_hex() for _ in range(self.key_length)])
        filepath = self._convert_key_to_filepath(key)
        new_key = self._convert_filepath_to_key(filepath)
        if key != new_key:
            raise ValueError(
                "filepath template {0} for class {1} is not reversible for a tuple of length {2}. "
                "Have you included all elements in the key tuple?".format(
                    self.filepath_template,
                    self.__class__.__name__,
                    self.key_length,
                ))


class FixedLengthTupleFilesystemStoreBackend(FixedLengthTupleStoreBackend):
    """Uses a local filepath as a store.

    The key to this StoreBackend must be a tuple with fixed length equal to key_length.
    The filepath_template is a string template used to convert the key to a filepath.
    There's a bit of regex magic in _convert_filepath_to_key that reverses this process,
    so that we can write AND read using filenames as keys.
    """

    def __init__(self,
                 base_directory,
                 filepath_template,
                 key_length,
                 forbidden_substrings=None,
                 platform_specific_separator=True,
                 root_directory=None):
        super(FixedLengthTupleFilesystemStoreBackend, self).__init__(
            filepath_template=filepath_template,
            key_length=key_length,
            forbidden_substrings=forbidden_substrings,
            platform_specific_separator=platform_specific_separator
        )
        if os.path.isabs(base_directory):
            self.full_base_directory = base_directory
        else:
            if root_directory is None:
                raise ValueError("base_directory must be an absolute path if root_directory is not provided")
            elif not os.path.isabs(root_directory):
                raise ValueError("root_directory must be an absolute path. Got {0} instead.".format(root_directory))
            else:
                self.full_base_directory = os.path.join(root_directory, base_directory)
        safe_mmkdir(str(os.path.dirname(self.full_base_directory)))

    def _get(self, key):
        filepath = os.path.join(
            self.full_base_directory,
            self._convert_key_to_filepath(key)
        )
        with open(filepath, 'r') as infile:
            return infile.read()

    def _set(self, key, value, **kwargs):
        filepath = os.path.join(
            self.full_base_directory,
            self._convert_key_to_filepath(key)
        )
        path, filename = os.path.split(filepath)

        safe_mmkdir(str(path))
        with open(filepath, "wb") as outfile:
            if isinstance(value, string_types):
                # Following try/except is to support py2, since both str and bytes objects pass above condition
                try:
                    outfile.write(value.encode("utf-8"))
                except TypeError:
                    outfile.write(value)
            else:
                outfile.write(value)
        return filepath

    def list_keys(self, prefix=()):
        key_list = []
        for root, dirs, files in os.walk(os.path.join(self.full_base_directory, *prefix)):
            for file_ in files:
                full_path, file_name = os.path.split(os.path.join(root, file_))
                relative_path = os.path.relpath(
                    full_path,
                    self.full_base_directory,
                )
                if relative_path == ".":
                    filepath = file_name
                else:
                    filepath = os.path.join(
                        relative_path,
                        file_name
                    )

                key = self._convert_filepath_to_key(filepath)
                if key:
                    key_list.append(key)

        return key_list

    def get_url_for_key(self, key, protocol=None):
        path = self._convert_key_to_filepath(key)
        full_path = os.path.join(self.full_base_directory, path)
        if protocol is None:
            protocol = "file:"
        url = protocol + "//" + full_path

        return url

    def _has_key(self, key):
        return os.path.isfile(os.path.join(self.full_base_directory, self._convert_key_to_filepath(key)))


class FixedLengthTupleS3StoreBackend(FixedLengthTupleStoreBackend):
    """
    Uses an S3 bucket as a store.

    The key to this StoreBackend must be a tuple with fixed length equal to key_length.
    The filepath_template is a string template used to convert the key to a filepath.
    There's a bit of regex magic in _convert_filepath_to_key that reverses this process,
    so that we can write AND read using filenames as keys.
    """
    def __init__(
        self,
        filepath_template,
        key_length,
        bucket,
        prefix="",
        forbidden_substrings=None,
        platform_specific_separator=False
    ):
        super(FixedLengthTupleS3StoreBackend, self).__init__(
            filepath_template=filepath_template,
            key_length=key_length,
            forbidden_substrings=forbidden_substrings,
            platform_specific_separator=platform_specific_separator
        )
        self.bucket = bucket
        self.prefix = prefix

    def _get(self, key):
        s3_object_key = os.path.join(
            self.prefix,
            self._convert_key_to_filepath(key)
        )

        import boto3
        s3 = boto3.client('s3')
        s3_response_object = s3.get_object(Bucket=self.bucket, Key=s3_object_key)
        return s3_response_object['Body'].read().decode(s3_response_object.get("ContentEncoding", 'utf-8'))

    def _set(self, key, value, content_encoding='utf-8', content_type='application/json'):
        s3_object_key = os.path.join(
            self.prefix,
            self._convert_key_to_filepath(key)
        )

        import boto3
        s3 = boto3.resource('s3')
        result_s3 = s3.Object(self.bucket, s3_object_key)
        if isinstance(value, string_types):
            # Following try/except is to support py2, since both str and bytes objects pass above condition
            try:
                result_s3.put(Body=value.encode(content_encoding), ContentEncoding=content_encoding,
                              ContentType=content_type)
            except TypeError:
                result_s3.put(Body=value, ContentType=content_type)
        else:
            result_s3.put(Body=value, ContentType=content_type)
        return s3_object_key

    def list_keys(self):
        key_list = []

        import boto3
        s3 = boto3.client('s3')

        for s3_object_info in s3.list_objects(Bucket=self.bucket, Prefix=self.prefix)['Contents']:
            s3_object_key = s3_object_info['Key']
            s3_object_key = os.path.relpath(
                s3_object_key,
                self.prefix,
            )

            key = self._convert_filepath_to_key(s3_object_key)
            if key:
                key_list.append(key)

        return key_list

    def get_url_for_key(self, key, protocol=None):
        import boto3

        location = boto3.client('s3').get_bucket_location(Bucket=self.bucket)['LocationConstraint']
        s3_key = self._convert_key_to_filepath(key)
        return "https://s3-%s.amazonaws.com/%s/%s" % (location, self.bucket, s3_key)

    def _has_key(self, key):
        all_keys = self.list_keys()
        return key in all_keys


class FixedLengthTupleGCSStoreBackend(FixedLengthTupleStoreBackend):
    """
    Uses a GCS bucket as a store.

    The key to this StoreBackend must be a tuple with fixed length equal to key_length.
    The filepath_template is a string template used to convert the key to a filepath.
    There's a bit of regex magic in _convert_filepath_to_key that reverses this process,
    so that we can write AND read using filenames as keys.
    """
    def __init__(
        self,
        filepath_template,
        key_length,
        bucket,
        prefix,
        project,
        forbidden_substrings=None,
        platform_specific_separator=False
    ):
        super(FixedLengthTupleGCSStoreBackend, self).__init__(
            filepath_template=filepath_template,
            key_length=key_length,
            forbidden_substrings=forbidden_substrings,
            platform_specific_separator=platform_specific_separator
        )
        self.bucket = bucket
        self.prefix = prefix
        self.project = project

    def _get(self, key):
        gcs_object_key = os.path.join(
            self.prefix,
            self._convert_key_to_filepath(key)
        )

        from google.cloud import storage
        gcs = storage.Client(project=self.project)
        bucket = gcs.get_bucket(self.bucket)
        gcs_response_object = bucket.get_blob(gcs_object_key)
        return gcs_response_object.download_as_string().decode("utf-8")

    def _set(self, key, value, content_encoding='utf-8', content_type='application/json'):
        gcs_object_key = os.path.join(
            self.prefix,
            self._convert_key_to_filepath(key)
        )

        from google.cloud import storage
        gcs = storage.Client(project=self.project)
        bucket = gcs.get_bucket(self.bucket)
        blob = bucket.blob(gcs_object_key)
        if isinstance(value, string_types):
            # Following try/except is to support py2, since both str and bytes objects pass above condition
            try:
                blob.upload_from_string(value.encode(content_encoding), content_encoding=content_encoding,
                                        content_type=content_type)
            except TypeError:
                blob.upload_from_string(value, content_type=content_type)
        else:
            blob.upload_from_string(value, content_type=content_type)
        return gcs_object_key

    def list_keys(self):
        key_list = []

        from google.cloud import storage
        gcs = storage.Client(self.project)

        for blob in gcs.list_blobs(self.bucket, prefix=self.prefix):
            gcs_object_name = blob.name
            gcs_object_key = os.path.relpath(
                gcs_object_name,
                self.prefix,
            )

            key = self._convert_filepath_to_key(gcs_object_key)
            if key:
                key_list.append(key)

        return key_list

    def _has_key(self, key):
        all_keys = self.list_keys()
        return key in all_keys
