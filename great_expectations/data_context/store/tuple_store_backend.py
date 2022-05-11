
import logging
import os
import random
import re
import shutil
from abc import ABCMeta
from typing import List, Tuple
from great_expectations.data_context.store.store_backend import StoreBackend
from great_expectations.exceptions import InvalidKeyError, StoreBackendError
from great_expectations.util import filter_properties_dict
logger = logging.getLogger(__name__)

class TupleStoreBackend(StoreBackend, metaclass=ABCMeta):
    '\n    If filepath_template is provided, the key to this StoreBackend abstract class must be a tuple with\n    fixed length equal to the number of unique components matching the regex r"{\\d+}"\n\n    For example, in the following template path: expectations/{0}/{1}/{2}/prefix-{2}.json, keys must have\n    three components.\n    '

    def __init__(self, filepath_template=None, filepath_prefix=None, filepath_suffix=None, forbidden_substrings=None, platform_specific_separator=True, fixed_length_key=False, suppress_store_backend_id=False, manually_initialize_store_backend_id: str='', base_public_path=None, store_name=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__(fixed_length_key=fixed_length_key, suppress_store_backend_id=suppress_store_backend_id, manually_initialize_store_backend_id=manually_initialize_store_backend_id, store_name=store_name)
        if (forbidden_substrings is None):
            forbidden_substrings = ['/', '\\']
        self.forbidden_substrings = forbidden_substrings
        self.platform_specific_separator = platform_specific_separator
        if ((filepath_template is not None) and (filepath_suffix is not None)):
            raise ValueError('filepath_suffix may only be used when filepath_template is None')
        self.filepath_template = filepath_template
        if (filepath_prefix and (len(filepath_prefix) > 0)):
            if (filepath_prefix[(- 1)] in self.forbidden_substrings):
                raise StoreBackendError(('Unable to initialize TupleStoreBackend: filepath_prefix may not end with a forbidden substring. Current forbidden substrings are ' + str(forbidden_substrings)))
        self.filepath_prefix = filepath_prefix
        self.filepath_suffix = filepath_suffix
        self.base_public_path = base_public_path
        if (filepath_template is not None):
            self.key_length = len(set(re.findall('{\\d+}', filepath_template)))
            self.verify_that_key_to_filepath_operation_is_reversible()
            self._fixed_length_key = True

    def _validate_key(self, key) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super()._validate_key(key)
        for key_element in key:
            for substring in self.forbidden_substrings:
                if (substring in key_element):
                    raise ValueError('Keys in {} must not contain substrings in {} : {}'.format(self.__class__.__name__, self.forbidden_substrings, key))

    def _validate_value(self, value) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if ((not isinstance(value, str)) and (not isinstance(value, bytes))):
            raise TypeError('Values in {} must be instances of {} or {}, not {}'.format(self.__class__.__name__, str, bytes, type(value)))

    def _convert_key_to_filepath(self, key):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self._validate_key(key)
        if (key == self.STORE_BACKEND_ID_KEY):
            filepath = f"{(self.filepath_prefix or '')}{('/' if self.filepath_prefix else '')}{key[0]}"
            return (filepath if (not self.platform_specific_separator) else os.path.normpath(filepath))
        if self.filepath_template:
            converted_string = self.filepath_template.format(*list(key))
        else:
            converted_string = '/'.join(key)
        if self.filepath_prefix:
            converted_string = f'{self.filepath_prefix}/{converted_string}'
        if self.filepath_suffix:
            converted_string += self.filepath_suffix
        if self.platform_specific_separator:
            converted_string = os.path.normpath(converted_string)
        return converted_string

    def _convert_filepath_to_key(self, filepath):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (filepath == self.STORE_BACKEND_ID_KEY[0]):
            return self.STORE_BACKEND_ID_KEY
        if self.platform_specific_separator:
            filepath = os.path.normpath(filepath)
        if self.filepath_prefix:
            if ((not filepath.startswith(self.filepath_prefix)) and (len(filepath) >= (len(self.filepath_prefix) + 1))):
                raise ValueError('filepath must start with the filepath_prefix when one is set by the store_backend')
            else:
                filepath = filepath[(len(self.filepath_prefix) + 1):]
        if self.filepath_suffix:
            if (not filepath.endswith(self.filepath_suffix)):
                raise ValueError('filepath must end with the filepath_suffix when one is set by the store_backend')
            else:
                filepath = filepath[:(- len(self.filepath_suffix))]
        if self.filepath_template:
            if self.platform_specific_separator:
                filepath_template = os.path.join(*self.filepath_template.split('/'))
                filepath_template = filepath_template.replace('\\', '\\\\')
            else:
                filepath_template = self.filepath_template
            indexed_string_substitutions = re.findall('{\\d+}', filepath_template)
            tuple_index_list = [f'(?P<tuple_index_{i}>.*)' for i in range(len(indexed_string_substitutions))]
            intermediate_filepath_regex = re.sub('{\\d+}', (lambda m, r=iter(tuple_index_list): next(r)), filepath_template)
            filepath_regex = intermediate_filepath_regex.format(*tuple_index_list)
            matches = re.compile(filepath_regex).match(filepath)
            if (matches is None):
                return None
            new_key = ([None] * self.key_length)
            for i in range(len(tuple_index_list)):
                tuple_index = int(re.search('\\d+', indexed_string_substitutions[i]).group(0))
                key_element = matches.group(f'tuple_index_{str(i)}')
                new_key[tuple_index] = key_element
            new_key = tuple(new_key)
        else:
            filepath = os.path.normpath(filepath)
            new_key = tuple(filepath.split(os.sep))
        return new_key

    def verify_that_key_to_filepath_operation_is_reversible(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')

        def get_random_hex(size=4):
            import inspect
            __frame = inspect.currentframe()
            __file = __frame.f_code.co_filename
            __func = __frame.f_code.co_name
            for (k, v) in __frame.f_locals.items():
                if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                    continue
                print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
            return ''.join([random.choice(list('ABCDEF0123456789')) for _ in range(size)])
        key = tuple((get_random_hex() for _ in range(self.key_length)))
        filepath = self._convert_key_to_filepath(key)
        new_key = self._convert_filepath_to_key(filepath)
        if (key != new_key):
            raise ValueError('filepath template {} for class {} is not reversible for a tuple of length {}. Have you included all elements in the key tuple?'.format(self.filepath_template, self.__class__.__name__, self.key_length))

    @property
    def config(self) -> dict:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._config

class TupleFilesystemStoreBackend(TupleStoreBackend):
    'Uses a local filepath as a store.\n\n    The key to this StoreBackend must be a tuple with fixed length based on the filepath_template,\n    or a variable-length tuple may be used and returned with an optional filepath_suffix (to be) added.\n    The filepath_template is a string template used to convert the key to a filepath.\n    '

    def __init__(self, base_directory, filepath_template=None, filepath_prefix=None, filepath_suffix=None, forbidden_substrings=None, platform_specific_separator=True, root_directory=None, fixed_length_key=False, suppress_store_backend_id=False, manually_initialize_store_backend_id: str='', base_public_path=None, store_name=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__(filepath_template=filepath_template, filepath_prefix=filepath_prefix, filepath_suffix=filepath_suffix, forbidden_substrings=forbidden_substrings, platform_specific_separator=platform_specific_separator, fixed_length_key=fixed_length_key, suppress_store_backend_id=suppress_store_backend_id, manually_initialize_store_backend_id=manually_initialize_store_backend_id, base_public_path=base_public_path, store_name=store_name)
        if os.path.isabs(base_directory):
            self.full_base_directory = base_directory
        elif (root_directory is None):
            raise ValueError('base_directory must be an absolute path if root_directory is not provided')
        elif (not os.path.isabs(root_directory)):
            raise ValueError('root_directory must be an absolute path. Got {} instead.'.format(root_directory))
        else:
            self.full_base_directory = os.path.join(root_directory, base_directory)
        os.makedirs(str(os.path.dirname(self.full_base_directory)), exist_ok=True)
        if (not self._suppress_store_backend_id):
            _ = self.store_backend_id
        self._config = {'base_directory': base_directory, 'filepath_template': filepath_template, 'filepath_prefix': filepath_prefix, 'filepath_suffix': filepath_suffix, 'forbidden_substrings': forbidden_substrings, 'platform_specific_separator': platform_specific_separator, 'root_directory': root_directory, 'fixed_length_key': fixed_length_key, 'suppress_store_backend_id': suppress_store_backend_id, 'manually_initialize_store_backend_id': manually_initialize_store_backend_id, 'base_public_path': base_public_path, 'store_name': store_name, 'module_name': self.__class__.__module__, 'class_name': self.__class__.__name__}
        filter_properties_dict(properties=self._config, clean_falsy=True, inplace=True)

    def _get(self, key):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        filepath: str = os.path.join(self.full_base_directory, self._convert_key_to_filepath(key))
        try:
            with open(filepath) as infile:
                contents: str = infile.read().rstrip('\n')
        except FileNotFoundError:
            raise InvalidKeyError(f'Unable to retrieve object from TupleFilesystemStoreBackend with the following Key: {str(filepath)}')
        return contents

    def _set(self, key, value, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (not isinstance(key, tuple)):
            key = key.to_tuple()
        filepath = os.path.join(self.full_base_directory, self._convert_key_to_filepath(key))
        (path, filename) = os.path.split(filepath)
        os.makedirs(str(path), exist_ok=True)
        with open(filepath, 'wb') as outfile:
            if isinstance(value, str):
                outfile.write(value.encode('utf-8'))
            else:
                outfile.write(value)
        return filepath

    def _move(self, source_key, dest_key, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        source_path = os.path.join(self.full_base_directory, self._convert_key_to_filepath(source_key))
        dest_path = os.path.join(self.full_base_directory, self._convert_key_to_filepath(dest_key))
        (dest_dir, dest_filename) = os.path.split(dest_path)
        if os.path.exists(source_path):
            os.makedirs(dest_dir, exist_ok=True)
            shutil.move(source_path, dest_path)
            return dest_key
        return False

    def list_keys(self, prefix: Tuple=()) -> List[Tuple]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        key_list = []
        for (root, dirs, files) in os.walk(os.path.join(self.full_base_directory, *prefix)):
            for file_ in files:
                (full_path, file_name) = os.path.split(os.path.join(root, file_))
                relative_path = os.path.relpath(full_path, self.full_base_directory)
                if (relative_path == '.'):
                    filepath = file_name
                else:
                    filepath = os.path.join(relative_path, file_name)
                if (self.filepath_prefix and (not filepath.startswith(self.filepath_prefix))):
                    continue
                elif (self.filepath_suffix and (not filepath.endswith(self.filepath_suffix))):
                    continue
                key = self._convert_filepath_to_key(filepath)
                if (key and (not self.is_ignored_key(key))):
                    key_list.append(key)
        return key_list

    def rrmdir(self, mroot, curpath) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        recursively removes empty dirs between curpath and mroot inclusive\n        '
        try:
            while ((not os.listdir(curpath)) and os.path.exists(curpath) and (mroot != curpath)):
                f2 = os.path.dirname(curpath)
                os.rmdir(curpath)
                curpath = f2
        except (NotADirectoryError, FileNotFoundError):
            pass

    def remove_key(self, key):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (not isinstance(key, tuple)):
            key = key.to_tuple()
        filepath = os.path.join(self.full_base_directory, self._convert_key_to_filepath(key))
        if os.path.exists(filepath):
            d_path = os.path.dirname(filepath)
            os.remove(filepath)
            self.rrmdir(self.full_base_directory, d_path)
            return True
        return False

    def get_url_for_key(self, key, protocol=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        path = self._convert_key_to_filepath(key)
        full_path = os.path.join(self.full_base_directory, path)
        if (protocol is None):
            protocol = 'file:'
        url = f'{protocol}//{full_path}'
        return url

    def get_public_url_for_key(self, key, protocol=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (not self.base_public_path):
            raise StoreBackendError('Error: No base_public_path was configured!\n                    - A public URL was requested base_public_path was not configured for the TupleFilesystemStoreBackend\n                ')
        path = self._convert_key_to_filepath(key)
        public_url = (self.base_public_path + path)
        return public_url

    def _has_key(self, key):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return os.path.isfile(os.path.join(self.full_base_directory, self._convert_key_to_filepath(key)))

    @property
    def config(self) -> dict:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._config

class TupleS3StoreBackend(TupleStoreBackend):
    '\n    Uses an S3 bucket as a store.\n\n    The key to this StoreBackend must be a tuple with fixed length based on the filepath_template,\n    or a variable-length tuple may be used and returned with an optional filepath_suffix (to be) added.\n    The filepath_template is a string template used to convert the key to a filepath.\n    '

    def __init__(self, bucket, prefix='', boto3_options=None, s3_put_options=None, filepath_template=None, filepath_prefix=None, filepath_suffix=None, forbidden_substrings=None, platform_specific_separator=False, fixed_length_key=False, suppress_store_backend_id=False, manually_initialize_store_backend_id: str='', base_public_path=None, endpoint_url=None, store_name=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__(filepath_template=filepath_template, filepath_prefix=filepath_prefix, filepath_suffix=filepath_suffix, forbidden_substrings=forbidden_substrings, platform_specific_separator=platform_specific_separator, fixed_length_key=fixed_length_key, suppress_store_backend_id=suppress_store_backend_id, manually_initialize_store_backend_id=manually_initialize_store_backend_id, base_public_path=base_public_path, store_name=store_name)
        self.bucket = bucket
        if prefix:
            if self.platform_specific_separator:
                prefix = prefix.strip(os.sep)
            prefix = prefix.strip('/')
        self.prefix = prefix
        if (boto3_options is None):
            boto3_options = {}
        self._boto3_options = boto3_options
        if (s3_put_options is None):
            s3_put_options = {}
        self.s3_put_options = s3_put_options
        self.endpoint_url = endpoint_url
        if (not self._suppress_store_backend_id):
            _ = self.store_backend_id
        self._config = {'bucket': bucket, 'prefix': prefix, 'boto3_options': boto3_options, 's3_put_options': s3_put_options, 'filepath_template': filepath_template, 'filepath_prefix': filepath_prefix, 'filepath_suffix': filepath_suffix, 'forbidden_substrings': forbidden_substrings, 'platform_specific_separator': platform_specific_separator, 'fixed_length_key': fixed_length_key, 'suppress_store_backend_id': suppress_store_backend_id, 'manually_initialize_store_backend_id': manually_initialize_store_backend_id, 'base_public_path = None': base_public_path, 'endpoint_url': endpoint_url, 'store_name': store_name, 'module_name': self.__class__.__module__, 'class_name': self.__class__.__name__}
        filter_properties_dict(properties=self._config, clean_falsy=True, inplace=True)

    def _build_s3_object_key(self, key):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if self.platform_specific_separator:
            if self.prefix:
                s3_object_key = os.path.join(self.prefix, self._convert_key_to_filepath(key))
            else:
                s3_object_key = self._convert_key_to_filepath(key)
        elif self.prefix:
            s3_object_key = '/'.join((self.prefix, self._convert_key_to_filepath(key)))
        else:
            s3_object_key = self._convert_key_to_filepath(key)
        return s3_object_key

    def _get(self, key):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        s3_object_key = self._build_s3_object_key(key)
        s3 = self._create_client()
        try:
            s3_response_object = s3.get_object(Bucket=self.bucket, Key=s3_object_key)
        except (s3.exceptions.NoSuchKey, s3.exceptions.NoSuchBucket):
            raise InvalidKeyError(f'Unable to retrieve object from TupleS3StoreBackend with the following Key: {str(s3_object_key)}')
        return s3_response_object['Body'].read().decode(s3_response_object.get('ContentEncoding', 'utf-8'))

    def _set(self, key, value, content_encoding='utf-8', content_type='application/json', **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        s3_object_key = self._build_s3_object_key(key)
        s3 = self._create_resource()
        try:
            result_s3 = s3.Object(self.bucket, s3_object_key)
            if isinstance(value, str):
                result_s3.put(Body=value.encode(content_encoding), ContentEncoding=content_encoding, ContentType=content_type, **self.s3_put_options)
            else:
                result_s3.put(Body=value, ContentType=content_type, **self.s3_put_options)
        except s3.meta.client.exceptions.ClientError as e:
            logger.debug(str(e))
            raise StoreBackendError('Unable to set object in s3.')
        return s3_object_key

    def _move(self, source_key, dest_key, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        s3 = self._create_resource()
        source_filepath = self._convert_key_to_filepath(source_key)
        if (not source_filepath.startswith(self.prefix)):
            source_filepath = os.path.join(self.prefix, source_filepath)
        dest_filepath = self._convert_key_to_filepath(dest_key)
        if (not dest_filepath.startswith(self.prefix)):
            dest_filepath = os.path.join(self.prefix, dest_filepath)
        s3.Bucket(self.bucket).copy({'Bucket': self.bucket, 'Key': source_filepath}, dest_filepath)
        s3.Object(self.bucket, source_filepath).delete()

    def list_keys(self, prefix: Tuple=()) -> List[Tuple]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        s3 = self._create_client()
        paginator = s3.get_paginator('list_objects_v2')
        if self.prefix:
            page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=self.prefix)
        else:
            page_iterator = paginator.paginate(Bucket=self.bucket)
        objects = []
        for page in page_iterator:
            current_page_contents = page.get('Contents')
            if ((current_page_contents is None) and (objects == []) and ('CommonPrefixes' in page)):
                logger.warning('TupleS3StoreBackend returned CommonPrefixes, but delimiter should not have been set.')
                objects = []
                break
            if (current_page_contents is not None):
                objects.extend(current_page_contents)
        key_list = []
        for s3_object_info in objects:
            s3_object_key = s3_object_info['Key']
            if self.platform_specific_separator:
                s3_object_key = os.path.relpath(s3_object_key, self.prefix)
            elif (self.prefix is None):
                if s3_object_key.startswith('/'):
                    s3_object_key = s3_object_key[1:]
            elif s3_object_key.startswith(f'{self.prefix}/'):
                s3_object_key = s3_object_key[(len(self.prefix) + 1):]
            if (self.filepath_prefix and (not s3_object_key.startswith(self.filepath_prefix))):
                continue
            elif (self.filepath_suffix and (not s3_object_key.endswith(self.filepath_suffix))):
                continue
            key = self._convert_filepath_to_key(s3_object_key)
            if key:
                key_list.append(key)
        return key_list

    def get_url_for_key(self, key, protocol=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        location = None
        if self.boto3_options.get('endpoint_url'):
            location = self.boto3_options.get('endpoint_url')
        else:
            location = self._create_client().get_bucket_location(Bucket=self.bucket)['LocationConstraint']
            if (location is None):
                location = 'https://s3.amazonaws.com'
            else:
                location = f'https://s3-{location}.amazonaws.com'
        s3_key = self._convert_key_to_filepath(key)
        if (not self.prefix):
            return f'{location}/{self.bucket}/{s3_key}'
        return f'{location}/{self.bucket}/{self.prefix}/{s3_key}'

    def get_public_url_for_key(self, key, protocol=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (not self.base_public_path):
            raise StoreBackendError('Error: No base_public_path was configured!\n                    - A public URL was requested base_public_path was not configured for the\n                ')
        s3_key = self._convert_key_to_filepath(key)
        if (self.base_public_path[(- 1)] != '/'):
            public_url = f'{self.base_public_path}/{s3_key}'
        else:
            public_url = (self.base_public_path + s3_key)
        return public_url

    def remove_key(self, key):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (not isinstance(key, tuple)):
            key = key.to_tuple()
        s3 = self._create_resource()
        s3_object_key = self._build_s3_object_key(key)
        if self.has_key(key):
            s3.Object(self.bucket, s3_object_key).delete()
            return True
        else:
            return False

    def _has_key(self, key):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        all_keys = self.list_keys()
        return (key in all_keys)

    @property
    def boto3_options(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        from botocore.client import Config
        result = {}
        if self._boto3_options.get('signature_version'):
            signature_version = self._boto3_options.pop('signature_version')
            result['config'] = Config(signature_version=signature_version)
        result.update(self._boto3_options)
        return result

    def _create_client(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        import boto3
        return boto3.client('s3', **self.boto3_options)

    def _create_resource(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        import boto3
        return boto3.resource('s3', **self.boto3_options)

    @property
    def config(self) -> dict:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._config

class TupleGCSStoreBackend(TupleStoreBackend):
    '\n    Uses a GCS bucket as a store.\n\n    The key to this StoreBackend must be a tuple with fixed length based on the filepath_template,\n    or a variable-length tuple may be used and returned with an optional filepath_suffix (to be) added.\n\n    The filepath_template is a string template used to convert the key to a filepath.\n    '

    def __init__(self, bucket, project, prefix='', filepath_template=None, filepath_prefix=None, filepath_suffix=None, forbidden_substrings=None, platform_specific_separator=False, fixed_length_key=False, suppress_store_backend_id=False, manually_initialize_store_backend_id: str='', public_urls=True, base_public_path=None, store_name=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__(filepath_template=filepath_template, filepath_prefix=filepath_prefix, filepath_suffix=filepath_suffix, forbidden_substrings=forbidden_substrings, platform_specific_separator=platform_specific_separator, fixed_length_key=fixed_length_key, suppress_store_backend_id=suppress_store_backend_id, manually_initialize_store_backend_id=manually_initialize_store_backend_id, base_public_path=base_public_path, store_name=store_name)
        self.bucket = bucket
        self.prefix = prefix
        self.project = project
        self._public_urls = public_urls
        if (not self._suppress_store_backend_id):
            _ = self.store_backend_id
        self._config = {'bucket': bucket, 'project': project, 'prefix': prefix, 'filepath_template': filepath_template, 'filepath_prefix': filepath_prefix, 'filepath_suffix': filepath_suffix, 'forbidden_substrings': forbidden_substrings, 'platform_specific_separator': platform_specific_separator, 'fixed_length_key': fixed_length_key, 'suppress_store_backend_id': suppress_store_backend_id, 'manually_initialize_store_backend_id': manually_initialize_store_backend_id, 'public_urls': public_urls, 'base_public_path': base_public_path, 'store_name': store_name, 'module_name': self.__class__.__module__, 'class_name': self.__class__.__name__}
        filter_properties_dict(properties=self._config, clean_falsy=True, inplace=True)

    def _build_gcs_object_key(self, key):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if self.platform_specific_separator:
            if self.prefix:
                gcs_object_key = os.path.join(self.prefix, self._convert_key_to_filepath(key))
            else:
                gcs_object_key = self._convert_key_to_filepath(key)
        elif self.prefix:
            gcs_object_key = '/'.join((self.prefix, self._convert_key_to_filepath(key)))
        else:
            gcs_object_key = self._convert_key_to_filepath(key)
        return gcs_object_key

    def _get(self, key):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        gcs_object_key = self._build_gcs_object_key(key)
        from google.cloud import storage
        gcs = storage.Client(project=self.project)
        bucket = gcs.bucket(self.bucket)
        gcs_response_object = bucket.get_blob(gcs_object_key)
        if (not gcs_response_object):
            raise InvalidKeyError(f'Unable to retrieve object from TupleGCSStoreBackend with the following Key: {str(key)}')
        else:
            return gcs_response_object.download_as_string().decode('utf-8')

    def _set(self, key, value, content_encoding='utf-8', content_type='application/json', **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        gcs_object_key = self._build_gcs_object_key(key)
        from google.cloud import storage
        gcs = storage.Client(project=self.project)
        bucket = gcs.bucket(self.bucket)
        blob = bucket.blob(gcs_object_key)
        if isinstance(value, str):
            blob.content_encoding = content_encoding
            blob.upload_from_string(value.encode(content_encoding), content_type=content_type)
        else:
            blob.upload_from_string(value, content_type=content_type)
        return gcs_object_key

    def _move(self, source_key, dest_key, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        from google.cloud import storage
        gcs = storage.Client(project=self.project)
        bucket = gcs.bucket(self.bucket)
        source_filepath = self._convert_key_to_filepath(source_key)
        if (not source_filepath.startswith(self.prefix)):
            source_filepath = os.path.join(self.prefix, source_filepath)
        dest_filepath = self._convert_key_to_filepath(dest_key)
        if (not dest_filepath.startswith(self.prefix)):
            dest_filepath = os.path.join(self.prefix, dest_filepath)
        blob = bucket.blob(source_filepath)
        _ = bucket.rename_blob(blob, dest_filepath)

    def list_keys(self, prefix: Tuple=()) -> List[Tuple]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        key_list = []
        from google.cloud import storage
        gcs = storage.Client(self.project)
        for blob in gcs.list_blobs(self.bucket, prefix=self.prefix):
            gcs_object_name = blob.name
            gcs_object_key = os.path.relpath(gcs_object_name, self.prefix)
            if (self.filepath_prefix and (not gcs_object_key.startswith(self.filepath_prefix))):
                continue
            elif (self.filepath_suffix and (not gcs_object_key.endswith(self.filepath_suffix))):
                continue
            key = self._convert_filepath_to_key(gcs_object_key)
            if key:
                key_list.append(key)
        return key_list

    def get_url_for_key(self, key, protocol=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        path = self._convert_key_to_filepath(key)
        if self._public_urls:
            base_url = 'https://storage.googleapis.com/'
        else:
            base_url = 'https://storage.cloud.google.com/'
        path_url = self._get_path_url(path)
        return (base_url + path_url)

    def get_public_url_for_key(self, key, protocol=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (not self.base_public_path):
            raise StoreBackendError('Error: No base_public_path was configured!\n                    - A public URL was requested base_public_path was not configured for the\n                ')
        path = self._convert_key_to_filepath(key)
        path_url = self._get_path_url(path)
        public_url = (self.base_public_path + path_url)
        return public_url

    def _get_path_url(self, path):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if self.prefix:
            path_url = '/'.join((self.bucket, self.prefix, path))
        elif self.base_public_path:
            if (self.base_public_path[(- 1)] != '/'):
                path_url = f'/{path}'
            else:
                path_url = path
        else:
            path_url = '/'.join((self.bucket, path))
        return path_url

    def remove_key(self, key):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        from google.cloud import storage
        from google.cloud.exceptions import NotFound
        gcs = storage.Client(project=self.project)
        bucket = gcs.bucket(self.bucket)
        try:
            bucket.delete_blobs(blobs=list(bucket.list_blobs(prefix=self.prefix)))
        except NotFound:
            return False
        return True

    def _has_key(self, key):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        all_keys = self.list_keys()
        return (key in all_keys)

class TupleAzureBlobStoreBackend(TupleStoreBackend):
    '\n    Uses an Azure Blob as a store.\n\n    The key to this StoreBackend must be a tuple with fixed length based on the filepath_template,\n    or a variable-length tuple may be used and returned with an optional filepath_suffix (to be) added.\n    The filepath_template is a string template used to convert the key to a filepath.\n\n    You need to setup the connection string environment variable\n    https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python\n    '

    def __init__(self, container, connection_string, prefix='', filepath_template=None, filepath_prefix=None, filepath_suffix=None, forbidden_substrings=None, platform_specific_separator=False, fixed_length_key=False, suppress_store_backend_id=False, manually_initialize_store_backend_id: str='', store_name=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__(filepath_template=filepath_template, filepath_prefix=filepath_prefix, filepath_suffix=filepath_suffix, forbidden_substrings=forbidden_substrings, platform_specific_separator=platform_specific_separator, fixed_length_key=fixed_length_key, suppress_store_backend_id=suppress_store_backend_id, manually_initialize_store_backend_id=manually_initialize_store_backend_id, store_name=store_name)
        self.connection_string = connection_string
        self.prefix = prefix
        self.container = container

    def _get_container_client(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        from azure.storage.blob import BlobServiceClient
        if self.connection_string:
            return BlobServiceClient.from_connection_string(self.connection_string).get_container_client(self.container)
        else:
            raise StoreBackendError('Unable to initialize ServiceClient, AZURE_STORAGE_CONNECTION_STRING should be set')

    def _get(self, key):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        az_blob_key = os.path.join(self.prefix, self._convert_key_to_filepath(key))
        return self._get_container_client().download_blob(az_blob_key).readall().decode('utf-8')

    def _set(self, key, value, content_encoding='utf-8', **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        from azure.storage.blob import ContentSettings
        az_blob_key = os.path.join(self.prefix, self._convert_key_to_filepath(key))
        if isinstance(value, str):
            if az_blob_key.endswith('.html'):
                my_content_settings = ContentSettings(content_type='text/html')
                self._get_container_client().upload_blob(name=az_blob_key, data=value, encoding=content_encoding, overwrite=True, content_settings=my_content_settings)
            else:
                self._get_container_client().upload_blob(name=az_blob_key, data=value, encoding=content_encoding, overwrite=True)
        else:
            self._get_container_client().upload_blob(name=az_blob_key, data=value, overwrite=True)
        return az_blob_key

    def list_keys(self, prefix: Tuple=()) -> List[Tuple]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        key_list = []
        for obj in self._get_container_client().list_blobs(name_starts_with=self.prefix):
            az_blob_key = os.path.relpath(obj.name)
            if az_blob_key.startswith(f'{self.prefix}/'):
                az_blob_key = az_blob_key[(len(self.prefix) + 1):]
            if (self.filepath_prefix and (not az_blob_key.startswith(self.filepath_prefix))):
                continue
            elif (self.filepath_suffix and (not az_blob_key.endswith(self.filepath_suffix))):
                continue
            key = self._convert_filepath_to_key(az_blob_key)
            key_list.append(key)
        return key_list

    def get_url_for_key(self, key, protocol=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        az_blob_key = self._convert_key_to_filepath(key)
        az_blob_path = os.path.join(self.container, self.prefix, az_blob_key)
        return 'https://{}.blob.core.windows.net/{}'.format(self._get_container_client().account_name, az_blob_path)

    def _has_key(self, key):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        all_keys = self.list_keys()
        return (key in all_keys)

    def _move(self, source_key, dest_key, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        source_blob_path = self._convert_key_to_filepath(source_key)
        if (not source_blob_path.startswith(self.prefix)):
            source_blob_path = os.path.join(self.prefix, source_blob_path)
        dest_blob_path = self._convert_key_to_filepath(dest_key)
        if (not dest_blob_path.startswith(self.prefix)):
            dest_blob_path = os.path.join(self.prefix, dest_blob_path)
        source_blob = self._get_container_client().get_blob_client(source_blob_path)
        dest_blob = self._get_container_client().get_blob_client(dest_blob_path)
        dest_blob.start_copy_from_url(source_blob.url, requires_sync=True)
        copy_properties = dest_blob.get_blob_properties().copy
        if (copy_properties.status != 'success'):
            dest_blob.abort_copy(copy_properties.id)
            raise StoreBackendError(('Unable to copy blob %s with status %s' % (source_blob_path, copy_properties.status)))
        source_blob.delete_blob()

    def remove_key(self, key):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (not isinstance(key, tuple)):
            key = key.to_tuple()
        az_blob_path = self._convert_key_to_filepath(key)
        if (not az_blob_path.startswith(self.prefix)):
            az_blob_path = os.path.join(self.prefix, az_blob_path)
        blob = self._get_container_client().get_blob_client(az_blob_path)
        blob.delete_blob()
        return True

    @property
    def config(self) -> dict:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._config
