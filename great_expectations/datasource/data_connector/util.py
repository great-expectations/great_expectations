
import copy
import logging
import os
import re
import sre_constants
import sre_parse
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import BatchDefinition, BatchRequestBase
from great_expectations.core.id_dict import IDDict
from great_expectations.data_context.types.base import assetConfigSchema
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.data_connector.asset import Asset
from great_expectations.datasource.data_connector.sorter import Sorter
logger = logging.getLogger(__name__)
try:
    from azure.storage.blob import BlobPrefix
except ImportError:
    BlobPrefix = None
    logger.debug('Unable to load azure types; install optional Azure dependency for support.')
try:
    from google.cloud import storage
except ImportError:
    storage = None
    logger.debug('Unable to load GCS connection object; install optional Google dependency for support')
try:
    import pyspark
    import pyspark.sql as pyspark_sql
except ImportError:
    pyspark = None
    pyspark_sql = None
    logger.debug('Unable to load pyspark and pyspark.sql; install optional Spark dependency for support.')
DEFAULT_DATA_ASSET_NAME: str = 'DEFAULT_ASSET_NAME'

def batch_definition_matches_batch_request(batch_definition: BatchDefinition, batch_request: BatchRequestBase) -> bool:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    assert isinstance(batch_definition, BatchDefinition)
    assert isinstance(batch_request, BatchRequestBase)
    if (batch_request.datasource_name and (batch_request.datasource_name != batch_definition.datasource_name)):
        return False
    if (batch_request.data_connector_name and (batch_request.data_connector_name != batch_definition.data_connector_name)):
        return False
    if (batch_request.data_asset_name and (batch_request.data_asset_name != batch_definition.data_asset_name)):
        return False
    if batch_request.data_connector_query:
        batch_filter_parameters: Any = batch_request.data_connector_query.get('batch_filter_parameters')
        if batch_filter_parameters:
            if (not isinstance(batch_filter_parameters, dict)):
                return False
            for key in batch_filter_parameters.keys():
                if (not ((key in batch_definition.batch_identifiers) and (batch_definition.batch_identifiers[key] == batch_filter_parameters[key]))):
                    return False
    if batch_request.batch_identifiers:
        if (not isinstance(batch_request.batch_identifiers, dict)):
            return False
        for key in batch_request.batch_identifiers.keys():
            if (not ((key in batch_definition.batch_identifiers) and (batch_definition.batch_identifiers[key] == batch_request.batch_identifiers[key]))):
                return False
    return True

def map_data_reference_string_to_batch_definition_list_using_regex(datasource_name: str, data_connector_name: str, data_reference: str, regex_pattern: str, group_names: List[str], data_asset_name: Optional[str]=None) -> Optional[List[BatchDefinition]]:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    processed_data_reference: Optional[Tuple[(str, IDDict)]] = convert_data_reference_string_to_batch_identifiers_using_regex(data_reference=data_reference, regex_pattern=regex_pattern, group_names=group_names)
    if (processed_data_reference is None):
        return None
    data_asset_name_from_batch_identifiers: str = processed_data_reference[0]
    batch_identifiers: IDDict = processed_data_reference[1]
    if (data_asset_name is None):
        data_asset_name = data_asset_name_from_batch_identifiers
    return [BatchDefinition(datasource_name=datasource_name, data_connector_name=data_connector_name, data_asset_name=data_asset_name, batch_identifiers=IDDict(batch_identifiers))]

def convert_data_reference_string_to_batch_identifiers_using_regex(data_reference: str, regex_pattern: str, group_names: List[str]) -> Optional[Tuple[(str, IDDict)]]:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    pattern = re.compile(regex_pattern)
    matches: Optional[re.Match] = pattern.match(data_reference)
    if (matches is None):
        return None
    match_dict = matches.groupdict()
    if match_dict:
        batch_identifiers = _determine_batch_identifiers_using_named_groups(match_dict, group_names)
    else:
        groups: list = list(matches.groups())
        batch_identifiers: IDDict = IDDict(dict(zip(group_names, groups)))
    data_asset_name: str = batch_identifiers.pop('data_asset_name', DEFAULT_DATA_ASSET_NAME)
    return (data_asset_name, batch_identifiers)

def _determine_batch_identifiers_using_named_groups(match_dict: dict, group_names: List[str]) -> IDDict:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    batch_identifiers = IDDict()
    for (key, value) in match_dict.items():
        if (key in group_names):
            batch_identifiers[key] = value
        else:
            logger.warning(f"The named group '{key}' must explicitly be stated in group_names to be parsed")
    return batch_identifiers

def map_batch_definition_to_data_reference_string_using_regex(batch_definition: BatchDefinition, regex_pattern: str, group_names: List[str]) -> str:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    if (not isinstance(batch_definition, BatchDefinition)):
        raise TypeError('batch_definition is not of an instance of type BatchDefinition')
    data_asset_name: str = batch_definition.data_asset_name
    batch_identifiers: IDDict = batch_definition.batch_identifiers
    data_reference: str = convert_batch_identifiers_to_data_reference_string_using_regex(batch_identifiers=batch_identifiers, regex_pattern=regex_pattern, group_names=group_names, data_asset_name=data_asset_name)
    return data_reference

def convert_batch_identifiers_to_data_reference_string_using_regex(batch_identifiers: IDDict, regex_pattern: str, group_names: List[str], data_asset_name: Optional[str]=None) -> str:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    if (not isinstance(batch_identifiers, IDDict)):
        raise TypeError('batch_identifiers is not an instance of type IDDict')
    template_arguments: dict = copy.deepcopy(batch_identifiers)
    if (data_asset_name is not None):
        template_arguments['data_asset_name'] = data_asset_name
    filepath_template: str = _invert_regex_to_data_reference_template(regex_pattern=regex_pattern, group_names=group_names)
    converted_string: str = filepath_template.format(**template_arguments)
    return converted_string

def _invert_regex_to_data_reference_template(regex_pattern: str, group_names: List[str]) -> str:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Create a string template based on a regex and corresponding list of group names.\n\n    For example:\n\n        filepath_template = _invert_regex_to_data_reference_template(\n            regex_pattern=r"^(.+)_(\\d+)_(\\d+)\\.csv$",\n            group_names=["name", "timestamp", "price"],\n        )\n        filepath_template\n        >> "{name}_{timestamp}_{price}.csv"\n\n    Such templates are useful because they can be populated using string substitution:\n\n        filepath_template.format(**{\n            "name": "user_logs",\n            "timestamp": "20200101",\n            "price": "250",\n        })\n        >> "user_logs_20200101_250.csv"\n\n\n    NOTE Abe 20201017: This method is almost certainly still brittle. I haven\'t exhaustively mapped the OPCODES in sre_constants\n    '
    data_reference_template: str = ''
    group_name_index: int = 0
    num_groups = len(group_names)
    parsed_sre = sre_parse.parse(regex_pattern)
    for (token, value) in parsed_sre:
        if (token == sre_constants.LITERAL):
            data_reference_template += chr(value)
        elif (token == sre_constants.SUBPATTERN):
            if (not (group_name_index < num_groups)):
                break
            data_reference_template += f'{{{group_names[group_name_index]}}}'
            group_name_index += 1
        elif (token in [sre_constants.MAX_REPEAT, sre_constants.IN, sre_constants.BRANCH, sre_constants.ANY]):
            data_reference_template += '*'
        elif (token in [sre_constants.AT, sre_constants.ASSERT_NOT, sre_constants.ASSERT]):
            pass
        else:
            raise ValueError(f'Unrecognized regex token {token} in regex pattern {regex_pattern}.')
    data_reference_template: str = re.sub('\\*+', '*', data_reference_template)
    return data_reference_template

def normalize_directory_path(dir_path: str, root_directory_path: Optional[str]=None) -> str:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    if (Path(dir_path).is_absolute() or (root_directory_path is None)):
        return dir_path
    else:
        return str(Path(root_directory_path).joinpath(dir_path))

def get_filesystem_one_level_directory_glob_path_list(base_directory_path: str, glob_directive: str) -> List[str]:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    '\n    List file names, relative to base_directory_path one level deep, with expansion specified by glob_directive.\n    :param base_directory_path -- base directory path, relative to which file paths will be collected\n    :param glob_directive -- glob expansion directive\n    :returns -- list of relative file paths\n    '
    globbed_paths = Path(base_directory_path).glob(glob_directive)
    path_list: List[str] = [os.path.relpath(str(posix_path), base_directory_path) for posix_path in globbed_paths]
    return path_list

def list_azure_keys(azure, query_options: dict, recursive: bool=False) -> List[str]:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    '\n    Utilizes the Azure Blob Storage connection object to retrieve blob names based on user-provided criteria.\n\n    For InferredAssetAzureDataConnector, we take container and name_starts_with and search for files using RegEx at and below the level\n    specified by those parameters. However, for ConfiguredAssetAzureDataConnector, we take container and name_starts_with and\n    search for files using RegEx only at the level specified by that bucket and prefix.\n\n    This restriction for the ConfiguredAssetAzureDataConnector is needed, because paths on Azure are comprised not only the leaf file name\n    but the full path that includes both the prefix and the file name.  Otherwise, in the situations where multiple data assets\n    share levels of a directory tree, matching files to data assets will not be possible, due to the path ambiguity.\n\n    Args:\n        azure (BlobServiceClient): Azure connnection object responsible for accessing container\n        query_options (dict): Azure query attributes ("container", "name_starts_with", "delimiter")\n        recursive (bool): True for InferredAssetAzureDataConnector and False for ConfiguredAssetAzureDataConnector (see above)\n\n    Returns:\n        List of keys representing Azure file paths (as filtered by the query_options dict)\n    '
    container: str = query_options['container']
    container_client = azure.get_container_client(container)
    path_list: List[str] = []

    def _walk_blob_hierarchy(name_starts_with: str) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        for item in container_client.walk_blobs(name_starts_with=name_starts_with):
            if isinstance(item, BlobPrefix):
                if recursive:
                    _walk_blob_hierarchy(name_starts_with=item.name)
            else:
                path_list.append(item.name)
    name_starts_with: str = query_options['name_starts_with']
    _walk_blob_hierarchy(name_starts_with)
    return path_list

def list_gcs_keys(gcs, query_options: dict, recursive: bool=False) -> List[str]:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    '\n    Utilizes the GCS connection object to retrieve blob names based on user-provided criteria.\n\n    For InferredAssetGCSDataConnector, we take `bucket_or_name` and `prefix` and search for files using RegEx at and below the level\n    specified by those parameters. However, for ConfiguredAssetGCSDataConnector, we take `bucket_or_name` and `prefix` and\n    search for files using RegEx only at the level specified by that bucket and prefix.\n\n    This restriction for the ConfiguredAssetGCSDataConnector is needed because paths on GCS are comprised not only the leaf file name\n    but the full path that includes both the prefix and the file name. Otherwise, in the situations where multiple data assets\n    share levels of a directory tree, matching files to data assets will not be possible due to the path ambiguity.\n\n    Please note that the SDK\'s `list_blobs` method takes in a `delimiter` key that drastically alters the traversal of a given bucket:\n        - If a delimiter is not set (default), the traversal is recursive and the output will contain all blobs in the current directory\n          as well as those in any nested directories.\n        - If a delimiter is set, the traversal will continue until that value is seen; as the default is "/", traversal will be scoped\n          within the current directory and end before visiting nested directories.\n\n    In order to provide users with finer control of their config while also ensuring output that is in line with the `recursive` arg,\n    we deem it appropriate to manually override the value of the delimiter only in cases where it is absolutely necessary.\n\n    Args:\n        gcs (storage.Client): GCS connnection object responsible for accessing bucket\n        query_options (dict): GCS query attributes ("bucket_or_name", "prefix", "delimiter", "max_results")\n        recursive (bool): True for InferredAssetGCSDataConnector and False for ConfiguredAssetGCSDataConnector (see above)\n\n    Returns:\n        List of keys representing GCS file paths (as filtered by the `query_options` dict)\n    '
    delimiter = query_options['delimiter']
    if ((delimiter is None) and (not recursive)):
        warnings.warn('In order to access blobs with a ConfiguredAssetGCSDataConnector,             the delimiter that has been passed to gcs_options in your config cannot be empty;             please note that the value is being set to the default "/" in order to work with the Google SDK.')
        query_options['delimiter'] = '/'
    elif ((delimiter is not None) and recursive):
        warnings.warn('In order to access blobs with an InferredAssetGCSDataConnector,             the delimiter that has been passed to gcs_options in your config must be empty;             please note that the value is being set to None in order to work with the Google SDK.')
        query_options['delimiter'] = None
    keys: List[str] = []
    for blob in gcs.list_blobs(**query_options):
        name: str = blob.name
        if name.endswith('/'):
            continue
        keys.append(name)
    return keys

def list_s3_keys(s3, query_options: dict, iterator_dict: dict, recursive: bool=False) -> str:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    '\n    For InferredAssetS3DataConnector, we take bucket and prefix and search for files using RegEx at and below the level\n    specified by that bucket and prefix.  However, for ConfiguredAssetS3DataConnector, we take bucket and prefix and\n    search for files using RegEx only at the level specified by that bucket and prefix.  This restriction for the\n    ConfiguredAssetS3DataConnector is needed, because paths on S3 are comprised not only the leaf file name but the\n    full path that includes both the prefix and the file name.  Otherwise, in the situations where multiple data assets\n    share levels of a directory tree, matching files to data assets will not be possible, due to the path ambiguity.\n    :param s3: s3 client connection\n    :param query_options: s3 query attributes ("Bucket", "Prefix", "Delimiter", "MaxKeys")\n    :param iterator_dict: dictionary to manage "NextContinuationToken" (if "IsTruncated" is returned from S3)\n    :param recursive: True for InferredAssetS3DataConnector and False for ConfiguredAssetS3DataConnector (see above)\n    :return: string valued key representing file path on S3 (full prefix and leaf file name)\n    '
    if (iterator_dict is None):
        iterator_dict = {}
    if ('continuation_token' in iterator_dict):
        query_options.update({'ContinuationToken': iterator_dict['continuation_token']})
    logger.debug(f'Fetching objects from S3 with query options: {query_options}')
    s3_objects_info: dict = s3.list_objects_v2(**query_options)
    if (not any(((key in s3_objects_info) for key in ['Contents', 'CommonPrefixes']))):
        raise ValueError('S3 query may not have been configured correctly.')
    if ('Contents' in s3_objects_info):
        keys: List[str] = [item['Key'] for item in s3_objects_info['Contents'] if (item['Size'] > 0)]
        (yield from keys)
    if (recursive and ('CommonPrefixes' in s3_objects_info)):
        common_prefixes: List[Dict[(str, Any)]] = s3_objects_info['CommonPrefixes']
        for prefix_info in common_prefixes:
            query_options_tmp: dict = copy.deepcopy(query_options)
            query_options_tmp.update({'Prefix': prefix_info['Prefix']})
            (yield from list_s3_keys(s3=s3, query_options=query_options_tmp, iterator_dict={}, recursive=recursive))
    if s3_objects_info['IsTruncated']:
        iterator_dict['continuation_token'] = s3_objects_info['NextContinuationToken']
        (yield from list_s3_keys(s3=s3, query_options=query_options, iterator_dict=iterator_dict, recursive=recursive))
    if ('continuation_token' in iterator_dict):
        del iterator_dict['continuation_token']

def build_sorters_from_config(config_list: List[Dict[(str, Any)]]) -> Optional[dict]:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    sorter_dict: Dict[(str, Sorter)] = {}
    if (config_list is not None):
        for sorter_config in config_list:
            if (sorter_config is None):
                return None
            if ('name' not in sorter_config):
                raise ValueError('Sorter config should have a name')
            sorter_name: str = sorter_config['name']
            new_sorter: Sorter = _build_sorter_from_config(sorter_config=sorter_config)
            sorter_dict[sorter_name] = new_sorter
    return sorter_dict

def _build_sorter_from_config(sorter_config: Dict[(str, Any)]) -> Sorter:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Build a Sorter using the provided configuration and return the newly-built Sorter.'
    runtime_environment: dict = {'name': sorter_config['name']}
    sorter: Sorter = instantiate_class_from_config(config=sorter_config, runtime_environment=runtime_environment, config_defaults={'module_name': 'great_expectations.datasource.data_connector.sorter'})
    return sorter

def _build_asset_from_config(runtime_environment: 'DataConnector', config: dict) -> Asset:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Build Asset from configuration and return asset. Used by both ConfiguredAssetDataConnector and RuntimeDataConnector'
    runtime_environment: dict = {'data_connector': runtime_environment}
    config = assetConfigSchema.load(config)
    config = assetConfigSchema.dump(config)
    asset: Asset = instantiate_class_from_config(config=config, runtime_environment=runtime_environment, config_defaults={})
    if (not asset):
        raise ge_exceptions.ClassInstantiationError(module_name='great_expectations.datasource.data_connector.asset', package_name=None, class_name=config['class_name'])
    return asset
