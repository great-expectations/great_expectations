from __future__ import annotations

import copy
import enum
import itertools
import json
import logging
import pathlib
import tempfile
import uuid
import warnings
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    List,
    Mapping,
    Optional,
    Set,
    Type,
    TypeVar,
    Union,
)

from marshmallow import (
    INCLUDE,
    Schema,
    ValidationError,
    fields,
    post_dump,
    post_load,
    pre_dump,
    validates_schema,
)
from marshmallow.warnings import RemovedInMarshmallow4Warning
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.compat import StringIO

import great_expectations.exceptions as gx_exceptions
from great_expectations._docs_decorators import public_api
from great_expectations.compatibility import pyspark
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.configuration import AbstractConfig, AbstractConfigSchema
from great_expectations.data_context.constants import (
    CURRENT_GX_CONFIG_VERSION,
    MINIMUM_SUPPORTED_CONFIG_VERSION,
)
from great_expectations.types import DictDot, SerializableDictDot
from great_expectations.util import (
    convert_to_json_serializable,  # noqa: TID251
    deep_filter_properties_iterable,
)

if TYPE_CHECKING:
    from io import TextIOWrapper

    from great_expectations.alias_types import JSONValues, PathStr
    from great_expectations.core.batch import BatchRequestBase
    from great_expectations.datasource.fluent.batch_request import (
        BatchRequest as FluentBatchRequest,
    )

yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# NOTE 121822: (kilo59) likely won't moving to marshmallow v4 so we don't care about this
warnings.simplefilter(action="ignore", category=RemovedInMarshmallow4Warning)


def object_to_yaml_str(obj):
    output_str: str
    with StringIO() as string_stream:
        yaml.dump(obj, string_stream)
        output_str = string_stream.getvalue()
    return output_str


BYC = TypeVar("BYC", bound="BaseYamlConfig")


class BaseYamlConfig(SerializableDictDot):
    _config_schema_class: ClassVar[Optional[Type[Schema]]] = None

    exclude_field_names: ClassVar[Set[str]] = {
        "commented_map",
    }

    def __init__(self, commented_map: Optional[CommentedMap] = None) -> None:
        if commented_map is None:
            commented_map = CommentedMap()
        self._commented_map = commented_map

    @classmethod
    def _get_schema_instance(cls: Type[BYC]) -> Schema:
        if not issubclass(cls.get_schema_class(), Schema):
            raise gx_exceptions.InvalidConfigError(  # noqa: TRY003
                "Invalid type: A configuration schema class needs to inherit from the Marshmallow Schema class."  # noqa: E501
            )

        if not issubclass(cls.get_config_class(), BaseYamlConfig):
            raise gx_exceptions.InvalidConfigError(  # noqa: TRY003
                "Invalid type: A configuration class needs to inherit from the BaseYamlConfig class."  # noqa: E501
            )

        if hasattr(cls.get_config_class(), "_schema_instance"):
            # noinspection PyProtectedMember
            schema_instance: Optional[Schema] = cls.get_config_class()._schema_instance
            if schema_instance is None:
                cls.get_config_class()._schema_instance = (cls.get_schema_class())()
                return cls.get_config_class().schema_instance
            else:
                return schema_instance
        else:
            cls.get_config_class().schema_instance = (cls.get_schema_class())()
            return cls.get_config_class().schema_instance

    @classmethod
    def from_commented_map(cls: Type[BYC], commented_map: Union[CommentedMap, Dict]) -> BYC:
        try:
            schema_instance: Schema = cls._get_schema_instance()
            config: Union[dict, BYC] = schema_instance.load(commented_map)
            if isinstance(config, dict):
                return cls.get_config_class()(commented_map=commented_map, **config)

            return config
        except ValidationError:
            logger.error(  # noqa: TRY400
                "Encountered errors during loading config.  See ValidationError for more details."
            )
            raise

    def _get_schema_validated_updated_commented_map(self) -> CommentedMap:
        commented_map: CommentedMap = copy.deepcopy(self._commented_map)
        schema_validated_map: dict = self._get_schema_instance().dump(self)
        commented_map.update(schema_validated_map)
        return commented_map

    def to_yaml(self, outfile: Union[str, pathlib.Path, TextIOWrapper]) -> None:
        """
        :returns None (but writes a YAML file containing the project configuration)
        """
        yaml.dump(self.commented_map, outfile)

    def to_yaml_str(self) -> str:
        """
        :returns a YAML string containing the project configuration
        """
        return object_to_yaml_str(obj=self.commented_map)

    @override
    def to_json_dict(self) -> dict[str, JSONValues]:
        """Returns a JSON-serializable dict containing this DataContextConfig.

        Returns:
            A JSON-serializable dict representation of this project configuration.
        """
        commented_map: CommentedMap = self.commented_map
        return convert_to_json_serializable(data=commented_map)

    @property
    def commented_map(self) -> CommentedMap:
        return self._get_schema_validated_updated_commented_map()

    @classmethod
    def get_config_class(cls: Type) -> Type:
        raise NotImplementedError

    @classmethod
    def get_schema_class(cls) -> Type[Schema]:
        raise NotImplementedError


class SorterConfig(DictDot):
    def __init__(  # noqa: PLR0913
        self,
        name,
        class_name=None,
        module_name=None,
        orderby="asc",
        reference_list=None,
        order_keys_by=None,
        key_reference_list=None,
        datetime_format=None,
        **kwargs,
    ) -> None:
        self._name = name
        self._class_name = class_name
        self._module_name = module_name
        self._orderby = orderby
        for k, v in kwargs.items():
            setattr(self, k, v)

        if reference_list is not None:
            self._reference_list = reference_list

        if order_keys_by is not None:
            self._order_keys_by = order_keys_by

        if key_reference_list is not None:
            self._key_reference_list = key_reference_list

        if datetime_format is not None:
            self._datetime_format = datetime_format

    @property
    def name(self):
        return self._name

    @property
    def module_name(self):
        return self._module_name

    @property
    def class_name(self):
        return self._class_name

    @property
    def orderby(self):
        return self._orderby

    @property
    def reference_list(self):
        return self._reference_list

    @property
    def order_keys_by(self):
        return self._order_keys_by

    @property
    def key_reference_list(self):
        return self._key_reference_list

    @property
    def datetime_format(self):
        return self._datetime_format


class SorterConfigSchema(Schema):
    class Meta:
        unknown = INCLUDE

    name = fields.String(required=True)
    class_name = fields.String(
        required=True,
        allow_none=False,
    )
    module_name = fields.String(
        required=False,
        allow_none=True,
        missing="great_expectations.datasource.data_connector.sorter",
    )
    orderby = fields.String(
        required=False,
        allow_none=True,
        missing="asc",
    )

    # allow_none = True because it is only used by some Sorters
    reference_list = fields.List(
        cls_or_instance=fields.Str(),
        required=False,
        missing=None,
        allow_none=True,
    )
    order_keys_by = fields.String(
        required=False,
        allow_none=True,
    )
    key_reference_list = fields.List(
        cls_or_instance=fields.Str(),
        required=False,
        missing=None,
        allow_none=True,
    )
    datetime_format = fields.String(
        required=False,
        missing=None,
        allow_none=True,
    )

    @validates_schema
    def validate_schema(self, data, **kwargs) -> None:
        pass

    # noinspection PyUnusedLocal
    @post_load
    def make_sorter_config(self, data, **kwargs):
        return SorterConfig(**data)


class AssetConfig(SerializableDictDot):
    def __init__(  # noqa: C901, PLR0912, PLR0913
        self,
        name: Optional[str] = None,
        class_name: Optional[str] = None,
        module_name: Optional[str] = None,
        bucket: Optional[str] = None,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
        max_keys: Optional[int] = None,
        schema_name: Optional[str] = None,
        batch_spec_passthrough: Optional[Dict[str, Any]] = None,
        batch_identifiers: Optional[List[str]] = None,
        partitioner_method: Optional[str] = None,
        partitioner_kwargs: Optional[Dict[str, str]] = None,
        sorters: Optional[dict] = None,
        sampling_method: Optional[str] = None,
        sampling_kwargs: Optional[Dict[str, str]] = None,
        reader_options: Optional[Dict[str, Any]] = None,
        **kwargs: Optional[dict],
    ) -> None:
        if name is not None:
            self.name = name
        self._class_name = class_name
        self._module_name = module_name
        if bucket is not None:
            self.bucket = bucket
        if prefix is not None:
            self.prefix = prefix
        if delimiter is not None:
            self.delimiter = delimiter
        if max_keys is not None:
            self.max_keys = max_keys
        if schema_name is not None:
            self.schema_name = schema_name
        if batch_spec_passthrough is not None:
            self.batch_spec_passthrough = batch_spec_passthrough
        if batch_identifiers is not None:
            self.batch_identifiers = batch_identifiers
        if partitioner_method is not None:
            self.partitioner_method = partitioner_method
        if partitioner_kwargs is not None:
            self.partitioner_kwargs = partitioner_kwargs
        if sorters is not None:
            self.sorters = sorters
        if sampling_method is not None:
            self.sampling_method = sampling_method
        if sampling_kwargs is not None:
            self.sampling_kwargs = sampling_kwargs
        if reader_options is not None:
            self.reader_options = reader_options
        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def class_name(self) -> Optional[str]:
        return self._class_name

    @property
    def module_name(self) -> Optional[str]:
        return self._module_name

    @override
    def to_json_dict(self) -> Dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this AssetConfig.

        Returns:
            A JSON-serializable dict representation of this AssetConfig.
        """
        # TODO: <Alex>2/4/2022</Alex>
        # This implementation of "SerializableDictDot.to_json_dict() occurs frequently and should ideally serve as the  # noqa: E501
        # reference implementation in the "SerializableDictDot" class itself.  However, the circular import dependencies,  # noqa: E501
        # due to the location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules  # noqa: E501
        # make this refactoring infeasible at the present time.
        dict_obj: dict = self.to_dict()
        serializeable_dict: dict = convert_to_json_serializable(data=dict_obj)
        return serializeable_dict


class AssetConfigSchema(Schema):
    class Meta:
        unknown = INCLUDE

    name = fields.String(required=False, allow_none=True)
    class_name = fields.String(
        required=False,
        allow_none=True,
        missing="Asset",
    )
    module_name = fields.String(
        required=False,
        all_none=True,
        missing="great_expectations.datasource.data_connector.asset",
    )
    base_directory = fields.String(required=False, allow_none=True)
    glob_directive = fields.String(required=False, allow_none=True)
    pattern = fields.String(required=False, allow_none=True)
    group_names = fields.List(cls_or_instance=fields.Str(), required=False, allow_none=True)
    bucket = fields.String(required=False, allow_none=True)
    prefix = fields.String(required=False, allow_none=True)
    delimiter = fields.String(required=False, allow_none=True)
    max_keys = fields.Integer(required=False, allow_none=True)
    schema_name = fields.String(required=False, allow_none=True)
    batch_spec_passthrough = fields.Dict(required=False, allow_none=True)

    """
    Necessary addition for AWS Glue Data Catalog assets.
    By using AWS Glue Data Catalog, we need to have both database and table names.
    The partitions are optional, it must match the partitions defined in the table
    and it is used to create batch identifiers that allows the validation of a single
    partition. Example: if we have two partitions (year, month), specifying these would
    create one batch id per combination of year and month. The connector gets the partition
    values from the AWS Glue Data Catalog.
    """
    database_name = fields.String(required=False, allow_none=True)
    partitions = fields.List(cls_or_instance=fields.Str(), required=False, allow_none=True)

    # Necessary addition for Cloud assets
    table_name = fields.String(required=False, allow_none=True)
    type = fields.String(required=False, allow_none=True)

    batch_identifiers = fields.List(cls_or_instance=fields.Str(), required=False, allow_none=True)

    data_asset_name_prefix = fields.String(required=False, allow_none=True)
    data_asset_name_suffix = fields.String(required=False, allow_none=True)
    include_schema_name = fields.Boolean(required=False, allow_none=True)
    partitioner_method = fields.String(required=False, allow_none=True)
    partitioner_kwargs = fields.Dict(required=False, allow_none=True)
    sorters = fields.List(
        cls_or_instance=fields.Nested(SorterConfigSchema, required=False, allow_none=True),
        required=False,
        allow_none=True,
    )
    sampling_method = fields.String(required=False, allow_none=True)
    sampling_kwargs = fields.Dict(required=False, allow_none=True)

    reader_options = fields.Dict(keys=fields.Str(), required=False, allow_none=True)

    @validates_schema
    def validate_schema(self, data, **kwargs) -> None:
        pass

    @pre_dump
    def prepare_dump(self, data, **kwargs):
        """
        Schemas in Spark Dataframes are defined as StructType, which is not serializable
        This method calls the schema's jsonValue() method, which translates the object into a json
        """
        # check whether spark exists
        if (not pyspark.types) or (pyspark.types.StructType is None):
            return data

        batch_spec_passthrough_config = data.get("batch_spec_passthrough")
        if batch_spec_passthrough_config:
            reader_options: dict = batch_spec_passthrough_config.get("reader_options")
            if reader_options:
                schema = reader_options.get("schema")
                if schema and pyspark.types and isinstance(schema, pyspark.types.StructType):
                    data["batch_spec_passthrough"]["reader_options"]["schema"] = schema.jsonValue()
        return data

    # noinspection PyUnusedLocal
    @post_load
    def make_asset_config(self, data, **kwargs):
        return AssetConfig(**data)


class DataConnectorConfig(AbstractConfig):
    def __init__(  # noqa: C901, PLR0912, PLR0913, PLR0915
        self,
        class_name,
        name: Optional[str] = None,
        id: Optional[str] = None,
        module_name=None,
        credentials=None,
        assets=None,
        base_directory=None,
        glob_directive=None,
        default_regex=None,
        batch_identifiers=None,
        # S3
        boto3_options=None,
        bucket=None,
        max_keys=None,
        # Azure
        azure_options=None,
        container=None,
        name_starts_with=None,
        # GCS
        bucket_or_name=None,
        max_results=None,
        # Both S3/GCS
        prefix=None,
        # Both S3/Azure
        delimiter=None,
        data_asset_name_prefix=None,
        data_asset_name_suffix=None,
        include_schema_name=None,
        partitioner_method=None,
        partitioner_kwargs=None,
        sorters=None,
        sampling_method=None,
        sampling_kwargs=None,
        excluded_tables=None,
        included_tables=None,
        skip_inapplicable_tables=None,
        introspection_directives=None,
        batch_spec_passthrough=None,
        **kwargs,
    ) -> None:
        self._class_name = class_name
        self._module_name = module_name
        if credentials is not None:
            self.credentials = credentials
        if assets is not None:
            self.assets = assets
        if base_directory is not None:
            self.base_directory = base_directory
        if glob_directive is not None:
            self.glob_directive = glob_directive
        if default_regex is not None:
            self.default_regex = default_regex
        if batch_identifiers is not None:
            self.batch_identifiers = batch_identifiers
        if data_asset_name_prefix is not None:
            self.data_asset_name_prefix = data_asset_name_prefix
        if data_asset_name_suffix is not None:
            self.data_asset_name_suffix = data_asset_name_suffix
        if include_schema_name is not None:
            self.include_schema_name = include_schema_name
        if partitioner_method is not None:
            self.partitioner_method = partitioner_method
        if partitioner_kwargs is not None:
            self.partitioner_kwargs = partitioner_kwargs
        if sorters is not None:
            self.sorters = sorters
        if sampling_method is not None:
            self.sampling_method = sampling_method
        if sampling_kwargs is not None:
            self.sampling_kwargs = sampling_kwargs
        if excluded_tables is not None:
            self.excluded_tables = excluded_tables
        if included_tables is not None:
            self.included_tables = included_tables
        if skip_inapplicable_tables is not None:
            self.skip_inapplicable_tables = skip_inapplicable_tables
        if introspection_directives is not None:
            self.introspection_directives = introspection_directives
        if batch_spec_passthrough is not None:
            self.batch_spec_passthrough = batch_spec_passthrough

        # S3
        if boto3_options is not None:
            self.boto3_options = boto3_options
        if bucket is not None:
            self.bucket = bucket
        if max_keys is not None:
            self.max_keys = max_keys

        # Azure
        if azure_options is not None:
            self.azure_options = azure_options
        if container is not None:
            self.container = container
        if name_starts_with is not None:
            self.name_starts_with = name_starts_with

        # GCS
        if bucket_or_name is not None:
            self.bucket_or_name = bucket_or_name
        if max_results is not None:
            self.max_results = max_results

        # Both S3/GCS
        if prefix is not None:
            self.prefix = prefix

        # Both S3/Azure
        if delimiter is not None:
            self.delimiter = delimiter

        super().__init__(id=id, name=name)

        # Note: optional samplers and partitioners are handled by setattr
        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def class_name(self):
        return self._class_name

    @property
    def module_name(self):
        return self._module_name

    @override
    def to_json_dict(self) -> Dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this DataConnectorConfig.

        Returns:
            A JSON-serializable dict representation of this DataConnectorConfig.
        """
        # # TODO: <Alex>2/4/2022</Alex>
        # This implementation of "SerializableDictDot.to_json_dict() occurs frequently and should ideally serve as the  # noqa: E501
        # reference implementation in the "SerializableDictDot" class itself.  However, the circular import dependencies,  # noqa: E501
        # due to the location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules  # noqa: E501
        # make this refactoring infeasible at the present time.
        dict_obj: dict = self.to_dict()
        serializeable_dict: dict = convert_to_json_serializable(data=dict_obj)
        return serializeable_dict


class DataConnectorConfigSchema(AbstractConfigSchema):
    class Meta:
        unknown = INCLUDE

    name = fields.String(
        required=False,
        allow_none=True,
    )

    id = fields.String(
        required=False,
        allow_none=True,
    )

    class_name = fields.String(
        required=True,
        allow_none=False,
    )
    module_name = fields.String(
        required=False,
        allow_none=True,
        missing="great_expectations.datasource.data_connector",
    )

    assets = fields.Dict(
        keys=fields.Str(),
        values=fields.Nested(AssetConfigSchema, required=False, allow_none=True),
        required=False,
        allow_none=True,
    )

    base_directory = fields.String(required=False, allow_none=True)
    glob_directive = fields.String(required=False, allow_none=True)
    default_regex = fields.Dict(required=False, allow_none=True)
    credentials = fields.Raw(required=False, allow_none=True)
    batch_identifiers = fields.List(cls_or_instance=fields.Str(), required=False, allow_none=True)

    # S3
    boto3_options = fields.Dict(
        keys=fields.Str(), values=fields.Str(), required=False, allow_none=True
    )
    bucket = fields.String(required=False, allow_none=True)
    max_keys = fields.Integer(required=False, allow_none=True)

    # Azure
    azure_options = fields.Dict(
        keys=fields.Str(), values=fields.Str(), required=False, allow_none=True
    )
    container = fields.String(required=False, allow_none=True)
    name_starts_with = fields.String(required=False, allow_none=True)

    # GCS
    gcs_options = fields.Dict(
        keys=fields.Str(), values=fields.Str(), required=False, allow_none=True
    )
    bucket_or_name = fields.String(required=False, allow_none=True)
    max_results = fields.String(required=False, allow_none=True)

    # Both S3/GCS
    prefix = fields.String(required=False, allow_none=True)

    # Both S3/Azure
    delimiter = fields.String(required=False, allow_none=True)

    data_asset_name_prefix = fields.String(required=False, allow_none=True)
    data_asset_name_suffix = fields.String(required=False, allow_none=True)
    include_schema_name = fields.Boolean(required=False, allow_none=True)
    partitioner_method = fields.String(required=False, allow_none=True)
    partitioner_kwargs = fields.Dict(required=False, allow_none=True)
    sorters = fields.List(
        cls_or_instance=fields.Nested(SorterConfigSchema, required=False, allow_none=True),
        required=False,
        allow_none=True,
    )
    sampling_method = fields.String(required=False, allow_none=True)
    sampling_kwargs = fields.Dict(required=False, allow_none=True)

    excluded_tables = fields.List(cls_or_instance=fields.Str(), required=False, allow_none=True)
    included_tables = fields.List(cls_or_instance=fields.Str(), required=False, allow_none=True)
    skip_inapplicable_tables = fields.Boolean(required=False, allow_none=True)
    introspection_directives = fields.Dict(required=False, allow_none=True)
    batch_spec_passthrough = fields.Dict(required=False, allow_none=True)

    # AWS Glue Data Catalog
    glue_introspection_directives = fields.Dict(required=False, allow_none=True)
    catalog_id = fields.String(required=False, allow_none=True)
    partitions = fields.List(cls_or_instance=fields.Str(), required=False, allow_none=True)

    # noinspection PyUnusedLocal
    @validates_schema
    def validate_schema(self, data, **kwargs):  # noqa: C901, PLR0912
        # If a class_name begins with the dollar sign ("$"), then it is assumed to be a variable name to be substituted.  # noqa: E501
        if data["class_name"][0] == "$":
            return
        if ("default_regex" in data) and not (
            data["class_name"]  # noqa: E713 # membership check
            in [
                "InferredAssetFilesystemDataConnector",
                "ConfiguredAssetFilesystemDataConnector",
                "InferredAssetS3DataConnector",
                "ConfiguredAssetS3DataConnector",
                "InferredAssetAzureDataConnector",
                "ConfiguredAssetAzureDataConnector",
                "InferredAssetGCSDataConnector",
                "ConfiguredAssetGCSDataConnector",
                "InferredAssetDBFSDataConnector",
                "ConfiguredAssetDBFSDataConnector",
            ]
        ):
            raise gx_exceptions.InvalidConfigError(  # noqa: TRY003
                f"""Your current configuration uses one or more keys in a data connector that are required only by a
subclass of the FilePathDataConnector class (your data connector is "{data['class_name']}").  Please update your
configuration to continue.
                """  # noqa: E501
            )
        if ("glob_directive" in data) and not (
            data["class_name"]  # noqa: E713 # membership check
            in [
                "InferredAssetFilesystemDataConnector",
                "ConfiguredAssetFilesystemDataConnector",
                "InferredAssetDBFSDataConnector",
                "ConfiguredAssetDBFSDataConnector",
            ]
        ):
            raise gx_exceptions.InvalidConfigError(  # noqa: TRY003
                f"""Your current configuration uses one or more keys in a data connector that are required only by a
filesystem type of the data connector (your data connector is "{data['class_name']}").  Please update your
configuration to continue.
                """  # noqa: E501
            )
        if ("delimiter" in data) and not (
            data["class_name"]  # noqa: E713 # membership check
            in [
                "InferredAssetS3DataConnector",
                "ConfiguredAssetS3DataConnector",
                "InferredAssetAzureDataConnector",
                "ConfiguredAssetAzureDataConnector",
                "InferredAssetGCSDataConnector",
                "ConfiguredAssetGCSDataConnector",
            ]
        ):
            raise gx_exceptions.InvalidConfigError(  # noqa: TRY003
                f"""Your current configuration uses one or more keys in a data connector that are required only by an
S3/Azure/GCS type of the data connector (your data connector is "{data['class_name']}").  Please update your configuration \
to continue.
"""  # noqa: E501
            )
        if ("prefix" in data) and not (
            data["class_name"]  # noqa: E713 # membership check
            in [
                "InferredAssetS3DataConnector",
                "ConfiguredAssetS3DataConnector",
                "InferredAssetGCSDataConnector",
                "ConfiguredAssetGCSDataConnector",
            ]
        ):
            raise gx_exceptions.InvalidConfigError(  # noqa: TRY003
                f"""Your current configuration uses one or more keys in a data connector that are required only by an
S3/GCS type of the data connector (your data connector is "{data['class_name']}").  Please update your configuration to
continue.
                """  # noqa: E501
            )
        if ("bucket" in data or "max_keys" in data) and not (
            data["class_name"]  # noqa: E713 # membership check
            in [
                "InferredAssetS3DataConnector",
                "ConfiguredAssetS3DataConnector",
            ]
        ):
            raise gx_exceptions.InvalidConfigError(  # noqa: TRY003
                f"""Your current configuration uses one or more keys in a data connector that are required only by an
S3 type of the data connector (your data connector is "{data['class_name']}").  Please update your configuration to
continue.
                """  # noqa: E501
            )
        if ("azure_options" in data or "container" in data or "name_starts_with" in data) and not (
            data["class_name"]  # noqa: E713 # membership check
            in [
                "InferredAssetAzureDataConnector",
                "ConfiguredAssetAzureDataConnector",
            ]
        ):
            raise gx_exceptions.InvalidConfigError(  # noqa: TRY003
                f"""Your current configuration uses one or more keys in a data connector that are required only by an
Azure type of the data connector (your data connector is "{data['class_name']}").  Please update your configuration to
continue.
                    """  # noqa: E501
            )
        if "azure_options" in data and data["class_name"] in [
            "InferredAssetAzureDataConnector",
            "ConfiguredAssetAzureDataConnector",
        ]:
            azure_options = data["azure_options"]
            if not (("conn_str" in azure_options) ^ ("account_url" in azure_options)):
                raise gx_exceptions.InvalidConfigError(  # noqa: TRY003
                    """Your current configuration is either missing methods of authentication or is using too many for \
the Azure type of data connector. You must only select one between `conn_str` or `account_url`. Please update your \
configuration to continue.
"""  # noqa: E501
                )
        if ("gcs_options" in data or "bucket_or_name" in data or "max_results" in data) and not (
            data["class_name"]  # noqa: E713 # membership check
            in [
                "InferredAssetGCSDataConnector",
                "ConfiguredAssetGCSDataConnector",
            ]
        ):
            raise gx_exceptions.InvalidConfigError(  # noqa: TRY003
                f"""Your current configuration uses one or more keys in a data connector that are required only by a
GCS type of the data connector (your data connector is "{data['class_name']}").  Please update your configuration to
continue.
                    """  # noqa: E501
            )
        if "gcs_options" in data and data["class_name"] in [
            "InferredAssetGCSDataConnector",
            "ConfiguredAssetGCSDataConnector",
        ]:
            gcs_options = data["gcs_options"]
            if "filename" in gcs_options and "info" in gcs_options:
                raise gx_exceptions.InvalidConfigError(  # noqa: TRY003
                    """Your current configuration can only use a single method of authentication for the GCS type of \
data connector. You must only select one between `filename` (from_service_account_file) and `info` \
(from_service_account_info). Please update your configuration to continue.
"""  # noqa: E501
                )
        if (
            "include_schema_name" in data
            or "partitioner_method" in data
            or "partitioner_kwargs" in data
            or "sampling_method" in data
            or "sampling_kwargs" in data
            or "skip_inapplicable_tables" in data
        ) and not (
            data["class_name"]  # noqa: E713 # membership check
            in [
                "InferredAssetSqlDataConnector",
                "ConfiguredAssetSqlDataConnector",
            ]
        ):
            raise gx_exceptions.InvalidConfigError(  # noqa: TRY003
                f"""Your current configuration uses one or more keys in a data connector that are required only by an
SQL type of the data connector (your data connector is "{data['class_name']}").  Please update your configuration to
continue.
                """  # noqa: E501
            )
        if (
            "data_asset_name_prefix" in data
            or "data_asset_name_suffix" in data
            or "excluded_tables" in data
            or "included_tables" in data
        ) and not (
            data["class_name"]  # noqa: E713 # membership check
            in [
                "InferredAssetSqlDataConnector",
                "ConfiguredAssetSqlDataConnector",
                "InferredAssetAWSGlueDataCatalogDataConnector",
                "ConfiguredAssetAWSGlueDataCatalogDataConnector",
            ]
        ):
            raise gx_exceptions.InvalidConfigError(  # noqa: TRY003
                f"""Your current configuration uses one or more keys in a data connector that are required only by an
SQL/GlueCatalog type of the data connector (your data connector is "{data['class_name']}").  Please update your configuration to
continue.
                """  # noqa: E501
            )

        if (
            "partitions" in data or "catalog_id" in data or "glue_introspection_directives" in data
        ) and not (
            data["class_name"]  # noqa: E713 # membership check
            in [
                "InferredAssetAWSGlueDataCatalogDataConnector",
                "ConfiguredAssetAWSGlueDataCatalogDataConnector",
            ]
        ):
            raise gx_exceptions.InvalidConfigError(  # noqa: TRY003
                f"""Your current configuration uses one or more keys in a data connector that are required only by an
GlueCatalog type of the data connector (your data connector is "{data['class_name']}").  Please update your configuration to
continue.
                """  # noqa: E501
            )

    # noinspection PyUnusedLocal
    @post_load
    def make_data_connector_config(self, data, **kwargs):
        return DataConnectorConfig(**data)

    @pre_dump
    def prepare_dump(self, data, **kwargs):
        """
        Schemas in Spark Dataframes are defined as StructType, which is not serializable
        This method calls the schema's jsonValue() method, which translates the object into a json
        """
        # check whether spark exists
        try:
            if (not pyspark.types) or (pyspark.types.StructType is None):
                return data
        except AttributeError:
            return data

        batch_spec_passthrough_config = data.get("batch_spec_passthrough")
        if batch_spec_passthrough_config:
            reader_options: dict = batch_spec_passthrough_config.get("reader_options")
            if reader_options:
                schema = reader_options.get("schema")
                if schema and pyspark.types and isinstance(schema, pyspark.types.StructType):
                    data["batch_spec_passthrough"]["reader_options"]["schema"] = schema.jsonValue()
        return data


class ExecutionEngineConfig(DictDot):
    def __init__(  # noqa: C901, PLR0913
        self,
        class_name,
        module_name=None,
        caching=None,
        batch_spec_defaults=None,
        connection_string=None,
        credentials=None,
        spark_config=None,
        boto3_options=None,
        azure_options=None,
        gcs_options=None,
        credentials_info=None,
        connect_args=None,
        **kwargs,
    ) -> None:
        self._class_name = class_name
        self._module_name = module_name
        if caching is not None:
            self.caching = caching
        if batch_spec_defaults is not None:
            self._batch_spec_defaults = batch_spec_defaults
        if connection_string is not None:
            self.connection_string = connection_string
        if credentials is not None:
            self.credentials = credentials
        if spark_config is not None:
            self.spark_config = spark_config
        if boto3_options is not None:
            self.boto3_options = boto3_options
        if azure_options is not None:
            self.azure_options = azure_options
        if gcs_options is not None:
            self.gcs_options = gcs_options
        if credentials_info is not None:
            self.credentials_info = credentials_info
        if connect_args is not None:
            self.connect_args = connect_args
        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def module_name(self):
        return self._module_name

    @property
    def class_name(self):
        return self._class_name

    @property
    def batch_spec_defaults(self):
        return self._batch_spec_defaults


class ExecutionEngineConfigSchema(Schema):
    class Meta:
        unknown = INCLUDE

    class_name = fields.String(
        required=True,
        allow_none=False,
    )
    module_name = fields.String(
        required=False,
        allow_none=True,
        missing="great_expectations.execution_engine",
    )
    connection_string = fields.String(required=False, allow_none=True)
    credentials = fields.Raw(required=False, allow_none=True)
    spark_config = fields.Raw(required=False, allow_none=True)
    boto3_options = fields.Dict(
        keys=fields.Str(), values=fields.Str(), required=False, allow_none=True
    )
    connect_args = fields.Dict(
        keys=fields.Str(), values=fields.Raw(), required=False, allow_none=True
    )
    azure_options = fields.Dict(
        keys=fields.Str(), values=fields.Str(), required=False, allow_none=True
    )
    gcs_options = fields.Dict(
        keys=fields.Str(), values=fields.Str(), required=False, allow_none=True
    )
    caching = fields.Boolean(required=False, allow_none=True)
    batch_spec_defaults = fields.Dict(required=False, allow_none=True)
    force_reuse_spark_context = fields.Boolean(required=False, allow_none=True)
    persist = fields.Boolean(required=False, allow_none=True)
    # BigQuery Service Account Credentials
    # https://googleapis.dev/python/sqlalchemy-bigquery/latest/README.html#connection-string-parameters
    credentials_info = fields.Dict(required=False, allow_none=True)

    create_temp_table = fields.Boolean(required=False, allow_none=True)

    # noinspection PyUnusedLocal
    @validates_schema
    def validate_schema(self, data, **kwargs):
        # If a class_name begins with the dollar sign ("$"), then it is assumed to be a variable name to be substituted.  # noqa: E501
        if data["class_name"][0] == "$":
            return
        if ("connection_string" in data or "credentials" in data) and not (
            data["class_name"] == "SqlAlchemyExecutionEngine"
        ):
            raise gx_exceptions.InvalidConfigError(  # noqa: TRY003
                f"""Your current configuration uses the "connection_string" key in an execution engine, but only
SqlAlchemyExecutionEngine requires this attribute (your execution engine is "{data['class_name']}").  Please update your
configuration to continue.
                """  # noqa: E501
            )
        if "spark_config" in data and not (data["class_name"] == "SparkDFExecutionEngine"):
            raise gx_exceptions.InvalidConfigError(  # noqa: TRY003
                f"""Your current configuration uses the "spark_config" key in an execution engine, but only
SparkDFExecutionEngine requires this attribute (your execution engine is "{data['class_name']}").  Please update your
configuration to continue.
                """  # noqa: E501
            )

    # noinspection PyUnusedLocal
    @post_load
    def make_execution_engine_config(self, data, **kwargs):
        return ExecutionEngineConfig(**data)


class ProgressBarsConfig(DictDot):
    def __init__(
        self,
        globally: Optional[bool] = None,
        metric_calculations: Optional[bool] = None,
    ) -> None:
        self.globally: bool = True if globally is None else globally
        self.metric_calculations: bool = (
            metric_calculations if metric_calculations is not None else self.globally
        )


class ProgressBarsConfigSchema(Schema):
    globally = fields.Boolean()
    metric_calculations = fields.Boolean()


class GXCloudConfig(DictDot):
    def __init__(
        self,
        base_url: str,
        access_token: Optional[str] = None,
        organization_id: Optional[str] = None,
    ) -> None:
        # access_token was given a default value to maintain arg position of organization_id
        if access_token is None:
            raise ValueError("Access token cannot be None.")  # noqa: TRY003

        # The base url doesn't point to a specific resource but is the prefix for constructing GX
        # cloud urls. We want it to end in a '/' so we can manipulate it using tools such as
        # urllib.parse.urljoin. `urljoin` will strip the last part of the path if it thinks it is
        # a specific resource  (ie not trailing /). So we append a '/' to the base_url if it
        # doesn't exist.
        self.base_url = base_url if base_url[-1] == "/" else base_url + "/"
        self.organization_id = organization_id
        self.access_token = access_token

    def to_json_dict(self) -> Dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this GXCloudConfig.

        Returns:
            A JSON-serializable dict representation of this GXCloudConfig.
        """
        # postpone importing to avoid circular imports
        from great_expectations.data_context.util import PasswordMasker

        return {
            "base_url": self.base_url,
            "organization_id": self.organization_id,
            "access_token": PasswordMasker.MASKED_PASSWORD_STRING,
        }


class DataContextConfigSchema(Schema):
    config_version = fields.Number(
        validate=lambda x: 0 < x < 100,  # noqa: PLR2004
        error_messages={"invalid": "config version must " "be a number."},
    )
    fluent_datasources = fields.Dict(
        keys=fields.Str(),
        required=False,
        allow_none=True,
        load_only=True,
    )
    expectations_store_name = fields.Str()
    validation_results_store_name = fields.Str()
    checkpoint_store_name = fields.Str(required=False, allow_none=True)
    plugins_directory = fields.Str(allow_none=True)
    stores = fields.Dict(keys=fields.Str(), values=fields.Dict())
    data_docs_sites = fields.Dict(keys=fields.Str(), values=fields.Dict(), allow_none=True)
    config_variables_file_path = fields.Str(allow_none=True)
    analytics_enabled = fields.Boolean(allow_none=True)
    data_context_id = fields.UUID(allow_none=True)
    progress_bars = fields.Nested(ProgressBarsConfigSchema, required=False, allow_none=True)

    # To ensure backwards compatability, we need to ensure that new options are "opt-in"
    # If a user has not explicitly configured the value, it will be None and will be wiped by the post_dump hook  # noqa: E501
    REMOVE_KEYS_IF_NONE = [
        "progress_bars",  # 0.13.49
        "fluent_datasources",
    ]

    # noinspection PyUnusedLocal
    @post_dump
    def remove_keys_if_none(self, data: dict, **kwargs) -> dict:
        data = copy.deepcopy(data)
        for key in self.REMOVE_KEYS_IF_NONE:
            if key in data and data[key] is None:
                data.pop(key)
        return data

    @override
    def handle_error(self, exc, data, **kwargs) -> None:  # type: ignore[override]
        """Log and raise our custom exception when (de)serialization fails."""
        if (
            exc
            and exc.messages
            and isinstance(exc.messages, dict)
            and all(key is None for key in exc.messages)
        ):
            exc.messages = list(itertools.chain.from_iterable(exc.messages.values()))

        message: str = f"Error while processing DataContextConfig: {' '.join(exc.messages)}"
        logger.error(message)
        raise gx_exceptions.InvalidDataContextConfigError(
            message=message,
        )

    # noinspection PyUnusedLocal
    @validates_schema
    def validate_schema(self, data, **kwargs) -> None:
        if "config_version" not in data:
            raise gx_exceptions.InvalidDataContextConfigError(  # noqa: TRY003
                "The key `config_version` is missing; please check your config file.",
                validation_error=ValidationError(message="no config_version key"),
            )

        if not isinstance(data["config_version"], (int, float)):
            raise gx_exceptions.InvalidDataContextConfigError(  # noqa: TRY003
                "The key `config_version` must be a number. Please check your config file.",
                validation_error=ValidationError(message="config version not a number"),
            )

        # When migrating from 0.7.x to 0.8.0
        if data["config_version"] == 0 and any(
            store_config["class_name"] == "ValidationResultsStore"
            for store_config in data["stores"].values()
        ):
            raise gx_exceptions.UnsupportedConfigVersionError(  # noqa: TRY003
                "You appear to be using a config version from the 0.7.x series. This version is no longer supported."  # noqa: E501
            )

        if data["config_version"] < MINIMUM_SUPPORTED_CONFIG_VERSION:
            raise gx_exceptions.UnsupportedConfigVersionError(
                "You appear to have an invalid config version ({}).\n    The version number must be at least {}. "  # noqa: E501
                "Please see the migration guide at https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api".format(
                    data["config_version"], MINIMUM_SUPPORTED_CONFIG_VERSION
                ),
            )

        if data["config_version"] > CURRENT_GX_CONFIG_VERSION:
            raise gx_exceptions.InvalidDataContextConfigError(
                "You appear to have an invalid config version ({}).\n    The maximum valid version is {}.".format(  # noqa: E501
                    data["config_version"], CURRENT_GX_CONFIG_VERSION
                ),
                validation_error=ValidationError(message="config version too high"),
            )


class DataContextConfigDefaults(enum.Enum):
    DEFAULT_CONFIG_VERSION = CURRENT_GX_CONFIG_VERSION
    UNCOMMITTED = "uncommitted"

    DEFAULT_EXPECTATIONS_STORE_NAME = "expectations_store"
    EXPECTATIONS_BASE_DIRECTORY = "expectations"
    DEFAULT_EXPECTATIONS_STORE_BASE_DIRECTORY_RELATIVE_NAME = f"{EXPECTATIONS_BASE_DIRECTORY}/"
    DEFAULT_VALIDATIONS_STORE_NAME = "validation_results_store"
    VALIDATIONS_BASE_DIRECTORY = "validations"
    DEFAULT_VALIDATIONS_STORE_BASE_DIRECTORY_RELATIVE_NAME = (
        f"{UNCOMMITTED}/{VALIDATIONS_BASE_DIRECTORY}/"
    )
    DEFAULT_VALIDATION_DEFINITION_STORE_NAME = "validation_definition_store"
    VALIDATION_DEFINITIONS_BASE_DIRECTORY = "validation_definitions"
    DEFAULT_VALIDATION_DEFINITION_STORE_BASE_DIRECTORY_RELATIVE_NAME = (
        f"{VALIDATION_DEFINITIONS_BASE_DIRECTORY}/"
    )

    DATA_DOCS_BASE_DIRECTORY = "data_docs"
    DEFAULT_DATA_DOCS_BASE_DIRECTORY_RELATIVE_NAME = f"{UNCOMMITTED}/{DATA_DOCS_BASE_DIRECTORY}"

    # Datasource
    DEFAULT_DATASOURCE_STORE_NAME = "datasource_store"

    # DataAsset
    DEFAULT_DATA_ASSET_STORE_NAME = "data_asset_store"

    # Checkpoints
    DEFAULT_CHECKPOINT_STORE_NAME = "checkpoint_store"
    CHECKPOINTS_BASE_DIRECTORY = "checkpoints"
    DEFAULT_CHECKPOINT_STORE_BASE_DIRECTORY_RELATIVE_NAME = f"{CHECKPOINTS_BASE_DIRECTORY}/"

    DEFAULT_DATA_DOCS_SITE_NAME = "local_site"
    DEFAULT_CONFIG_VARIABLES_FILEPATH = f"{UNCOMMITTED}/config_variables.yml"
    PLUGINS_BASE_DIRECTORY = "plugins"
    DEFAULT_PLUGINS_DIRECTORY = f"{PLUGINS_BASE_DIRECTORY}/"
    DEFAULT_ACTION_LIST = [
        {
            "name": "store_validation_result",
            "action": {"class_name": "StoreValidationResultAction"},
        },
        {
            "name": "update_data_docs",
            "action": {"class_name": "UpdateDataDocsAction"},
        },
    ]
    DEFAULT_EXPECTATIONS_STORE = {
        "class_name": "ExpectationsStore",
        "store_backend": {
            "class_name": "TupleFilesystemStoreBackend",
            "base_directory": DEFAULT_EXPECTATIONS_STORE_BASE_DIRECTORY_RELATIVE_NAME,
        },
    }
    DEFAULT_VALIDATIONS_STORE = {
        "class_name": "ValidationResultsStore",
        "store_backend": {
            "class_name": "TupleFilesystemStoreBackend",
            "base_directory": DEFAULT_VALIDATIONS_STORE_BASE_DIRECTORY_RELATIVE_NAME,
        },
    }
    DEFAULT_VALIDATION_DEFINITION_STORE = {
        "class_name": "ValidationDefinitionStore",
        "store_backend": {
            "class_name": "TupleFilesystemStoreBackend",
            "base_directory": DEFAULT_VALIDATION_DEFINITION_STORE_BASE_DIRECTORY_RELATIVE_NAME,
        },
    }
    DEFAULT_CHECKPOINT_STORE = {
        "class_name": "CheckpointStore",
        "store_backend": {
            "class_name": "TupleFilesystemStoreBackend",
            "suppress_store_backend_id": True,
            "base_directory": DEFAULT_CHECKPOINT_STORE_BASE_DIRECTORY_RELATIVE_NAME,
        },
    }
    DEFAULT_STORES = {
        DEFAULT_EXPECTATIONS_STORE_NAME: DEFAULT_EXPECTATIONS_STORE,
        DEFAULT_VALIDATIONS_STORE_NAME: DEFAULT_VALIDATIONS_STORE,
        DEFAULT_VALIDATION_DEFINITION_STORE_NAME: DEFAULT_VALIDATION_DEFINITION_STORE,
        DEFAULT_CHECKPOINT_STORE_NAME: DEFAULT_CHECKPOINT_STORE,
    }

    DEFAULT_DATA_DOCS_SITES = {
        DEFAULT_DATA_DOCS_SITE_NAME: {
            "class_name": "SiteBuilder",
            "show_how_to_buttons": True,
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": f"{DEFAULT_DATA_DOCS_BASE_DIRECTORY_RELATIVE_NAME}/local_site/",
            },
            "site_index_builder": {
                "class_name": "DefaultSiteIndexBuilder",
            },
        }
    }


class BaseStoreBackendDefaults(DictDot):
    """
    Define base defaults for platform specific StoreBackendDefaults.
    StoreBackendDefaults define defaults for specific cases of often used configurations.
    For example, if you plan to store expectations, validations, and data_docs in s3 use the S3StoreBackendDefaults and you may be able to specify less parameters.
    """  # noqa: E501

    def __init__(  # noqa: PLR0913
        self,
        expectations_store_name: str = DataContextConfigDefaults.DEFAULT_EXPECTATIONS_STORE_NAME.value,  # noqa: E501
        validation_results_store_name: str = DataContextConfigDefaults.DEFAULT_VALIDATIONS_STORE_NAME.value,  # noqa: E501
        checkpoint_store_name: str = DataContextConfigDefaults.DEFAULT_CHECKPOINT_STORE_NAME.value,
        data_docs_site_name: str = DataContextConfigDefaults.DEFAULT_DATA_DOCS_SITE_NAME.value,
        stores: Optional[dict] = None,
        data_docs_sites: Optional[dict] = None,
    ) -> None:
        self.expectations_store_name = expectations_store_name
        self.validation_results_store_name = validation_results_store_name
        self.checkpoint_store_name = checkpoint_store_name
        self.validation_definition_store_name = (
            DataContextConfigDefaults.DEFAULT_VALIDATION_DEFINITION_STORE_NAME.value
        )
        if stores is None:
            stores = copy.deepcopy(DataContextConfigDefaults.DEFAULT_STORES.value)

        self.stores = stores
        if data_docs_sites is None:
            data_docs_sites = copy.deepcopy(DataContextConfigDefaults.DEFAULT_DATA_DOCS_SITES.value)

        self.data_docs_sites = data_docs_sites
        self.data_docs_site_name = data_docs_site_name


@public_api
class S3StoreBackendDefaults(BaseStoreBackendDefaults):
    """Default store configs for s3 backends, with some accessible parameters.

    Args:
        default_bucket_name: Use this bucket name for stores that do not have a bucket name provided
        expectations_store_bucket_name: Overrides default_bucket_name if supplied
        validation_results_store_bucket_name: Overrides default_bucket_name if supplied
        data_docs_bucket_name: Overrides default_bucket_name if supplied
        checkpoint_store_bucket_name: Overrides default_bucket_name if supplied
        expectations_store_prefix: Overrides default if supplied
        validation_results_store_prefix: Overrides default if supplied
        data_docs_prefix: Overrides default if supplied
        checkpoint_store_prefix: Overrides default if supplied
        expectations_store_name: Overrides default if supplied
        validation_results_store_name: Overrides default if supplied
        checkpoint_store_name: Overrides default if supplied
    """

    def __init__(  # noqa: PLR0913
        self,
        default_bucket_name: Optional[str] = None,
        expectations_store_bucket_name: Optional[str] = None,
        validation_results_store_bucket_name: Optional[str] = None,
        validation_definition_store_bucket_name: Optional[str] = None,
        data_docs_bucket_name: Optional[str] = None,
        checkpoint_store_bucket_name: Optional[str] = None,
        expectations_store_prefix: str = "expectations",
        validation_results_store_prefix: str = "validations",
        validation_definition_store_prefix: str = "validation_definitions",
        data_docs_prefix: str = "data_docs",
        checkpoint_store_prefix: str = "checkpoints",
        expectations_store_name: str = "expectations_S3_store",
        validation_results_store_name: str = "validation_results_S3_store",
        checkpoint_store_name: str = "checkpoint_S3_store",
    ) -> None:
        # Initialize base defaults
        super().__init__()

        # Use default_bucket_name if separate store buckets are not provided
        if expectations_store_bucket_name is None:
            expectations_store_bucket_name = default_bucket_name
        if validation_results_store_bucket_name is None:
            validation_results_store_bucket_name = default_bucket_name
        if validation_definition_store_bucket_name is None:
            validation_definition_store_bucket_name = default_bucket_name
        if data_docs_bucket_name is None:
            data_docs_bucket_name = default_bucket_name
        if checkpoint_store_bucket_name is None:
            checkpoint_store_bucket_name = default_bucket_name

        # Overwrite defaults
        self.expectations_store_name = expectations_store_name
        self.validation_results_store_name = validation_results_store_name
        self.checkpoint_store_name = checkpoint_store_name
        self.stores = {
            expectations_store_name: {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": expectations_store_bucket_name,
                    "prefix": expectations_store_prefix,
                },
            },
            validation_results_store_name: {
                "class_name": "ValidationResultsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": validation_results_store_bucket_name,
                    "prefix": validation_results_store_prefix,
                },
            },
            self.validation_definition_store_name: {
                "class_name": "ValidationDefinitionStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": validation_definition_store_bucket_name,
                    "prefix": validation_definition_store_prefix,
                },
            },
            checkpoint_store_name: {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": checkpoint_store_bucket_name,
                    "prefix": checkpoint_store_prefix,
                },
            },
        }
        self.data_docs_sites = {
            "s3_site": {
                "class_name": "SiteBuilder",
                "show_how_to_buttons": True,
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": data_docs_bucket_name,
                    "prefix": data_docs_prefix,
                },
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder",
                },
            }
        }


@public_api
class FilesystemStoreBackendDefaults(BaseStoreBackendDefaults):
    """Default store configs for filesystem backends, with some accessible parameters.

    Args:
        root_directory: Absolute directory prepended to the base_directory for each store
        plugins_directory: Overrides default if supplied
    """

    def __init__(
        self,
        root_directory: Optional[PathStr] = None,
        plugins_directory: Optional[PathStr] = None,
    ) -> None:
        # Initialize base defaults
        super().__init__()

        if plugins_directory is None:
            plugins_directory = DataContextConfigDefaults.DEFAULT_PLUGINS_DIRECTORY.value
        self.plugins_directory = str(plugins_directory)
        if root_directory is not None:
            root_directory = str(root_directory)
            self.stores[self.expectations_store_name]["store_backend"][  # type: ignore[index]
                "root_directory"
            ] = root_directory
            self.stores[self.validation_results_store_name]["store_backend"][  # type: ignore[index]
                "root_directory"
            ] = root_directory
            self.stores[self.checkpoint_store_name]["store_backend"][  # type: ignore[index]
                "root_directory"
            ] = root_directory
            self.stores[self.validation_definition_store_name]["store_backend"][  # type: ignore[index]
                "root_directory"
            ] = root_directory
            self.data_docs_sites[self.data_docs_site_name]["store_backend"][  # type: ignore[index]
                "root_directory"
            ] = root_directory


@public_api
class InMemoryStoreBackendDefaults(BaseStoreBackendDefaults):
    """Default store configs for in memory backends.

    This is useful for testing without persistence.
    """

    def __init__(
        self,
        init_temp_docs_sites: bool = False,
    ) -> None:
        # Initialize base defaults
        super().__init__()

        self.stores = {
            self.expectations_store_name: {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "InMemoryStoreBackend",
                },
            },
            self.validation_results_store_name: {
                "class_name": "ValidationResultsStore",
                "store_backend": {
                    "class_name": "InMemoryStoreBackend",
                },
            },
            self.checkpoint_store_name: {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "InMemoryStoreBackend",
                },
            },
            self.validation_definition_store_name: {
                "class_name": "ValidationDefinitionStore",
                "store_backend": {
                    "class_name": "InMemoryStoreBackend",
                },
            },
        }
        if init_temp_docs_sites:
            temp_dir = tempfile.TemporaryDirectory()
            path = temp_dir.name
            logger.info(f"Created temporary directory '{path}' for ephemeral docs site")
            self.data_docs_sites[DataContextConfigDefaults.DEFAULT_DATA_DOCS_SITE_NAME.value][  # type: ignore[index]
                "store_backend"
            ]["base_directory"] = path
        else:
            self.data_docs_sites = {}


class GCSStoreBackendDefaults(BaseStoreBackendDefaults):
    """
    Default store configs for Google Cloud Storage (GCS) backends, with some accessible parameters
    Args:
        default_bucket_name: Use this bucket name for stores that do not have a bucket name provided
        default_project_name: Use this project name for stores that do not have a project name provided
        expectations_store_bucket_name: Overrides default_bucket_name if supplied
        validation_results_store_bucket_name: Overrides default_bucket_name if supplied
        data_docs_bucket_name: Overrides default_bucket_name if supplied
        checkpoint_store_bucket_name: Overrides default_bucket_name if supplied
        expectations_store_project_name: Overrides default_project_name if supplied
        validation_results_store_project_name: Overrides default_project_name if supplied
        data_docs_project_name: Overrides default_project_name if supplied
        checkpoint_store_project_name: Overrides default_project_name if supplied
        expectations_store_prefix: Overrides default if supplied
        validation_results_store_prefix: Overrides default if supplied
        data_docs_prefix: Overrides default if supplied
        checkpoint_store_prefix: Overrides default if supplied
        expectations_store_name: Overrides default if supplied
        validation_results_store_name: Overrides default if supplied
        checkpoint_store_name: Overrides default if supplied
    """  # noqa: E501

    def __init__(  # noqa: C901, PLR0913
        self,
        default_bucket_name: Optional[str] = None,
        default_project_name: Optional[str] = None,
        expectations_store_bucket_name: Optional[str] = None,
        validation_results_store_bucket_name: Optional[str] = None,
        validation_definition_store_bucket_name: Optional[str] = None,
        data_docs_bucket_name: Optional[str] = None,
        checkpoint_store_bucket_name: Optional[str] = None,
        expectations_store_project_name: Optional[str] = None,
        validation_results_store_project_name: Optional[str] = None,
        validation_definition_store_project_name: Optional[str] = None,
        data_docs_project_name: Optional[str] = None,
        checkpoint_store_project_name: Optional[str] = None,
        expectations_store_prefix: str = "expectations",
        validation_results_store_prefix: str = "validations",
        validation_definition_store_prefix: str = "validation_definitions",
        data_docs_prefix: str = "data_docs",
        checkpoint_store_prefix: str = "checkpoints",
        expectations_store_name: str = "expectations_GCS_store",
        validation_results_store_name: str = "validation_results_GCS_store",
        checkpoint_store_name: str = "checkpoint_GCS_store",
    ) -> None:
        # Initialize base defaults
        super().__init__()

        # Use default_bucket_name if separate store buckets are not provided
        if expectations_store_bucket_name is None:
            expectations_store_bucket_name = default_bucket_name
        if validation_results_store_bucket_name is None:
            validation_results_store_bucket_name = default_bucket_name
        if validation_definition_store_bucket_name is None:
            validation_definition_store_bucket_name = default_bucket_name
        if data_docs_bucket_name is None:
            data_docs_bucket_name = default_bucket_name
        if checkpoint_store_bucket_name is None:
            checkpoint_store_bucket_name = default_bucket_name

        # Use default_project_name if separate store projects are not provided
        if expectations_store_project_name is None:
            expectations_store_project_name = default_project_name
        if validation_results_store_project_name is None:
            validation_results_store_project_name = default_project_name
        if validation_definition_store_project_name is None:
            validation_definition_store_project_name = default_project_name
        if data_docs_project_name is None:
            data_docs_project_name = default_project_name
        if checkpoint_store_project_name is None:
            checkpoint_store_project_name = default_project_name

        # Overwrite defaults
        self.expectations_store_name = expectations_store_name
        self.validation_results_store_name = validation_results_store_name
        self.checkpoint_store_name = checkpoint_store_name
        self.stores = {
            expectations_store_name: {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleGCSStoreBackend",
                    "project": expectations_store_project_name,
                    "bucket": expectations_store_bucket_name,
                    "prefix": expectations_store_prefix,
                },
            },
            validation_results_store_name: {
                "class_name": "ValidationResultsStore",
                "store_backend": {
                    "class_name": "TupleGCSStoreBackend",
                    "project": validation_results_store_project_name,
                    "bucket": validation_results_store_bucket_name,
                    "prefix": validation_results_store_prefix,
                },
            },
            self.validation_definition_store_name: {
                "class_name": "ValidationDefinitionStore",
                "store_backend": {
                    "class_name": "TupleGCSStoreBackend",
                    "project": validation_definition_store_project_name,
                    "bucket": validation_definition_store_bucket_name,
                    "prefix": validation_definition_store_prefix,
                },
            },
            checkpoint_store_name: {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "TupleGCSStoreBackend",
                    "project": checkpoint_store_project_name,
                    "bucket": checkpoint_store_bucket_name,
                    "prefix": checkpoint_store_prefix,
                },
            },
        }
        self.data_docs_sites = {
            "gcs_site": {
                "class_name": "SiteBuilder",
                "show_how_to_buttons": True,
                "store_backend": {
                    "class_name": "TupleGCSStoreBackend",
                    "project": data_docs_project_name,
                    "bucket": data_docs_bucket_name,
                    "prefix": data_docs_prefix,
                },
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder",
                },
            }
        }


class DatabaseStoreBackendDefaults(BaseStoreBackendDefaults):
    """
    Default store configs for database backends, with some accessible parameters
    Args:
        default_credentials: Use these credentials for all stores that do not have credentials provided
        expectations_store_credentials: Overrides default_credentials if supplied
        validation_results_store_credentials: Overrides default_credentials if supplied
        checkpoint_store_credentials: Overrides default_credentials if supplied
        expectations_store_name: Overrides default if supplied
        validation_results_store_name: Overrides default if supplied
        checkpoint_store_name: Overrides default if supplied
    """  # noqa: E501

    def __init__(  # noqa: PLR0913
        self,
        default_credentials: Optional[Dict] = None,
        expectations_store_credentials: Optional[Dict] = None,
        validation_results_store_credentials: Optional[Dict] = None,
        validation_definition_store_credentials: Optional[Dict] = None,
        checkpoint_store_credentials: Optional[Dict] = None,
        expectations_store_name: str = "expectations_database_store",
        validation_results_store_name: str = "validation_results_database_store",
        checkpoint_store_name: str = "checkpoint_database_store",
    ) -> None:
        # Initialize base defaults
        super().__init__()

        # Use default credentials if separate credentials not supplied for expectations_store and validation_results_store  # noqa: E501
        if expectations_store_credentials is None:
            expectations_store_credentials = default_credentials
        if validation_results_store_credentials is None:
            validation_results_store_credentials = default_credentials
        if validation_definition_store_credentials is None:
            validation_definition_store_credentials = default_credentials
        if checkpoint_store_credentials is None:
            checkpoint_store_credentials = default_credentials

        # Overwrite defaults
        self.expectations_store_name = expectations_store_name
        self.validation_results_store_name = validation_results_store_name
        self.checkpoint_store_name = checkpoint_store_name

        self.stores = {
            expectations_store_name: {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "DatabaseStoreBackend",
                    "credentials": expectations_store_credentials,
                },
            },
            validation_results_store_name: {
                "class_name": "ValidationResultsStore",
                "store_backend": {
                    "class_name": "DatabaseStoreBackend",
                    "credentials": validation_results_store_credentials,
                },
            },
            self.validation_definition_store_name: {
                "class_name": "ValidationDefinitionStore",
                "store_backend": {
                    "class_name": "DatabaseStoreBackend",
                    "credentials": validation_definition_store_credentials,
                },
            },
            checkpoint_store_name: {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "DatabaseStoreBackend",
                    "credentials": checkpoint_store_credentials,
                },
            },
        }


@public_api
class DataContextConfig(BaseYamlConfig):
    """Config class for DataContext.

    The DataContextConfig holds all associated configuration parameters to build a Data Context. There are defaults set
    for minimizing configuration in typical cases, but every parameter is configurable and all defaults are overridable.

    In cases where the DataContext is instantitated without a yml file, the DataContextConfig can be instantiated directly.

    --Documentation--
        - https://docs.greatexpectations.io/docs/guides/setup/configuring_data_contexts/how_to_instantiate_a_data_context_without_a_yml_file/

    Args:
        config_version (Optional[float]): config version of this DataContext.
        fluent_datasources (Optional[dict]): temporary placeholder for Experimental Datasources.
        expectations_store_name (Optional[str]): name of ExpectationStore to be used by DataContext.
        validation_results_store_name (Optional[str]): name of ValidationResultsStore to be used by DataContext.
        checkpoint_store_name (Optional[str]): name of CheckpointStore to be used by DataContext.
        plugins_directory (Optional[str]): the directory in which custom plugin modules should be placed.
        stores (Optional[dict]): single holder for all Stores associated with this DataContext.
        data_docs_sites (Optional[dict]): DataDocs sites associated with DataContext.
        config_variables_file_path (Optional[str]): path for config_variables file, if used.
        analytics_enabled (Optional[bool]): whether or not to send usage statistics to the Great Expectations team.
        data_context_id (Optional[UUID]): unique identifier for the DataContext.
        store_backend_defaults (Optional[BaseStoreBackendDefaults]):  define base defaults for platform specific StoreBackendDefaults.
            For example, if you plan to store expectations, validations, and data_docs in s3 use the S3StoreBackendDefaults
            and you may be able to specify fewer parameters.
        commented_map (Optional[CommentedMap]): the CommentedMap associated with DataContext configuration. Used when
            instantiating with yml file.
        progress_bars (Optional[ProgressBarsConfig]): allows progress_bars to be enabled or disabled globally or for metrics calculations.
    """  # noqa: E501

    def __init__(  # noqa: PLR0913
        self,
        config_version: Optional[float] = None,
        fluent_datasources: Optional[dict] = None,
        expectations_store_name: Optional[str] = None,
        validation_results_store_name: Optional[str] = None,
        checkpoint_store_name: Optional[str] = None,
        plugins_directory: Optional[str] = None,
        stores: Optional[Dict] = None,
        data_docs_sites: Optional[Dict] = None,
        config_variables_file_path: Optional[str] = None,
        analytics_enabled: Optional[bool] = None,
        data_context_id: Optional[uuid.UUID] = None,
        store_backend_defaults: Optional[BaseStoreBackendDefaults] = None,
        commented_map: Optional[CommentedMap] = None,
        progress_bars: Optional[ProgressBarsConfig] = None,
    ) -> None:
        # Set defaults
        if config_version is None:
            config_version = DataContextConfigDefaults.DEFAULT_CONFIG_VERSION.value

        # Set defaults via store_backend_defaults if one is passed in
        # Override attributes from store_backend_defaults with any items passed into the constructor:  # noqa: E501
        if store_backend_defaults is not None:
            if stores is None:
                stores = store_backend_defaults.stores
            if expectations_store_name is None:
                expectations_store_name = store_backend_defaults.expectations_store_name
            if validation_results_store_name is None:
                validation_results_store_name = store_backend_defaults.validation_results_store_name
            if data_docs_sites is None:
                data_docs_sites = store_backend_defaults.data_docs_sites
            if checkpoint_store_name is None:
                checkpoint_store_name = store_backend_defaults.checkpoint_store_name

        self._config_version = config_version
        self.fluent_datasources = fluent_datasources or {}
        self.expectations_store_name = expectations_store_name
        self.validation_results_store_name = validation_results_store_name
        self.checkpoint_store_name = checkpoint_store_name
        self.plugins_directory = plugins_directory
        self.stores = self._init_stores(stores)
        self.data_docs_sites = data_docs_sites
        self.config_variables_file_path = config_variables_file_path
        self.analytics_enabled = analytics_enabled
        self.data_context_id = data_context_id
        self.progress_bars = progress_bars

        super().__init__(commented_map=commented_map)

    def _init_stores(self, store_configs: dict | None) -> dict:
        # If missing all, use all defaults
        if not store_configs:
            return DataContextConfigDefaults.DEFAULT_STORES.value

        # If missing individual stores, just add what is missing
        configured_stores = {config["class_name"] for config in store_configs.values()}
        for name, config in DataContextConfigDefaults.DEFAULT_STORES.value.items():
            if not isinstance(config, dict):
                raise ValueError(  # noqa: TRY003, TRY004
                    "Store defaults must be a mapping of default names to default dictionary configurations."  # noqa: E501
                )
            if config["class_name"] not in configured_stores:
                # Create ephemeral store config
                store_configs[name] = {"class_name": config["class_name"]}

        return store_configs

    # TODO: <Alex>ALEX (we still need the next two properties)</Alex>
    @classmethod
    def get_config_class(cls):
        return cls  # DataContextConfig

    @classmethod
    def get_schema_class(cls):
        return DataContextConfigSchema

    @property
    def config_version(self):
        return self._config_version

    @config_version.setter
    def config_version(self, config_version: float) -> None:
        self._config_version = config_version

    @public_api
    @override
    def to_json_dict(self) -> Dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this DataContextConfig.

        Returns:
            A JSON-serializable dict representation of this DataContextConfig.
        """
        # TODO: <Alex>2/4/2022</Alex>
        # This implementation of "SerializableDictDot.to_json_dict() occurs frequently and should ideally serve as the  # noqa: E501
        # reference implementation in the "SerializableDictDot" class itself.  However, the circular import dependencies,  # noqa: E501
        # due to the location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules  # noqa: E501
        # make this refactoring infeasible at the present time.
        dict_obj: dict = self.to_dict()
        serializeable_dict: dict = convert_to_json_serializable(data=dict_obj)
        return serializeable_dict

    def to_sanitized_json_dict(self) -> dict:
        """
        Wrapper for `to_json_dict` which ensures sensitive fields are properly masked.
        """
        # postpone importing to avoid circular imports
        from great_expectations.data_context.util import PasswordMasker

        serializeable_dict = self.to_json_dict()
        return PasswordMasker.sanitize_config(serializeable_dict)

    def update(self, config: DataContextConfig | Mapping) -> None:
        for k, v in config.items():
            self[k] = v

    @override
    def __repr__(self) -> str:
        """
        # TODO: <Alex>2/4/2022</Alex>
        This implementation of a custom "__repr__()" occurs frequently and should ideally serve as the reference
        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the
        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this
        refactoring infeasible at the present time.
        """  # noqa: E501
        json_dict: dict = self.to_sanitized_json_dict()
        deep_filter_properties_iterable(
            properties=json_dict,
            inplace=True,
        )

        keys: List[str] = sorted(list(json_dict.keys()))

        key: str
        sorted_json_dict: dict = {key: json_dict[key] for key in keys}

        return json.dumps(sorted_json_dict, indent=2)

    @override
    def __str__(self) -> str:
        """
        # TODO: <Alex>2/4/2022</Alex>
        This implementation of a custom "__str__()" occurs frequently and should ideally serve as the reference
        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the
        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this
        refactoring infeasible at the present time.
        """  # noqa: E501
        return self.__repr__()


class CheckpointValidationDefinition(AbstractConfig):
    def __init__(
        self,
        id: str | None = None,
        expectation_suite_name: str | None = None,
        expectation_suite_id: str | None = None,
        batch_request: BatchRequestBase | FluentBatchRequest | dict | None = None,
        **kwargs,
    ) -> None:
        self.expectation_suite_name = expectation_suite_name
        self.expectation_suite_id = expectation_suite_id
        self.batch_request = batch_request
        super().__init__(id=id)

        for k, v in kwargs.items():
            setattr(self, k, v)


class CheckpointValidationDefinitionSchema(AbstractConfigSchema):
    class Meta:
        unknown = INCLUDE

    id = fields.String(required=False, allow_none=True)

    @override
    def dump(self, obj: dict, *, many: Optional[bool] = None) -> dict:
        """
        Chetan - 20220803 - By design, Marshmallow accepts unknown fields through the
        `unknown = INCLUDE` directive but only upon load. When dumping, it validates
        each item against the declared fields and only includes explicitly named values.

        As such, this override of parent behavior is meant to keep ALL values provided
        to the config in the output dict. To get rid of this function, we need to
        explicitly name all possible values in CheckpoingValidationDefinitionSchema as
        schema fields.
        """
        data = super().dump(obj, many=many)

        for key, value in obj.items():
            if key not in data and key not in self.declared_fields and value is not None:
                data[key] = value

        sorted_data = dict(sorted(data.items()))
        return sorted_data

    @pre_dump
    def prepare_dump(self, data, **kwargs):
        data = copy.deepcopy(data)
        for key, value in data.items():
            data[key] = convert_to_json_serializable(data=value)
        return data


dataContextConfigSchema = DataContextConfigSchema()
dataConnectorConfigSchema = DataConnectorConfigSchema()
executionEngineConfigSchema = ExecutionEngineConfigSchema()
assetConfigSchema = AssetConfigSchema()
sorterConfigSchema = SorterConfigSchema()
progressBarsConfigSchema = ProgressBarsConfigSchema()
