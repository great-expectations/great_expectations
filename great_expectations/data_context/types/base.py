from __future__ import annotations

import copy
import datetime
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
    MutableMapping,
    Optional,
    Sequence,
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
from great_expectations.compatibility import pyspark
from great_expectations.core._docs_decorators import deprecated_argument, public_api
from great_expectations.core.batch import BatchRequestBase, get_batch_request_as_dict
from great_expectations.core.configuration import AbstractConfig, AbstractConfigSchema
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.types import DictDot, SerializableDictDot, safe_deep_copy
from great_expectations.types.configurations import ClassConfigSchema
from great_expectations.util import deep_filter_properties_iterable

if TYPE_CHECKING:
    from io import TextIOWrapper

    from great_expectations.alias_types import JSONValues
    from great_expectations.checkpoint import Checkpoint
    from great_expectations.checkpoint.configurator import ActionDict
    from great_expectations.datasource.fluent.batch_request import (
        BatchRequest as FluentBatchRequest,
    )
    from great_expectations.validator.validator import Validator

yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

CURRENT_GX_CONFIG_VERSION = 3
FIRST_GX_CONFIG_VERSION_WITH_CHECKPOINT_STORE = 3
CURRENT_CHECKPOINT_CONFIG_VERSION = 1
MINIMUM_SUPPORTED_CONFIG_VERSION = 2
DEFAULT_USAGE_STATISTICS_URL = (
    "https://stats.greatexpectations.io/great_expectations/v1/usage_statistics"
)


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
            raise gx_exceptions.InvalidConfigError(
                "Invalid type: A configuration schema class needs to inherit from the Marshmallow Schema class."
            )

        if not issubclass(cls.get_config_class(), BaseYamlConfig):
            raise gx_exceptions.InvalidConfigError(
                "Invalid type: A configuration class needs to inherit from the BaseYamlConfig class."
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
    def from_commented_map(
        cls: Type[BYC], commented_map: Union[CommentedMap, Dict]
    ) -> BYC:
        try:
            schema_instance: Schema = cls._get_schema_instance()
            config: Union[dict, BYC] = schema_instance.load(commented_map)
            if isinstance(config, dict):
                return cls.get_config_class()(commented_map=commented_map, **config)

            return config
        except ValidationError:
            logger.error(
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

    @public_api
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
        splitter_method: Optional[str] = None,
        splitter_kwargs: Optional[Dict[str, str]] = None,
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
        if splitter_method is not None:
            self.splitter_method = splitter_method
        if splitter_kwargs is not None:
            self.splitter_kwargs = splitter_kwargs
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

    @public_api
    def to_json_dict(self) -> Dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this AssetConfig.

        Returns:
            A JSON-serializable dict representation of this AssetConfig.
        """
        # TODO: <Alex>2/4/2022</Alex>
        # This implementation of "SerializableDictDot.to_json_dict() occurs frequently and should ideally serve as the
        # reference implementation in the "SerializableDictDot" class itself.  However, the circular import dependencies,
        # due to the location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules
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
    group_names = fields.List(
        cls_or_instance=fields.Str(), required=False, allow_none=True
    )
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
    partitions = fields.List(
        cls_or_instance=fields.Str(), required=False, allow_none=True
    )

    # Necessary addition for Cloud assets
    table_name = fields.String(required=False, allow_none=True)
    type = fields.String(required=False, allow_none=True)

    batch_identifiers = fields.List(
        cls_or_instance=fields.Str(), required=False, allow_none=True
    )

    data_asset_name_prefix = fields.String(required=False, allow_none=True)
    data_asset_name_suffix = fields.String(required=False, allow_none=True)
    include_schema_name = fields.Boolean(required=False, allow_none=True)
    splitter_method = fields.String(required=False, allow_none=True)
    splitter_kwargs = fields.Dict(required=False, allow_none=True)
    sorters = fields.List(
        cls_or_instance=fields.Nested(
            SorterConfigSchema, required=False, allow_none=True
        ),
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
                if (
                    schema
                    and pyspark.types
                    and isinstance(schema, pyspark.types.StructType)
                ):
                    data["batch_spec_passthrough"]["reader_options"][
                        "schema"
                    ] = schema.jsonValue()
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
        splitter_method=None,
        splitter_kwargs=None,
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
        if splitter_method is not None:
            self.splitter_method = splitter_method
        if splitter_kwargs is not None:
            self.splitter_kwargs = splitter_kwargs
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

        # Note: optional samplers and splitters are handled by setattr
        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def class_name(self):
        return self._class_name

    @property
    def module_name(self):
        return self._module_name

    @public_api
    def to_json_dict(self) -> Dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this DataConnectorConfig.

        Returns:
            A JSON-serializable dict representation of this DataConnectorConfig.
        """
        # # TODO: <Alex>2/4/2022</Alex>
        # This implementation of "SerializableDictDot.to_json_dict() occurs frequently and should ideally serve as the
        # reference implementation in the "SerializableDictDot" class itself.  However, the circular import dependencies,
        # due to the location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules
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
    batch_identifiers = fields.List(
        cls_or_instance=fields.Str(), required=False, allow_none=True
    )

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
    splitter_method = fields.String(required=False, allow_none=True)
    splitter_kwargs = fields.Dict(required=False, allow_none=True)
    sorters = fields.List(
        cls_or_instance=fields.Nested(
            SorterConfigSchema, required=False, allow_none=True
        ),
        required=False,
        allow_none=True,
    )
    sampling_method = fields.String(required=False, allow_none=True)
    sampling_kwargs = fields.Dict(required=False, allow_none=True)

    excluded_tables = fields.List(
        cls_or_instance=fields.Str(), required=False, allow_none=True
    )
    included_tables = fields.List(
        cls_or_instance=fields.Str(), required=False, allow_none=True
    )
    skip_inapplicable_tables = fields.Boolean(required=False, allow_none=True)
    introspection_directives = fields.Dict(required=False, allow_none=True)
    batch_spec_passthrough = fields.Dict(required=False, allow_none=True)

    # AWS Glue Data Catalog
    glue_introspection_directives = fields.Dict(required=False, allow_none=True)
    catalog_id = fields.String(required=False, allow_none=True)
    partitions = fields.List(
        cls_or_instance=fields.Str(), required=False, allow_none=True
    )

    # noinspection PyUnusedLocal
    @validates_schema
    def validate_schema(self, data, **kwargs):  # noqa: C901, PLR0912
        # If a class_name begins with the dollar sign ("$"), then it is assumed to be a variable name to be substituted.
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
            raise gx_exceptions.InvalidConfigError(
                f"""Your current configuration uses one or more keys in a data connector that are required only by a
subclass of the FilePathDataConnector class (your data connector is "{data['class_name']}").  Please update your
configuration to continue.
                """
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
            raise gx_exceptions.InvalidConfigError(
                f"""Your current configuration uses one or more keys in a data connector that are required only by a
filesystem type of the data connector (your data connector is "{data['class_name']}").  Please update your
configuration to continue.
                """
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
            raise gx_exceptions.InvalidConfigError(
                f"""Your current configuration uses one or more keys in a data connector that are required only by an
S3/Azure/GCS type of the data connector (your data connector is "{data['class_name']}").  Please update your configuration \
to continue.
"""
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
            raise gx_exceptions.InvalidConfigError(
                f"""Your current configuration uses one or more keys in a data connector that are required only by an
S3/GCS type of the data connector (your data connector is "{data['class_name']}").  Please update your configuration to
continue.
                """
            )
        if ("bucket" in data or "max_keys" in data) and not (
            data["class_name"]  # noqa: E713 # membership check
            in [
                "InferredAssetS3DataConnector",
                "ConfiguredAssetS3DataConnector",
            ]
        ):
            raise gx_exceptions.InvalidConfigError(
                f"""Your current configuration uses one or more keys in a data connector that are required only by an
S3 type of the data connector (your data connector is "{data['class_name']}").  Please update your configuration to
continue.
                """
            )
        if (
            "azure_options" in data or "container" in data or "name_starts_with" in data
        ) and not (
            data["class_name"]  # noqa: E713 # membership check
            in [
                "InferredAssetAzureDataConnector",
                "ConfiguredAssetAzureDataConnector",
            ]
        ):
            raise gx_exceptions.InvalidConfigError(
                f"""Your current configuration uses one or more keys in a data connector that are required only by an
Azure type of the data connector (your data connector is "{data['class_name']}").  Please update your configuration to
continue.
                    """
            )
        if "azure_options" in data and data["class_name"] in [
            "InferredAssetAzureDataConnector",
            "ConfiguredAssetAzureDataConnector",
        ]:
            azure_options = data["azure_options"]
            if not (("conn_str" in azure_options) ^ ("account_url" in azure_options)):
                raise gx_exceptions.InvalidConfigError(
                    """Your current configuration is either missing methods of authentication or is using too many for \
the Azure type of data connector. You must only select one between `conn_str` or `account_url`. Please update your \
configuration to continue.
"""
                )
        if (
            "gcs_options" in data or "bucket_or_name" in data or "max_results" in data
        ) and not (
            data["class_name"]  # noqa: E713 # membership check
            in [
                "InferredAssetGCSDataConnector",
                "ConfiguredAssetGCSDataConnector",
            ]
        ):
            raise gx_exceptions.InvalidConfigError(
                f"""Your current configuration uses one or more keys in a data connector that are required only by a
GCS type of the data connector (your data connector is "{data['class_name']}").  Please update your configuration to
continue.
                    """
            )
        if "gcs_options" in data and data["class_name"] in [
            "InferredAssetGCSDataConnector",
            "ConfiguredAssetGCSDataConnector",
        ]:
            gcs_options = data["gcs_options"]
            if "filename" in gcs_options and "info" in gcs_options:
                raise gx_exceptions.InvalidConfigError(
                    """Your current configuration can only use a single method of authentication for the GCS type of \
data connector. You must only select one between `filename` (from_service_account_file) and `info` \
(from_service_account_info). Please update your configuration to continue.
"""
                )
        if (
            "include_schema_name" in data
            or "splitter_method" in data
            or "splitter_kwargs" in data
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
            raise gx_exceptions.InvalidConfigError(
                f"""Your current configuration uses one or more keys in a data connector that are required only by an
SQL type of the data connector (your data connector is "{data['class_name']}").  Please update your configuration to
continue.
                """
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
            raise gx_exceptions.InvalidConfigError(
                f"""Your current configuration uses one or more keys in a data connector that are required only by an
SQL/GlueCatalog type of the data connector (your data connector is "{data['class_name']}").  Please update your configuration to
continue.
                """
            )

        if (
            "partitions" in data
            or "catalog_id" in data
            or "glue_introspection_directives" in data
        ) and not (
            data["class_name"]  # noqa: E713 # membership check
            in [
                "InferredAssetAWSGlueDataCatalogDataConnector",
                "ConfiguredAssetAWSGlueDataCatalogDataConnector",
            ]
        ):
            raise gx_exceptions.InvalidConfigError(
                f"""Your current configuration uses one or more keys in a data connector that are required only by an
GlueCatalog type of the data connector (your data connector is "{data['class_name']}").  Please update your configuration to
continue.
                """
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
                if (
                    schema
                    and pyspark.types
                    and isinstance(schema, pyspark.types.StructType)
                ):
                    data["batch_spec_passthrough"]["reader_options"][
                        "schema"
                    ] = schema.jsonValue()
        return data


class ExecutionEngineConfig(DictDot):
    def __init__(  # noqa: PLR0913
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
    # BigQuery Service Account Credentials
    # https://googleapis.dev/python/sqlalchemy-bigquery/latest/README.html#connection-string-parameters
    credentials_info = fields.Dict(required=False, allow_none=True)

    create_temp_table = fields.Boolean(required=False, allow_none=True)

    # noinspection PyUnusedLocal
    @validates_schema
    def validate_schema(self, data, **kwargs):
        # If a class_name begins with the dollar sign ("$"), then it is assumed to be a variable name to be substituted.
        if data["class_name"][0] == "$":
            return
        if ("connection_string" in data or "credentials" in data) and not (
            data["class_name"] == "SqlAlchemyExecutionEngine"
        ):
            raise gx_exceptions.InvalidConfigError(
                f"""Your current configuration uses the "connection_string" key in an execution engine, but only
SqlAlchemyExecutionEngine requires this attribute (your execution engine is "{data['class_name']}").  Please update your
configuration to continue.
                """
            )
        if "spark_config" in data and not (
            data["class_name"] == "SparkDFExecutionEngine"
        ):
            raise gx_exceptions.InvalidConfigError(
                f"""Your current configuration uses the "spark_config" key in an execution engine, but only
SparkDFExecutionEngine requires this attribute (your execution engine is "{data['class_name']}").  Please update your
configuration to continue.
                """
            )

    # noinspection PyUnusedLocal
    @post_load
    def make_execution_engine_config(self, data, **kwargs):
        return ExecutionEngineConfig(**data)


class DatasourceConfig(AbstractConfig):
    def __init__(  # noqa: C901, PLR0912, PLR0913
        self,
        name: Optional[
            str
        ] = None,  # Note: name is optional currently to avoid updating all documentation within
        # the scope of this work.
        id: Optional[str] = None,
        class_name: Optional[str] = None,
        module_name: str = "great_expectations.datasource",
        execution_engine=None,
        data_connectors=None,
        data_asset_type=None,
        batch_kwargs_generators=None,
        connection_string=None,
        credentials=None,
        introspection=None,
        tables=None,
        boto3_options=None,
        azure_options=None,
        gcs_options=None,
        credentials_info=None,
        reader_method=None,
        reader_options=None,
        limit=None,
        **kwargs,
    ) -> None:
        super().__init__(id=id, name=name)
        # NOTE - JPC - 20200316: Currently, we are mostly inconsistent with respect to this type...
        self._class_name = class_name
        self._module_name = module_name
        if execution_engine is not None:
            self.execution_engine = execution_engine
        if data_connectors is not None and isinstance(data_connectors, dict):
            self.data_connectors = data_connectors

        # NOTE - AJB - 20201202: This should use the datasource class build_configuration method as in DataContext.add_datasource()
        if data_asset_type is None:
            if class_name == "PandasDatasource":
                data_asset_type = {
                    "class_name": "PandasDataset",
                    "module_name": "great_expectations.dataset",
                }
            elif class_name == "SqlAlchemyDatasource":
                data_asset_type = {
                    "class_name": "SqlAlchemyDataset",
                    "module_name": "great_expectations.dataset",
                }
            elif class_name == "SparkDFDatasource":
                data_asset_type = {
                    "class_name": "SparkDFDataset",
                    "module_name": "great_expectations.dataset",
                }
        if data_asset_type is not None:
            self.data_asset_type = data_asset_type
        if batch_kwargs_generators is not None:
            self.batch_kwargs_generators = batch_kwargs_generators
        if connection_string is not None:
            self.connection_string = connection_string
        if credentials is not None:
            self.credentials = credentials
        if introspection is not None:
            self.introspection = introspection
        if tables is not None:
            self.tables = tables
        if boto3_options is not None:
            self.boto3_options = boto3_options
        if azure_options is not None:
            self.azure_options = azure_options
        if gcs_options is not None:
            self.gcs_options = gcs_options
        if credentials_info is not None:
            self.credentials_info = credentials_info
        if reader_method is not None:
            self.reader_method = reader_method
        if reader_options is not None:
            self.reader_options = reader_options
        if limit is not None:
            self.limit = limit
        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def class_name(self):
        return self._class_name

    @property
    def module_name(self):
        return self._module_name

    @public_api
    def to_json_dict(self) -> Dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this DatasourceConfig.

        Returns:
            A JSON-serializable dict representation of this DatasourceConfig.
        """
        # # TODO: <Alex>2/4/2022</Alex>
        # This implementation of "SerializableDictDot.to_json_dict() occurs frequently and should ideally serve as the
        # reference implementation in the "SerializableDictDot" class itself.  However, the circular import dependencies,
        # due to the location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules
        # make this refactoring infeasible at the present time.
        dict_obj: dict = self.to_dict()
        serializeable_dict: dict = convert_to_json_serializable(data=dict_obj)
        return serializeable_dict


class DatasourceConfigSchema(AbstractConfigSchema):
    class Meta:
        unknown = INCLUDE

    # Note: name is optional currently to avoid updating all documentation within
    # the scope of this work.
    name = fields.String(
        required=False,
        allow_none=True,
    )
    id = fields.String(
        required=False,
        allow_none=True,
    )

    class_name = fields.String(
        required=False,
        allow_none=True,
        missing="Datasource",
    )
    module_name = fields.String(
        required=False,
        allow_none=True,
        missing="great_expectations.datasource",
    )
    force_reuse_spark_context = fields.Bool(required=False, allow_none=True)
    spark_config = fields.Raw(required=False, allow_none=True)
    execution_engine = fields.Nested(
        ExecutionEngineConfigSchema, required=False, allow_none=True
    )
    data_connectors = fields.Dict(
        keys=fields.Str(),
        values=fields.Nested(DataConnectorConfigSchema),
        required=False,
        allow_none=True,
    )

    data_asset_type = fields.Nested(ClassConfigSchema, required=False, allow_none=True)

    # TODO: Update to generator-specific
    # batch_kwargs_generators = fields.Mapping(keys=fields.Str(), values=fields.Nested(fields.GeneratorSchema))
    batch_kwargs_generators = fields.Dict(
        keys=fields.Str(), values=fields.Dict(), required=False, allow_none=True
    )
    connection_string = fields.String(required=False, allow_none=True)
    credentials = fields.Raw(required=False, allow_none=True)
    introspection = fields.Dict(required=False, allow_none=True)
    tables = fields.Dict(required=False, allow_none=True)
    boto3_options = fields.Dict(
        keys=fields.Str(), values=fields.Str(), required=False, allow_none=True
    )
    azure_options = fields.Dict(
        keys=fields.Str(), values=fields.Str(), required=False, allow_none=True
    )
    gcs_options = fields.Dict(
        keys=fields.Str(), values=fields.Str(), required=False, allow_none=True
    )
    # BigQuery Service Account Credentials
    # https://googleapis.dev/python/sqlalchemy-bigquery/latest/README.html#connection-string-parameters
    credentials_info = fields.Dict(required=False, allow_none=True)
    reader_method = fields.String(required=False, allow_none=True)
    reader_options = fields.Dict(
        keys=fields.Str(), values=fields.Str(), required=False, allow_none=True
    )
    limit = fields.Integer(required=False, allow_none=True)

    # noinspection PyUnusedLocal
    @validates_schema
    def validate_schema(self, data, **kwargs):
        if "generators" in data:
            raise gx_exceptions.InvalidConfigError(
                'Your current configuration uses the "generators" key in a datasource, but in version 0.10 of '
                'GX that key is renamed to "batch_kwargs_generators". Please update your configuration to continue.'
            )
        # If a class_name begins with the dollar sign ("$"), then it is assumed to be a variable name to be substituted.
        if data["class_name"][0] == "$":
            return

        if (
            "connection_string" in data
            or "credentials" in data
            or "introspection" in data
            or "tables" in data
        ) and not (
            data["class_name"]  # noqa: E713 # membership check
            in [
                "SqlAlchemyDatasource",
                "SimpleSqlalchemyDatasource",
            ]
        ):
            raise gx_exceptions.InvalidConfigError(
                f"""Your current configuration uses one or more keys in a data source that are required only by a
sqlalchemy data source (your data source is "{data['class_name']}").  Please update your configuration to continue.
                """
            )

    # noinspection PyUnusedLocal
    @post_load
    def make_datasource_config(self, data, **kwargs):
        # Add names to data connectors
        for data_connector_name, data_connector_config in data.get(
            "data_connectors", {}
        ).items():
            data_connector_config["name"] = data_connector_name
        return DatasourceConfig(**data)


class AnonymizedUsageStatisticsConfig(DictDot):
    def __init__(
        self, enabled=True, data_context_id=None, usage_statistics_url=None
    ) -> None:
        self._enabled = enabled

        if data_context_id is None:
            data_context_id = str(uuid.uuid4())
            self._explicit_id = False
        else:
            self._explicit_id = True

        self._data_context_id = data_context_id

        if usage_statistics_url is None:
            usage_statistics_url = DEFAULT_USAGE_STATISTICS_URL
            self._explicit_url = False
        else:
            self._explicit_url = True

        self._usage_statistics_url = usage_statistics_url

    @property
    def enabled(self) -> bool:
        return self._enabled

    @enabled.setter
    def enabled(self, enabled) -> None:
        if not isinstance(enabled, bool):
            raise ValueError("usage statistics enabled property must be boolean")

        self._enabled = enabled

    @property
    def data_context_id(self) -> str:
        return self._data_context_id

    @data_context_id.setter
    def data_context_id(self, data_context_id) -> None:
        try:
            uuid.UUID(data_context_id)
        except ValueError:
            raise gx_exceptions.InvalidConfigError(
                "data_context_id must be a valid uuid"
            )

        self._data_context_id = data_context_id
        self._explicit_id = True

    @property
    def explicit_id(self) -> bool:
        return self._explicit_id

    @property
    def explicit_url(self) -> bool:
        return self._explicit_url

    @property
    def usage_statistics_url(self) -> str:
        return self._usage_statistics_url

    @usage_statistics_url.setter
    def usage_statistics_url(self, usage_statistics_url) -> None:
        self._usage_statistics_url = usage_statistics_url
        self._explicit_url = True


class AnonymizedUsageStatisticsConfigSchema(Schema):
    data_context_id = fields.UUID()
    enabled = fields.Boolean(default=True)
    usage_statistics_url = fields.URL(allow_none=True)
    _explicit_url = fields.Boolean(required=False)

    # noinspection PyUnusedLocal
    @post_load()
    def make_usage_statistics_config(self, data, **kwargs):
        if "data_context_id" in data:
            data["data_context_id"] = str(data["data_context_id"])
        return AnonymizedUsageStatisticsConfig(**data)

    # noinspection PyUnusedLocal
    @post_dump()
    def filter_implicit(self, data, **kwargs):
        if not data.get("_explicit_url") and "usage_statistics_url" in data:
            del data["usage_statistics_url"]
        if "_explicit_url" in data:
            del data["_explicit_url"]
        return data


class NotebookTemplateConfig(DictDot):
    def __init__(self, file_name, template_kwargs=None) -> None:
        self.file_name = file_name
        if template_kwargs:
            self.template_kwargs = template_kwargs
        else:
            self.template_kwargs = {}


class NotebookTemplateConfigSchema(Schema):
    file_name = fields.String()
    template_kwargs = fields.Dict(
        keys=fields.Str(), values=fields.Str(), allow_none=True
    )

    # noinspection PyUnusedLocal
    @post_load
    def make_notebook_template_config(self, data, **kwargs):
        return NotebookTemplateConfig(**data)


class NotebookConfig(DictDot):
    def __init__(  # noqa: PLR0913
        self,
        class_name,
        module_name,
        custom_templates_module=None,
        header_markdown=None,
        footer_markdown=None,
        table_expectations_header_markdown=None,
        column_expectations_header_markdown=None,
        table_expectations_not_found_markdown=None,
        column_expectations_not_found_markdown=None,
        authoring_intro_markdown=None,
        column_expectations_markdown=None,
        header_code=None,
        footer_code=None,
        table_expectation_code=None,
        column_expectation_code=None,
    ) -> None:
        self.class_name = class_name
        self.module_name = module_name
        self.custom_templates_module = custom_templates_module

        self.header_markdown = header_markdown
        self.footer_markdown = footer_markdown
        self.table_expectations_header_markdown = table_expectations_header_markdown
        self.column_expectations_header_markdown = column_expectations_header_markdown
        self.table_expectations_not_found_markdown = (
            table_expectations_not_found_markdown
        )
        self.column_expectations_not_found_markdown = (
            column_expectations_not_found_markdown
        )
        self.authoring_intro_markdown = authoring_intro_markdown
        self.column_expectations_markdown = column_expectations_markdown

        self.header_code = header_code
        self.footer_code = footer_code
        self.table_expectation_code = table_expectation_code
        self.column_expectation_code = column_expectation_code


class NotebookConfigSchema(Schema):
    class_name = fields.String(missing="SuiteEditNotebookRenderer")
    module_name = fields.String(
        missing="great_expectations.render.renderer.v3.suite_edit_notebook_renderer"
    )
    custom_templates_module = fields.String(allow_none=True)

    header_markdown = fields.Nested(NotebookTemplateConfigSchema, allow_none=True)
    footer_markdown = fields.Nested(NotebookTemplateConfigSchema, allow_none=True)
    table_expectations_header_markdown = fields.Nested(
        NotebookTemplateConfigSchema, allow_none=True
    )
    column_expectations_header_markdown = fields.Nested(
        NotebookTemplateConfigSchema, allow_none=True
    )
    table_expectations_not_found_markdown = fields.Nested(
        NotebookTemplateConfigSchema, allow_none=True
    )
    column_expectations_not_found_markdown = fields.Nested(
        NotebookTemplateConfigSchema, allow_none=True
    )
    authoring_intro_markdown = fields.Nested(
        NotebookTemplateConfigSchema, allow_none=True
    )
    column_expectations_markdown = fields.Nested(
        NotebookTemplateConfigSchema, allow_none=True
    )

    header_code = fields.Nested(NotebookTemplateConfigSchema, allow_none=True)
    footer_code = fields.Nested(NotebookTemplateConfigSchema, allow_none=True)
    table_expectation_code = fields.Nested(
        NotebookTemplateConfigSchema, allow_none=True
    )
    column_expectation_code = fields.Nested(
        NotebookTemplateConfigSchema, allow_none=True
    )

    # noinspection PyUnusedLocal
    @post_load
    def make_notebook_config(self, data, **kwargs):
        return NotebookConfig(**data)


class NotebooksConfig(DictDot):
    def __init__(self, suite_edit) -> None:
        self.suite_edit = suite_edit


class NotebooksConfigSchema(Schema):
    # for now only suite_edit, could have other customization options for
    # notebooks in the future
    suite_edit = fields.Nested(NotebookConfigSchema)

    # noinspection PyUnusedLocal
    @post_load
    def make_notebooks_config(self, data, **kwargs):
        return NotebooksConfig(**data)


class ProgressBarsConfig(DictDot):
    def __init__(
        self,
        globally: bool = True,
        profilers: bool = True,
        metric_calculations: bool = True,
    ) -> None:
        self.globally = globally
        self.profilers = profilers
        self.metric_calculations = metric_calculations


class ProgressBarsConfigSchema(Schema):
    globally = fields.Boolean(default=True)
    profilers = fields.Boolean(default=True)
    metric_calculations = fields.Boolean(default=True)


class IncludeRenderedContentConfig(DictDot):
    def __init__(
        self,
        globally: bool = False,
        expectation_suite: bool = False,
        expectation_validation_result: bool = False,
    ) -> None:
        self.globally = globally
        self.expectation_suite = expectation_suite
        self.expectation_validation_result = expectation_validation_result


class IncludeRenderedContentConfigSchema(Schema):
    globally = fields.Boolean(default=False)
    expectation_suite = fields.Boolean(default=False)
    expectation_validation_result = fields.Boolean(default=False)


class ConcurrencyConfig(DictDot):
    """WARNING: This class is experimental."""

    def __init__(self, enabled: bool = False) -> None:
        """Initialize a concurrency configuration to control multithreaded execution.

        Args:
            enabled: Whether or not multithreading is enabled.
        """
        self._enabled = enabled

    @property
    def enabled(self):
        """Whether or not multithreading is enabled."""
        return self._enabled

    @property
    def max_database_query_concurrency(self) -> int:
        """Max number of concurrent database queries to execute with mulithreading."""
        # BigQuery has a limit of 100 for "Concurrent rate limit for interactive queries" as described at
        # (https://cloud.google.com/bigquery/quotas#query_jobs). If necessary, this can later be tuned for other
        # databases and/or be manually user configurable.
        return 100

    def add_sqlalchemy_create_engine_parameters(
        self, parameters: MutableMapping[str, Any]
    ):
        """Update SqlAlchemy parameters to prevent concurrency errors (e.g. http://sqlalche.me/e/14/3o7r) and
        bottlenecks.

        Args:
            parameters: SqlAlchemy create_engine parameters to which we add concurrency appropriate parameters. If the
                concurrency parameters are already set, those parameters are left unchanged.
        """
        if not self._enabled:
            return

        if "pool_size" not in parameters:
            # https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine.params.pool_size
            parameters["pool_size"] = 0
        if "max_overflow" not in parameters:
            # https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine.params.max_overflow
            parameters["max_overflow"] = -1


class ConcurrencyConfigSchema(Schema):
    """WARNING: This class is experimental."""

    enabled = fields.Boolean(default=False)


class GXCloudConfig(DictDot):
    def __init__(
        self,
        base_url: str,
        access_token: Optional[str] = None,
        organization_id: Optional[str] = None,
    ) -> None:
        # access_token was given a default value to maintain arg position of organization_id
        if access_token is None:
            raise ValueError("Access token cannot be None.")

        self.base_url = base_url
        self.organization_id = organization_id
        self.access_token = access_token

    @public_api
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
    datasources = fields.Dict(
        keys=fields.Str(),
        values=fields.Nested(DatasourceConfigSchema),
        required=False,
        allow_none=True,
    )
    fluent_datasources = fields.Dict(
        keys=fields.Str(),
        required=False,
        allow_none=True,
        load_only=True,
    )
    expectations_store_name = fields.Str()
    validations_store_name = fields.Str()
    evaluation_parameter_store_name = fields.Str()
    checkpoint_store_name = fields.Str(required=False, allow_none=True)
    profiler_store_name = fields.Str(required=False, allow_none=True)
    plugins_directory = fields.Str(allow_none=True)
    validation_operators = fields.Dict(
        keys=fields.Str(), values=fields.Dict(), required=False, allow_none=True
    )
    stores = fields.Dict(keys=fields.Str(), values=fields.Dict())
    notebooks = fields.Nested(NotebooksConfigSchema, allow_none=True)
    data_docs_sites = fields.Dict(
        keys=fields.Str(), values=fields.Dict(), allow_none=True
    )
    config_variables_file_path = fields.Str(allow_none=True)
    anonymous_usage_statistics = fields.Nested(AnonymizedUsageStatisticsConfigSchema)
    progress_bars = fields.Nested(
        ProgressBarsConfigSchema, required=False, allow_none=True
    )
    include_rendered_content = fields.Nested(
        IncludeRenderedContentConfigSchema, required=False, allow_none=True
    )
    concurrency = fields.Nested(
        ConcurrencyConfigSchema, required=False, allow_none=True
    )

    # To ensure backwards compatability, we need to ensure that new options are "opt-in"
    # If a user has not explicitly configured the value, it will be None and will be wiped by the post_dump hook
    REMOVE_KEYS_IF_NONE = [
        "concurrency",  # 0.13.33
        "progress_bars",  # 0.13.49
        "include_rendered_content",  # 0.15.19,
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

    def handle_error(self, exc, data, **kwargs) -> None:  # type: ignore[override]
        """Log and raise our custom exception when (de)serialization fails."""
        if (
            exc
            and exc.messages
            and isinstance(exc.messages, dict)
            and all(key is None for key in exc.messages.keys())
        ):
            exc.messages = list(itertools.chain.from_iterable(exc.messages.values()))

        message: str = (
            f"Error while processing DataContextConfig: {' '.join(exc.messages)}"
        )
        logger.error(message)
        raise gx_exceptions.InvalidDataContextConfigError(
            message=message,
        )

    # noinspection PyUnusedLocal
    @validates_schema
    def validate_schema(self, data, **kwargs) -> None:
        if "config_version" not in data:
            raise gx_exceptions.InvalidDataContextConfigError(
                "The key `config_version` is missing; please check your config file.",
                validation_error=ValidationError(message="no config_version key"),
            )

        if not isinstance(data["config_version"], (int, float)):
            raise gx_exceptions.InvalidDataContextConfigError(
                "The key `config_version` must be a number. Please check your config file.",
                validation_error=ValidationError(message="config version not a number"),
            )

        # When migrating from 0.7.x to 0.8.0
        if data["config_version"] == 0 and any(
            store_config["class_name"] == "ValidationsStore"
            for store_config in data["stores"].values()
        ):
            raise gx_exceptions.UnsupportedConfigVersionError(
                "You appear to be using a config version from the 0.7.x series. This version is no longer supported."
            )

        if data["config_version"] < MINIMUM_SUPPORTED_CONFIG_VERSION:
            raise gx_exceptions.UnsupportedConfigVersionError(
                "You appear to have an invalid config version ({}).\n    The version number must be at least {}. "
                "Please see the migration guide at https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api".format(
                    data["config_version"], MINIMUM_SUPPORTED_CONFIG_VERSION
                ),
            )

        if data["config_version"] > CURRENT_GX_CONFIG_VERSION:
            raise gx_exceptions.InvalidDataContextConfigError(
                "You appear to have an invalid config version ({}).\n    The maximum valid version is {}.".format(
                    data["config_version"], CURRENT_GX_CONFIG_VERSION
                ),
                validation_error=ValidationError(message="config version too high"),
            )

        if data["config_version"] < CURRENT_GX_CONFIG_VERSION and (
            "checkpoint_store_name" in data
            or any(
                store_config["class_name"] == "CheckpointStore"
                for store_config in data["stores"].values()
            )
        ):
            raise gx_exceptions.InvalidDataContextConfigError(
                "You appear to be using a Checkpoint store with an invalid config version ({}).\n    Your data context with this older configuration version specifies a Checkpoint store, which is a new feature.  Please update your configuration to the new version number {} before adding a Checkpoint store.\n  Visit https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api to learn more about the upgrade process.".format(
                    data["config_version"], float(CURRENT_GX_CONFIG_VERSION)
                ),
                validation_error=ValidationError(
                    message="You appear to be using a Checkpoint store with an invalid config version ({}).\n    Your data context with this older configuration version specifies a Checkpoint store, which is a new feature.  Please update your configuration to the new version number {} before adding a Checkpoint store.\n  Visit https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api to learn more about the upgrade process.".format(
                        data["config_version"], float(CURRENT_GX_CONFIG_VERSION)
                    )
                ),
            )

        if (
            data["config_version"] >= FIRST_GX_CONFIG_VERSION_WITH_CHECKPOINT_STORE
            and "validation_operators" in data
            and data["validation_operators"] is not None
        ):
            logger.warning(
                f"""You appear to be using a legacy capability with the latest config version \
({data["config_version"]}).\n    Your data context with this configuration version uses validation_operators, which \
are being deprecated.  Please consult the V3 API migration guide \
https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api and \
update your configuration to be compatible with the version number {CURRENT_GX_CONFIG_VERSION}.\n    (This message \
will appear repeatedly until your configuration is updated.)
"""
            )


class DataContextConfigDefaults(enum.Enum):
    DEFAULT_CONFIG_VERSION = CURRENT_GX_CONFIG_VERSION
    UNCOMMITTED = "uncommitted"

    DEFAULT_EXPECTATIONS_STORE_NAME = "expectations_store"
    EXPECTATIONS_BASE_DIRECTORY = "expectations"
    DEFAULT_EXPECTATIONS_STORE_BASE_DIRECTORY_RELATIVE_NAME = (
        f"{EXPECTATIONS_BASE_DIRECTORY}/"
    )
    DEFAULT_VALIDATIONS_STORE_NAME = "validations_store"
    VALIDATIONS_BASE_DIRECTORY = "validations"
    DEFAULT_VALIDATIONS_STORE_BASE_DIRECTORY_RELATIVE_NAME = (
        f"{UNCOMMITTED}/{VALIDATIONS_BASE_DIRECTORY}/"
    )
    DEFAULT_EVALUATION_PARAMETER_STORE_NAME = "evaluation_parameter_store"
    DEFAULT_EVALUATION_PARAMETER_STORE_BASE_DIRECTORY_RELATIVE_NAME = (
        "evaluation_parameters/"
    )
    DATA_DOCS_BASE_DIRECTORY = "data_docs"
    DEFAULT_DATA_DOCS_BASE_DIRECTORY_RELATIVE_NAME = (
        f"{UNCOMMITTED}/{DATA_DOCS_BASE_DIRECTORY}"
    )

    # Datasource
    DEFAULT_DATASOURCE_STORE_NAME = "datasource_store"

    # Checkpoints
    DEFAULT_CHECKPOINT_STORE_NAME = "checkpoint_store"
    CHECKPOINTS_BASE_DIRECTORY = "checkpoints"
    DEFAULT_CHECKPOINT_STORE_BASE_DIRECTORY_RELATIVE_NAME = (
        f"{CHECKPOINTS_BASE_DIRECTORY}/"
    )

    # Profilers
    DEFAULT_PROFILER_STORE_NAME = "profiler_store"
    PROFILERS_BASE_DIRECTORY = "profilers"
    DEFAULT_PROFILER_STORE_BASE_DIRECTORY_RELATIVE_NAME = f"{PROFILERS_BASE_DIRECTORY}/"

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
            "name": "store_evaluation_params",
            "action": {"class_name": "StoreEvaluationParametersAction"},
        },
        {
            "name": "update_data_docs",
            "action": {"class_name": "UpdateDataDocsAction"},
        },
    ]
    DEFAULT_VALIDATION_OPERATORS = {
        "action_list_operator": {
            "class_name": "ActionListValidationOperator",
            "action_list": DEFAULT_ACTION_LIST,
        }
    }
    DEFAULT_STORES = {
        DEFAULT_EXPECTATIONS_STORE_NAME: {
            "class_name": "ExpectationsStore",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": DEFAULT_EXPECTATIONS_STORE_BASE_DIRECTORY_RELATIVE_NAME,
            },
        },
        DEFAULT_VALIDATIONS_STORE_NAME: {
            "class_name": "ValidationsStore",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": DEFAULT_VALIDATIONS_STORE_BASE_DIRECTORY_RELATIVE_NAME,
            },
        },
        DEFAULT_EVALUATION_PARAMETER_STORE_NAME: {
            "class_name": "EvaluationParameterStore"
        },
        DEFAULT_CHECKPOINT_STORE_NAME: {
            "class_name": "CheckpointStore",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "suppress_store_backend_id": True,
                "base_directory": DEFAULT_CHECKPOINT_STORE_BASE_DIRECTORY_RELATIVE_NAME,
            },
        },
        DEFAULT_PROFILER_STORE_NAME: {
            "class_name": "ProfilerStore",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "suppress_store_backend_id": True,
                "base_directory": DEFAULT_PROFILER_STORE_BASE_DIRECTORY_RELATIVE_NAME,
            },
        },
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


class CheckpointConfigDefaults(enum.Enum):
    DEFAULT_CONFIG_VERSION = CURRENT_CHECKPOINT_CONFIG_VERSION


class BaseStoreBackendDefaults(DictDot):
    """
    Define base defaults for platform specific StoreBackendDefaults.
    StoreBackendDefaults define defaults for specific cases of often used configurations.
    For example, if you plan to store expectations, validations, and data_docs in s3 use the S3StoreBackendDefaults and you may be able to specify less parameters.
    """

    def __init__(  # noqa: PLR0913
        self,
        expectations_store_name: str = DataContextConfigDefaults.DEFAULT_EXPECTATIONS_STORE_NAME.value,
        validations_store_name: str = DataContextConfigDefaults.DEFAULT_VALIDATIONS_STORE_NAME.value,
        evaluation_parameter_store_name: str = DataContextConfigDefaults.DEFAULT_EVALUATION_PARAMETER_STORE_NAME.value,
        checkpoint_store_name: str = DataContextConfigDefaults.DEFAULT_CHECKPOINT_STORE_NAME.value,
        profiler_store_name: str = DataContextConfigDefaults.DEFAULT_PROFILER_STORE_NAME.value,
        data_docs_site_name: str = DataContextConfigDefaults.DEFAULT_DATA_DOCS_SITE_NAME.value,
        validation_operators: Optional[dict] = None,
        stores: Optional[dict] = None,
        data_docs_sites: Optional[dict] = None,
    ) -> None:
        self.expectations_store_name = expectations_store_name
        self.validations_store_name = validations_store_name
        self.evaluation_parameter_store_name = evaluation_parameter_store_name
        self.checkpoint_store_name = checkpoint_store_name
        self.profiler_store_name = profiler_store_name
        self.validation_operators = validation_operators
        if stores is None:
            stores = copy.deepcopy(DataContextConfigDefaults.DEFAULT_STORES.value)

        self.stores = stores
        if data_docs_sites is None:
            data_docs_sites = copy.deepcopy(
                DataContextConfigDefaults.DEFAULT_DATA_DOCS_SITES.value
            )

        self.data_docs_sites = data_docs_sites
        self.data_docs_site_name = data_docs_site_name


@public_api
class S3StoreBackendDefaults(BaseStoreBackendDefaults):
    """Default store configs for s3 backends, with some accessible parameters.

    Args:
        default_bucket_name: Use this bucket name for stores that do not have a bucket name provided
        expectations_store_bucket_name: Overrides default_bucket_name if supplied
        validations_store_bucket_name: Overrides default_bucket_name if supplied
        data_docs_bucket_name: Overrides default_bucket_name if supplied
        checkpoint_store_bucket_name: Overrides default_bucket_name if supplied
        profiler_store_bucket_name: Overrides default_bucket_name if supplied
        expectations_store_prefix: Overrides default if supplied
        validations_store_prefix: Overrides default if supplied
        data_docs_prefix: Overrides default if supplied
        checkpoint_store_prefix: Overrides default if supplied
        profiler_store_prefix: Overrides default if supplied
        expectations_store_name: Overrides default if supplied
        validations_store_name: Overrides default if supplied
        evaluation_parameter_store_name: Overrides default if supplied
        checkpoint_store_name: Overrides default if supplied
        profiler_store_name: Overrides default if supplied
    """

    def __init__(  # noqa: PLR0913
        self,
        default_bucket_name: Optional[str] = None,
        expectations_store_bucket_name: Optional[str] = None,
        validations_store_bucket_name: Optional[str] = None,
        data_docs_bucket_name: Optional[str] = None,
        checkpoint_store_bucket_name: Optional[str] = None,
        profiler_store_bucket_name: Optional[str] = None,
        expectations_store_prefix: str = "expectations",
        validations_store_prefix: str = "validations",
        data_docs_prefix: str = "data_docs",
        checkpoint_store_prefix: str = "checkpoints",
        profiler_store_prefix: str = "profilers",
        expectations_store_name: str = "expectations_S3_store",
        validations_store_name: str = "validations_S3_store",
        evaluation_parameter_store_name: str = "evaluation_parameter_store",
        checkpoint_store_name: str = "checkpoint_S3_store",
        profiler_store_name: str = "profiler_S3_store",
    ) -> None:
        # Initialize base defaults
        super().__init__()

        # Use default_bucket_name if separate store buckets are not provided
        if expectations_store_bucket_name is None:
            expectations_store_bucket_name = default_bucket_name
        if validations_store_bucket_name is None:
            validations_store_bucket_name = default_bucket_name
        if data_docs_bucket_name is None:
            data_docs_bucket_name = default_bucket_name
        if checkpoint_store_bucket_name is None:
            checkpoint_store_bucket_name = default_bucket_name
        if profiler_store_bucket_name is None:
            profiler_store_bucket_name = default_bucket_name

        # Overwrite defaults
        self.expectations_store_name = expectations_store_name
        self.validations_store_name = validations_store_name
        self.evaluation_parameter_store_name = evaluation_parameter_store_name
        self.checkpoint_store_name = checkpoint_store_name
        self.profiler_store_name = profiler_store_name
        self.stores = {
            expectations_store_name: {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": expectations_store_bucket_name,
                    "prefix": expectations_store_prefix,
                },
            },
            validations_store_name: {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": validations_store_bucket_name,
                    "prefix": validations_store_prefix,
                },
            },
            evaluation_parameter_store_name: {"class_name": "EvaluationParameterStore"},
            checkpoint_store_name: {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": checkpoint_store_bucket_name,
                    "prefix": checkpoint_store_prefix,
                },
            },
            profiler_store_name: {
                "class_name": "ProfilerStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": profiler_store_bucket_name,
                    "prefix": profiler_store_prefix,
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
        root_directory: Optional[str] = None,
        plugins_directory: Optional[str] = None,
    ) -> None:
        # Initialize base defaults
        super().__init__()

        if plugins_directory is None:
            plugins_directory = (
                DataContextConfigDefaults.DEFAULT_PLUGINS_DIRECTORY.value
            )
        self.plugins_directory = plugins_directory
        if root_directory is not None:
            self.stores[self.expectations_store_name]["store_backend"][  # type: ignore[index]
                "root_directory"
            ] = root_directory
            self.stores[self.validations_store_name]["store_backend"][  # type: ignore[index]
                "root_directory"
            ] = root_directory
            self.stores[self.checkpoint_store_name]["store_backend"][  # type: ignore[index]
                "root_directory"
            ] = root_directory
            self.stores[self.profiler_store_name]["store_backend"][  # type: ignore[index]
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
            self.validations_store_name: {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "InMemoryStoreBackend",
                },
            },
            self.evaluation_parameter_store_name: {
                "class_name": "EvaluationParameterStore"
            },
            self.checkpoint_store_name: {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "InMemoryStoreBackend",
                },
            },
            self.profiler_store_name: {
                "class_name": "ProfilerStore",
                "store_backend": {
                    "class_name": "InMemoryStoreBackend",
                },
            },
        }
        if init_temp_docs_sites:
            temp_dir = tempfile.TemporaryDirectory()
            path = temp_dir.name
            logger.info(f"Created temporary directory '{path}' for ephemeral docs site")
            self.data_docs_sites[  # type: ignore[index]
                DataContextConfigDefaults.DEFAULT_DATA_DOCS_SITE_NAME.value
            ]["store_backend"]["base_directory"] = path
        else:
            self.data_docs_sites = {}


class GCSStoreBackendDefaults(BaseStoreBackendDefaults):
    """
    Default store configs for Google Cloud Storage (GCS) backends, with some accessible parameters
    Args:
        default_bucket_name: Use this bucket name for stores that do not have a bucket name provided
        default_project_name: Use this project name for stores that do not have a project name provided
        expectations_store_bucket_name: Overrides default_bucket_name if supplied
        validations_store_bucket_name: Overrides default_bucket_name if supplied
        data_docs_bucket_name: Overrides default_bucket_name if supplied
        checkpoint_store_bucket_name: Overrides default_bucket_name if supplied
        profiler_store_bucket_name: Overrides default_bucket_name if supplied
        expectations_store_project_name: Overrides default_project_name if supplied
        validations_store_project_name: Overrides default_project_name if supplied
        data_docs_project_name: Overrides default_project_name if supplied
        checkpoint_store_project_name: Overrides default_project_name if supplied
        profiler_store_project_name: Overrides default_project_name if supplied
        expectations_store_prefix: Overrides default if supplied
        validations_store_prefix: Overrides default if supplied
        data_docs_prefix: Overrides default if supplied
        checkpoint_store_prefix: Overrides default if supplied
        profiler_store_prefix: Overrides default if supplied
        expectations_store_name: Overrides default if supplied
        validations_store_name: Overrides default if supplied
        evaluation_parameter_store_name: Overrides default if supplied
        checkpoint_store_name: Overrides default if supplied
        profiler_store_name: Overrides default if supplied
    """

    def __init__(  # noqa: PLR0913
        self,
        default_bucket_name: Optional[str] = None,
        default_project_name: Optional[str] = None,
        expectations_store_bucket_name: Optional[str] = None,
        validations_store_bucket_name: Optional[str] = None,
        data_docs_bucket_name: Optional[str] = None,
        checkpoint_store_bucket_name: Optional[str] = None,
        profiler_store_bucket_name: Optional[str] = None,
        expectations_store_project_name: Optional[str] = None,
        validations_store_project_name: Optional[str] = None,
        data_docs_project_name: Optional[str] = None,
        checkpoint_store_project_name: Optional[str] = None,
        profiler_store_project_name: Optional[str] = None,
        expectations_store_prefix: str = "expectations",
        validations_store_prefix: str = "validations",
        data_docs_prefix: str = "data_docs",
        checkpoint_store_prefix: str = "checkpoints",
        profiler_store_prefix: str = "profilers",
        expectations_store_name: str = "expectations_GCS_store",
        validations_store_name: str = "validations_GCS_store",
        evaluation_parameter_store_name: str = "evaluation_parameter_store",
        checkpoint_store_name: str = "checkpoint_GCS_store",
        profiler_store_name: str = "profiler_GCS_store",
    ) -> None:
        # Initialize base defaults
        super().__init__()

        # Use default_bucket_name if separate store buckets are not provided
        if expectations_store_bucket_name is None:
            expectations_store_bucket_name = default_bucket_name
        if validations_store_bucket_name is None:
            validations_store_bucket_name = default_bucket_name
        if data_docs_bucket_name is None:
            data_docs_bucket_name = default_bucket_name
        if checkpoint_store_bucket_name is None:
            checkpoint_store_bucket_name = default_bucket_name
        if profiler_store_bucket_name is None:
            profiler_store_bucket_name = default_bucket_name

        # Use default_project_name if separate store projects are not provided
        if expectations_store_project_name is None:
            expectations_store_project_name = default_project_name
        if validations_store_project_name is None:
            validations_store_project_name = default_project_name
        if data_docs_project_name is None:
            data_docs_project_name = default_project_name
        if checkpoint_store_project_name is None:
            checkpoint_store_project_name = default_project_name
        if profiler_store_project_name is None:
            profiler_store_project_name = default_project_name

        # Overwrite defaults
        self.expectations_store_name = expectations_store_name
        self.validations_store_name = validations_store_name
        self.evaluation_parameter_store_name = evaluation_parameter_store_name
        self.checkpoint_store_name = checkpoint_store_name
        self.profiler_store_name = profiler_store_name
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
            validations_store_name: {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleGCSStoreBackend",
                    "project": validations_store_project_name,
                    "bucket": validations_store_bucket_name,
                    "prefix": validations_store_prefix,
                },
            },
            evaluation_parameter_store_name: {"class_name": "EvaluationParameterStore"},
            checkpoint_store_name: {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "TupleGCSStoreBackend",
                    "project": checkpoint_store_project_name,
                    "bucket": checkpoint_store_bucket_name,
                    "prefix": checkpoint_store_prefix,
                },
            },
            profiler_store_name: {
                "class_name": "ProfilerStore",
                "store_backend": {
                    "class_name": "TupleGCSStoreBackend",
                    "project": profiler_store_project_name,
                    "bucket": profiler_store_bucket_name,
                    "prefix": profiler_store_prefix,
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
        validations_store_credentials: Overrides default_credentials if supplied
        checkpoint_store_credentials: Overrides default_credentials if supplied
        profiler_store_credentials: Overrides default_credentials if supplied
        expectations_store_name: Overrides default if supplied
        validations_store_name: Overrides default if supplied
        evaluation_parameter_store_name: Overrides default if supplied
        checkpoint_store_name: Overrides default if supplied
        profiler_store_name: Overrides default if supplied
    """

    def __init__(  # noqa: PLR0913
        self,
        default_credentials: Optional[Dict] = None,
        expectations_store_credentials: Optional[Dict] = None,
        validations_store_credentials: Optional[Dict] = None,
        checkpoint_store_credentials: Optional[Dict] = None,
        profiler_store_credentials: Optional[Dict] = None,
        expectations_store_name: str = "expectations_database_store",
        validations_store_name: str = "validations_database_store",
        evaluation_parameter_store_name: str = "evaluation_parameter_store",
        checkpoint_store_name: str = "checkpoint_database_store",
        profiler_store_name: str = "profiler_database_store",
    ) -> None:
        # Initialize base defaults
        super().__init__()

        # Use default credentials if separate credentials not supplied for expectations_store and validations_store
        if expectations_store_credentials is None:
            expectations_store_credentials = default_credentials
        if validations_store_credentials is None:
            validations_store_credentials = default_credentials
        if checkpoint_store_credentials is None:
            checkpoint_store_credentials = default_credentials
        if profiler_store_credentials is None:
            profiler_store_credentials = default_credentials

        # Overwrite defaults
        self.expectations_store_name = expectations_store_name
        self.validations_store_name = validations_store_name
        self.evaluation_parameter_store_name = evaluation_parameter_store_name
        self.checkpoint_store_name = checkpoint_store_name
        self.profiler_store_name = profiler_store_name

        self.stores = {
            expectations_store_name: {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "DatabaseStoreBackend",
                    "credentials": expectations_store_credentials,
                },
            },
            validations_store_name: {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "DatabaseStoreBackend",
                    "credentials": validations_store_credentials,
                },
            },
            evaluation_parameter_store_name: {"class_name": "EvaluationParameterStore"},
            checkpoint_store_name: {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "DatabaseStoreBackend",
                    "credentials": checkpoint_store_credentials,
                },
            },
            profiler_store_name: {
                "class_name": "ProfilerStore",
                "store_backend": {
                    "class_name": "DatabaseStoreBackend",
                    "credentials": profiler_store_credentials,
                },
            },
        }


@public_api
@deprecated_argument(argument_name="validation_operators", version="0.14.0")
class DataContextConfig(BaseYamlConfig):
    """Config class for DataContext.

    The DataContextConfig holds all associated configuration parameters to build a Data Context. There are defaults set
    for minimizing configuration in typical cases, but every parameter is configurable and all defaults are overridable.

    In cases where the DataContext is instantitated without a yml file, the DataContextConfig can be instantiated directly.

    --Documentation--
        - https://docs.greatexpectations.io/docs/guides/setup/configuring_data_contexts/how_to_instantiate_a_data_context_without_a_yml_file/

    Args:
        config_version (Optional[float]): config version of this DataContext.
        datasources (Optional[Union[Dict[str, DatasourceConfig], Dict[str, Dict[str, Union[Dict[str, str], str, dict]]]]):
            DatasourceConfig or Dict containing configurations for Datasources associated with DataContext.
        fluent_datasources (Optional[dict]): temporary placeholder for Experimental Datasources.
        expectations_store_name (Optional[str]): name of ExpectationStore to be used by DataContext.
        validations_store_name (Optional[str]): name of ValidationsStore to be used by DataContext.
        evaluation_parameter_store_name (Optional[str]): name of EvaluationParamterStore to be used by DataContext.
        checkpoint_store_name (Optional[str]): name of CheckpointStore to be used by DataContext.
        profiler_store_name (Optional[str]): name of ProfilerStore to be used by DataContext.
        plugins_directory (Optional[str]): the directory in which custom plugin modules should be placed.
        validation_operators: list of validation operators configured by this DataContext.
        stores (Optional[dict]): single holder for all Stores associated with this DataContext.
        data_docs_sites (Optional[dict]): DataDocs sites associated with DataContext.
        notebooks (Optional[NotebookConfig]): Configurations for Jupyter Notebooks associated with DataContext, such as
            the `suite_edit` Notebook.
        config_variables_file_path (Optional[str]): path for config_variables file, if used.
        anonymous_usage_statistics (Optional[AnonymizedUsageStatisticsConfig]): configuration for enabling or disabling
            anonymous usage statistics for GX.
        store_backend_defaults (Optional[BaseStoreBackendDefaults]):  define base defaults for platform specific StoreBackendDefaults.
            For example, if you plan to store expectations, validations, and data_docs in s3 use the S3StoreBackendDefaults
            and you may be able to specify fewer parameters.
        commented_map (Optional[CommentedMap]): the CommentedMap associated with DataContext configuration. Used when
            instantiating with yml file.
        concurrency (Optional[Union[ConcurrencyConfig, Dict]]): if enabled, Checkpoints associated with the DataContext
            can run validations in parallel with multithreading.
        progress_bars (Optional[ProgressBarsConfig]): allows progress_bars to be enabled or disabled globally, for
            profilers, or metrics calculations.
        include_rendered_content (Optional[IncludedRenderedContentConfig]): allows rendered content to be configured
            globally, at the ExpectationSuite or ExpectationValidationResults-level.
    """

    # TODO: <Alex>ALEX (does not work yet)</Alex>
    # _config_schema_class = DataContextConfigSchema

    def __init__(  # noqa: C901, PLR0912, PLR0913, PLR0915
        self,
        config_version: Optional[float] = None,
        datasources: Optional[
            Union[
                Dict[str, DatasourceConfig],
                Dict[str, Dict[str, Union[Dict[str, str], str, dict]]],
            ]
        ] = None,
        fluent_datasources: Optional[dict] = None,
        expectations_store_name: Optional[str] = None,
        validations_store_name: Optional[str] = None,
        evaluation_parameter_store_name: Optional[str] = None,
        checkpoint_store_name: Optional[str] = None,
        profiler_store_name: Optional[str] = None,
        plugins_directory: Optional[str] = None,
        validation_operators=None,
        stores: Optional[Dict] = None,
        data_docs_sites: Optional[Dict] = None,
        notebooks: Optional[NotebookConfig] = None,
        config_variables_file_path: Optional[str] = None,
        anonymous_usage_statistics: Optional[AnonymizedUsageStatisticsConfig] = None,
        store_backend_defaults: Optional[BaseStoreBackendDefaults] = None,
        commented_map: Optional[CommentedMap] = None,
        concurrency: Optional[Union[ConcurrencyConfig, Dict]] = None,
        progress_bars: Optional[ProgressBarsConfig] = None,
        include_rendered_content: Optional[IncludeRenderedContentConfig] = None,
    ) -> None:
        # Set defaults
        if config_version is None:
            config_version = DataContextConfigDefaults.DEFAULT_CONFIG_VERSION.value

        # Set defaults via store_backend_defaults if one is passed in
        # Override attributes from store_backend_defaults with any items passed into the constructor:
        if store_backend_defaults is not None:
            if stores is None:
                stores = store_backend_defaults.stores
            if expectations_store_name is None:
                expectations_store_name = store_backend_defaults.expectations_store_name
            if validations_store_name is None:
                validations_store_name = store_backend_defaults.validations_store_name
            if evaluation_parameter_store_name is None:
                evaluation_parameter_store_name = (
                    store_backend_defaults.evaluation_parameter_store_name
                )
            if data_docs_sites is None:
                data_docs_sites = store_backend_defaults.data_docs_sites
            if checkpoint_store_name is None:
                checkpoint_store_name = store_backend_defaults.checkpoint_store_name
            if profiler_store_name is None:
                profiler_store_name = store_backend_defaults.profiler_store_name

        self._config_version = config_version
        if datasources is None:
            datasources = {}  # type: ignore[assignment]
        self.datasources = datasources
        self.fluent_datasources = fluent_datasources or {}
        self.expectations_store_name = expectations_store_name
        self.validations_store_name = validations_store_name
        self.evaluation_parameter_store_name = evaluation_parameter_store_name
        if checkpoint_store_name is not None:
            self.checkpoint_store_name = checkpoint_store_name
        if profiler_store_name is not None:
            self.profiler_store_name = profiler_store_name
        self.plugins_directory = plugins_directory
        if validation_operators is not None:
            self.validation_operators = validation_operators
        self.stores = stores or {}
        self.notebooks = notebooks
        self.data_docs_sites = data_docs_sites
        self.config_variables_file_path = config_variables_file_path
        if anonymous_usage_statistics is None:
            anonymous_usage_statistics = AnonymizedUsageStatisticsConfig()
        elif isinstance(anonymous_usage_statistics, dict):
            anonymous_usage_statistics = AnonymizedUsageStatisticsConfig(
                **anonymous_usage_statistics
            )
        self.anonymous_usage_statistics = anonymous_usage_statistics
        if isinstance(concurrency, dict):
            concurrency = ConcurrencyConfig(**concurrency)
        self.concurrency = concurrency
        self.progress_bars = progress_bars
        if include_rendered_content is None:
            include_rendered_content = IncludeRenderedContentConfig()
        elif isinstance(include_rendered_content, dict):
            include_rendered_content = IncludeRenderedContentConfig(
                **include_rendered_content
            )
        self.include_rendered_content = include_rendered_content

        super().__init__(commented_map=commented_map)

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
    def to_json_dict(self) -> Dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this DataContextConfig.

        Returns:
            A JSON-serializable dict representation of this DataContextConfig.
        """
        # TODO: <Alex>2/4/2022</Alex>
        # This implementation of "SerializableDictDot.to_json_dict() occurs frequently and should ideally serve as the
        # reference implementation in the "SerializableDictDot" class itself.  However, the circular import dependencies,
        # due to the location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules
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

    def __repr__(self) -> str:
        """
        # TODO: <Alex>2/4/2022</Alex>
        This implementation of a custom "__repr__()" occurs frequently and should ideally serve as the reference
        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the
        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this
        refactoring infeasible at the present time.
        """
        json_dict: dict = self.to_sanitized_json_dict()
        deep_filter_properties_iterable(
            properties=json_dict,
            inplace=True,
        )

        keys: List[str] = sorted(list(json_dict.keys()))

        key: str
        sorted_json_dict: dict = {key: json_dict[key] for key in keys}

        return json.dumps(sorted_json_dict, indent=2)

    def __str__(self) -> str:
        """
        # TODO: <Alex>2/4/2022</Alex>
        This implementation of a custom "__str__()" occurs frequently and should ideally serve as the reference
        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the
        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this
        refactoring infeasible at the present time.
        """
        return self.__repr__()


class CheckpointValidationConfig(AbstractConfig):
    def __init__(
        self,
        id: str | None = None,
        expectation_suite_name: str | None = None,
        expectation_suite_ge_cloud_id: str | None = None,
        batch_request: BatchRequestBase | FluentBatchRequest | dict | None = None,
        **kwargs,
    ) -> None:
        self.expectation_suite_name = expectation_suite_name
        self.expectation_suite_ge_cloud_id = expectation_suite_ge_cloud_id
        self.batch_request = batch_request
        super().__init__(id=id)

        for k, v in kwargs.items():
            setattr(self, k, v)


class CheckpointValidationConfigSchema(AbstractConfigSchema):
    class Meta:
        unknown = INCLUDE

    id = fields.String(required=False, allow_none=False)

    def dump(self, obj: dict, *, many: Optional[bool] = None) -> dict:
        """
        Chetan - 20220803 - By design, Marshmallow accepts unknown fields through the
        `unknown = INCLUDE` directive but only upon load. When dumping, it validates
        each item against the declared fields and only includes explicitly named values.

        As such, this override of parent behavior is meant to keep ALL values provided
        to the config in the output dict. To get rid of this function, we need to
        explicitly name all possible values in CheckpoingValidationConfigSchema as
        schema fields.
        """
        data = super().dump(obj, many=many)

        for key, value in obj.items():
            if (
                key not in data
                and key not in self.declared_fields
                and value is not None
            ):
                data[key] = value

        sorted_data = dict(sorted(data.items()))
        return sorted_data

    @pre_dump
    def prepare_dump(self, data, **kwargs):
        data = copy.deepcopy(data)
        for key, value in data.items():
            data[key] = convert_to_json_serializable(data=value)
        return data


class CheckpointConfigSchema(Schema):
    class Meta:
        unknown = INCLUDE
        fields = (
            "name",
            "config_version",
            "template_name",
            "module_name",
            "class_name",
            "run_name_template",
            "expectation_suite_name",
            "batch_request",
            "action_list",
            "evaluation_parameters",
            "runtime_configuration",
            "validations",
            "default_validation_id",
            "profilers",
            # Next fields are used by configurators
            "site_names",
            "slack_webhook",
            "notify_on",
            "notify_with",
            "ge_cloud_id",
            "expectation_suite_ge_cloud_id",
        )
        ordered = True

    # if keys have None value, remove in post_dump
    REMOVE_KEYS_IF_NONE = [
        "site_names",
        "slack_webhook",
        "notify_on",
        "notify_with",
        "default_validation_id",
    ]

    ge_cloud_id = fields.UUID(required=False, allow_none=True)
    name = fields.String(required=False, allow_none=True)
    config_version = fields.Number(
        validate=lambda x: (0 < x < 100) or x is None,  # noqa: PLR2004
        error_messages={"invalid": "config version must " "be a number or None."},
        required=False,
        allow_none=True,
    )
    template_name = fields.String(required=False, allow_none=True)
    class_name = fields.Str(
        required=False,
        allow_none=True,
    )
    module_name = fields.String(
        required=False,
        allow_none=True,
        missing="great_expectations.checkpoint",
    )
    run_name_template = fields.String(required=False, allow_none=True)
    expectation_suite_name = fields.String(required=False, allow_none=True)
    expectation_suite_ge_cloud_id = fields.UUID(required=False, allow_none=True)
    batch_request = fields.Dict(required=False, allow_none=True)
    action_list = fields.List(
        cls_or_instance=fields.Dict(), required=False, allow_none=True
    )
    evaluation_parameters = fields.Dict(required=False, allow_none=True)
    runtime_configuration = fields.Dict(required=False, allow_none=True)
    validations = fields.List(
        cls_or_instance=fields.Nested(CheckpointValidationConfigSchema),
        required=False,
        allow_none=True,
    )
    default_validation_id = fields.String(required=False, allow_none=True)

    profilers = fields.List(
        cls_or_instance=fields.Dict(), required=False, allow_none=True
    )
    # Next fields are used by configurators
    site_names = fields.Raw(required=False, allow_none=True)
    slack_webhook = fields.String(required=False, allow_none=True)
    notify_on = fields.String(required=False, allow_none=True)
    notify_with = fields.String(required=False, allow_none=True)

    # noinspection PyUnusedLocal
    @validates_schema
    def validate_schema(self, data, **kwargs) -> None:
        if not (
            "name" in data or "validation_operator_name" in data or "batches" in data
        ):
            raise gx_exceptions.InvalidConfigError(
                """Your current Checkpoint configuration is incomplete.  Please update your Checkpoint configuration to
                continue.
                """
            )

        if data.get("config_version"):
            if "name" not in data:
                raise gx_exceptions.InvalidConfigError(
                    """Your Checkpoint configuration requires the "name" field.  Please update your current Checkpoint
                    configuration to continue.
                    """
                )

    # noinspection PyUnusedLocal
    @pre_dump
    def prepare_dump(self, data, **kwargs):
        data = copy.deepcopy(data)
        for key, value in data.items():
            data[key] = convert_to_json_serializable(data=value)

        return data

    # noinspection PyUnusedLocal
    @post_dump
    def remove_keys_if_none(self, data, **kwargs):
        data = copy.deepcopy(data)
        for key in self.REMOVE_KEYS_IF_NONE:
            if key in data and data[key] is None:
                data.pop(key)

        return data


@public_api
class CheckpointConfig(BaseYamlConfig):
    # TODO: <Alex>ALEX (does not work yet)</Alex>
    # _config_schema_class = CheckpointConfigSchema
    """Initializes the CheckpointConfig using a BaseYamlConfig.

    Args:
        name: The name of the checkpoint.
        config_version: Your config version
        template_name: The template name of your checkpoint
        module_name: The module name used for your checkpoint
        class_name: The class name of your checkpoint
        run_name_template: The run template name
        expectation_suite_name: The expectation suite name of your checkpoint
        batch_request: The batch request
        action_list: The action list
        evaluation_parameters: The evaluation parameters
        runtime_configuration: The runtime configuration for your checkpoint
        validations: An optional list of validations in your checkpoint
        default_validation_id: The default validation id of your checkpoint
        profilers: An optional list of profilers in your checkpoint
        validation_operator_name: The validation operator name
        batches: An optional list of batches
        commented_map: The commented map
        ge_cloud_id: Your GE Cloud ID
        site_names: The site names
        slack_webhook: The slack webhook
        notify_on: The notify on
        notify_with: The notify with
        expectation_suite_ge_cloud_id: Your expectation suite
    """

    def __init__(  # noqa: PLR0913
        self,
        name: Optional[str] = None,
        config_version: Union[int, float] = 1.0,
        template_name: Optional[str] = None,
        module_name: str = "great_expectations.checkpoint",
        class_name: str = "Checkpoint",
        run_name_template: Optional[str] = None,
        expectation_suite_name: Optional[str] = None,
        batch_request: Optional[dict] = None,
        action_list: Optional[Sequence[ActionDict]] = None,
        evaluation_parameters: Optional[dict] = None,
        runtime_configuration: Optional[dict] = None,
        validations: Optional[List[dict]] = None,
        default_validation_id: Optional[str] = None,
        profilers: Optional[List[dict]] = None,
        commented_map: Optional[CommentedMap] = None,
        ge_cloud_id: Optional[str] = None,
        # the following four args are used by SimpleCheckpoint
        site_names: Optional[Union[list, str]] = None,
        slack_webhook: Optional[str] = None,
        notify_on: Optional[str] = None,
        notify_with: Optional[str] = None,
        expectation_suite_ge_cloud_id: Optional[str] = None,
    ) -> None:
        self._name = name
        self._config_version = config_version
        self._template_name = template_name
        self._run_name_template = run_name_template
        self._expectation_suite_name = expectation_suite_name
        self._expectation_suite_ge_cloud_id = expectation_suite_ge_cloud_id
        self._batch_request = batch_request or {}
        self._action_list = action_list or []
        self._evaluation_parameters = evaluation_parameters or {}
        self._runtime_configuration = runtime_configuration or {}
        self._validations = validations or []
        self._default_validation_id = default_validation_id
        self._profilers = profilers or []
        self._ge_cloud_id = ge_cloud_id
        # the following attributes are used by SimpleCheckpoint
        self._site_names = site_names
        self._slack_webhook = slack_webhook
        self._notify_on = notify_on
        self._notify_with = notify_with

        self._module_name = module_name or "great_expectations.checkpoint"
        self._class_name = class_name or "Checkpoint"

        super().__init__(commented_map=commented_map)

    # TODO: <Alex>ALEX (we still need the next two properties)</Alex>
    @classmethod
    def get_config_class(cls):
        return cls  # CheckpointConfig

    @classmethod
    def get_schema_class(cls):
        return CheckpointConfigSchema

    @property
    def ge_cloud_id(self) -> Optional[str]:
        return self._ge_cloud_id

    @ge_cloud_id.setter
    def ge_cloud_id(self, value: str) -> None:
        self._ge_cloud_id = value

    @property
    def expectation_suite_ge_cloud_id(self) -> Optional[str]:
        return self._expectation_suite_ge_cloud_id

    @expectation_suite_ge_cloud_id.setter
    def expectation_suite_ge_cloud_id(self, value: str) -> None:
        self._expectation_suite_ge_cloud_id = value

    @property
    def name(self) -> str:
        return self._name  # type: ignore[return-value]

    @name.setter
    def name(self, value: str) -> None:
        self._name = value

    @property
    def template_name(self) -> str:
        return self._template_name  # type: ignore[return-value]

    @template_name.setter
    def template_name(self, value: str) -> None:
        self._template_name = value

    @property
    def config_version(self) -> Union[int, float]:
        return self._config_version

    @config_version.setter
    def config_version(self, value: float) -> None:
        self._config_version = value

    @property
    def validations(self) -> List[dict]:
        return self._validations

    @validations.setter
    def validations(self, value: List[dict]) -> None:
        self._validations = value

    @property
    def default_validation_id(self) -> Optional[str]:
        return self._default_validation_id

    @default_validation_id.setter
    def default_validation_id(self, validation_id: str) -> None:
        self._default_validation_id = validation_id

    @property
    def profilers(self) -> List[dict]:
        return self._profilers

    @profilers.setter
    def profilers(self, value: List[dict]) -> None:
        self._profilers = value

    @property
    def module_name(self) -> str:
        return self._module_name

    @module_name.setter
    def module_name(self, value: str) -> None:
        self._module_name = value

    @property
    def class_name(self) -> str:
        return self._class_name

    @class_name.setter
    def class_name(self, value: str) -> None:
        self._class_name = value

    @property
    def run_name_template(self) -> str:
        return self._run_name_template  # type: ignore[return-value]

    @run_name_template.setter
    def run_name_template(self, value: str) -> None:
        self._run_name_template = value

    @property
    def batch_request(self) -> dict:
        return self._batch_request

    @batch_request.setter
    def batch_request(self, value: dict) -> None:
        self._batch_request = value

    @property
    def expectation_suite_name(self) -> str:
        return self._expectation_suite_name  # type: ignore[return-value]

    @expectation_suite_name.setter
    def expectation_suite_name(self, value: str) -> None:
        self._expectation_suite_name = value

    @property
    def action_list(self) -> Sequence[ActionDict]:
        return self._action_list

    @action_list.setter
    def action_list(self, value: Sequence[ActionDict]) -> None:
        self._action_list = value

    @property
    def site_names(self) -> List[str]:
        return self._site_names  # type: ignore[return-value]

    @site_names.setter
    def site_names(self, value: List[str]) -> None:
        self._site_names = value

    @property
    def slack_webhook(self) -> str:
        return self._slack_webhook  # type: ignore[return-value]

    @slack_webhook.setter
    def slack_webhook(self, value: str) -> None:
        self._slack_webhook = value

    @property
    def notify_on(self) -> str:
        return self._notify_on  # type: ignore[return-value]

    @notify_on.setter
    def notify_on(self, value: str) -> None:
        self._notify_on = value

    @property
    def notify_with(self) -> str:
        return self._notify_with  # type: ignore[return-value]

    @notify_with.setter
    def notify_with(self, value: str) -> None:
        self._notify_with = value

    @property
    def evaluation_parameters(self) -> dict:
        return self._evaluation_parameters

    @evaluation_parameters.setter
    def evaluation_parameters(self, value: dict) -> None:
        self._evaluation_parameters = value

    @property
    def runtime_configuration(self) -> dict:
        return self._runtime_configuration

    @runtime_configuration.setter
    def runtime_configuration(self, value: dict) -> None:
        self._runtime_configuration = value

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)

        memo[id(self)] = result

        attributes_to_copy = set(CheckpointConfigSchema().fields.keys())
        for key in attributes_to_copy:
            try:
                value = self[key]
                value_copy = safe_deep_copy(data=value, memo=memo)
                setattr(result, key, value_copy)
            except AttributeError:
                pass

        return result

    @public_api
    def to_json_dict(self) -> Dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this CheckpointConfig.

        Returns:
            A JSON-serializable dict representation of this CheckpointConfig.
        """
        # # TODO: <Alex>2/4/2022</Alex>
        # This implementation of "SerializableDictDot.to_json_dict() occurs frequently and should ideally serve as the
        # reference implementation in the "SerializableDictDot" class itself.  However, the circular import dependencies,
        # due to the location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules
        # make this refactoring infeasible at the present time.
        dict_obj: dict = self.to_dict()
        serializeable_dict: dict = convert_to_json_serializable(data=dict_obj)
        return serializeable_dict

    def __repr__(self) -> str:
        """
        # TODO: <Alex>2/4/2022</Alex>
        This implementation of a custom "__repr__()" occurs frequently and should ideally serve as the reference
        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the
        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this
        refactoring infeasible at the present time.
        """
        json_dict: dict = self.to_json_dict()
        deep_filter_properties_iterable(
            properties=json_dict,
            inplace=True,
        )

        keys: List[str] = sorted(list(json_dict.keys()))

        key: str
        sorted_json_dict: dict = {key: json_dict[key] for key in keys}

        return json.dumps(sorted_json_dict, indent=2)

    def __str__(self) -> str:
        """
        # TODO: <Alex>2/4/2022</Alex>
        This implementation of a custom "__str__()" occurs frequently and should ideally serve as the reference
        implementation in the "SerializableDictDot" class.  However, the circular import dependencies, due to the
        location of the "great_expectations/types/__init__.py" and "great_expectations/core/util.py" modules make this
        refactoring infeasible at the present time.
        """
        return self.__repr__()

    # noinspection PyUnusedLocal,PyUnresolvedReferences
    @staticmethod
    def resolve_config_using_acceptable_arguments(  # noqa: PLR0913
        checkpoint: Checkpoint,
        template_name: Optional[str] = None,
        run_name_template: Optional[str] = None,
        expectation_suite_name: Optional[str] = None,
        batch_request: Optional[Union[BatchRequestBase, dict]] = None,
        validator: Optional[Validator] = None,
        action_list: Optional[List[dict]] = None,
        evaluation_parameters: Optional[dict] = None,
        runtime_configuration: Optional[dict] = None,
        validations: Optional[
            Union[List[dict], List[CheckpointValidationConfig]]
        ] = None,
        profilers: Optional[List[dict]] = None,
        run_id: Optional[Union[str, RunIdentifier]] = None,
        run_name: Optional[str] = None,
        run_time: Optional[Union[str, datetime.datetime]] = None,
        result_format: Optional[Union[str, dict]] = None,
        expectation_suite_ge_cloud_id: Optional[str] = None,
    ) -> dict:
        """
        This method reconciles the Checkpoint configuration (e.g., obtained from the Checkpoint store) with dynamically
        supplied arguments in order to obtain that Checkpoint specification that is ready for running validation on it.
        This procedure is necessecitated by the fact that the Checkpoint configuration is hierarchical in its form,
        which was established for the purposes of making the specification of different Checkpoint capabilities easy.
        In particular, entities, such as BatchRequest, expectation_suite_name, and action_list, can be specified at the
        top Checkpoint level with the suitable ovverrides provided at lower levels (e.g., in the validations section).
        Reconciling and normalizing the Checkpoint configuration is essential for usage statistics, because the exact
        values of the entities in their formally validated form (e.g., BatchRequest) is the required level of detail.
        """
        assert not (run_id and run_name) and not (
            run_id and run_time
        ), "Please provide either a run_id or run_name and/or run_time."

        run_time = run_time or datetime.datetime.now(tz=datetime.timezone.utc)
        runtime_configuration = runtime_configuration or {}

        from great_expectations.checkpoint.util import (
            get_substituted_validation_dict,
            get_validations_with_batch_request_as_dict,
            validate_validation_dict,
        )

        batch_request = get_batch_request_as_dict(batch_request=batch_request)

        validations = get_validations_with_batch_request_as_dict(
            validations=validations
        )

        runtime_kwargs: dict = {
            "template_name": template_name,
            "run_name_template": run_name_template,
            "expectation_suite_name": expectation_suite_name,
            "batch_request": batch_request,
            "validator": validator,
            "action_list": action_list,
            "evaluation_parameters": evaluation_parameters,
            "runtime_configuration": runtime_configuration,
            "validations": validations,
            "profilers": profilers,
            "expectation_suite_ge_cloud_id": expectation_suite_ge_cloud_id,
        }
        substituted_runtime_config: dict = checkpoint.get_substituted_config(
            runtime_kwargs=runtime_kwargs
        )
        run_name_template = substituted_runtime_config.get("run_name_template")
        validations = substituted_runtime_config.get("validations") or []
        batch_request = substituted_runtime_config.get("batch_request")
        if len(validations) == 0 and not batch_request:
            raise gx_exceptions.CheckpointError(
                f'Checkpoint "{checkpoint.name}" configuration must contain either a batch_request or validations.'
            )

        if run_name is None and run_name_template is not None:
            if isinstance(run_time, datetime.datetime):
                run_name = run_time.strftime(run_name_template)

        run_id = run_id or RunIdentifier(run_name=run_name, run_time=run_time)

        validation_dict: dict

        for validation_dict in validations:  # type: ignore[assignment]
            substituted_validation_dict: dict = get_substituted_validation_dict(
                substituted_runtime_config=substituted_runtime_config,
                validation_dict=validation_dict,
            )
            validate_validation_dict(
                validation_dict=substituted_validation_dict,
                batch_request_required=False,
            )
            validation_batch_request: BatchRequestBase = substituted_validation_dict.get(  # type: ignore[assignment]
                "batch_request"
            )
            validation_dict["batch_request"] = validation_batch_request
            validation_expectation_suite_name: str = substituted_validation_dict.get(  # type: ignore[assignment]
                "expectation_suite_name"
            )
            validation_dict[
                "expectation_suite_name"
            ] = validation_expectation_suite_name
            validation_expectation_suite_ge_cloud_id: str = substituted_validation_dict.get(  # type: ignore[assignment]
                "expectation_suite_ge_cloud_id"
            )
            validation_dict[
                "expectation_suite_ge_cloud_id"
            ] = validation_expectation_suite_ge_cloud_id
            validation_action_list: list = substituted_validation_dict.get(  # type: ignore[assignment]
                "action_list"
            )
            validation_dict["action_list"] = validation_action_list

        return substituted_runtime_config


dataContextConfigSchema = DataContextConfigSchema()
datasourceConfigSchema = DatasourceConfigSchema()
dataConnectorConfigSchema = DataConnectorConfigSchema()
executionEngineConfigSchema = ExecutionEngineConfigSchema()
assetConfigSchema = AssetConfigSchema()
sorterConfigSchema = SorterConfigSchema()
# noinspection SpellCheckingInspection
anonymizedUsageStatisticsSchema = AnonymizedUsageStatisticsConfigSchema()
notebookConfigSchema = NotebookConfigSchema()
checkpointConfigSchema = CheckpointConfigSchema()
concurrencyConfigSchema = ConcurrencyConfigSchema()
progressBarsConfigSchema = ProgressBarsConfigSchema()
