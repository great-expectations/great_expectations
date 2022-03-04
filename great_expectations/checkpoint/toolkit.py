import datetime
import itertools
import os
import uuid
from typing import List, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.checkpoint import Checkpoint, LegacyCheckpoint, SimpleCheckpoint
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.checkpoint.util import (
    batch_request_in_validations_contains_batch_data,
    get_validations_with_batch_request_as_dict,
)
from great_expectations.core.batch import (
    BatchRequest,
    RuntimeBatchRequest,
    batch_request_contains_batch_data,
    get_batch_request_as_dict,
)
from great_expectations.data_context.store import CheckpointStore
from great_expectations.data_context.types.base import (
    CheckpointConfig,
    DataContextConfigDefaults,
)
from great_expectations.data_context.types.refs import GeCloudIdAwareRef
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    GeCloudIdentifier,
)
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.marshmallow__shade import ValidationError
from great_expectations.util import (
    deep_filter_properties_iterable,
    filter_properties_dict,
)


def default_checkpoints_exist(directory_path: str) -> bool:
    if not directory_path:
        return False

    checkpoints_directory_path: str = os.path.join(
        directory_path,
        DataContextConfigDefaults.DEFAULT_CHECKPOINT_STORE_BASE_DIRECTORY_RELATIVE_NAME.value,
    )
    return os.path.isdir(checkpoints_directory_path)
