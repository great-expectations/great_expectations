import logging
from abc import ABC
from dataclasses import dataclass

from great_expectations.types import SerializableDictDot

logger = logging.getLogger(__name__)


class BatchIdentifierException(BaseException):
    # !!! Do we want to create a class for this? Is this the right name and inheritance?
    pass

class GxExperimentalWarning(Warning):
    pass


class BatchIdentifiers(dict, SerializableDictDot):
    pass


class PassthroughParameters(dict, SerializableDictDot):
    pass

class GxData(object):
    pass

@dataclass
class NewBatchRequestBase(SerializableDictDot, ABC):

    datasource_name: str
    data_asset_name: str
    batch_identifiers: BatchIdentifiers


@dataclass
class NewConfiguredBatchRequest(NewBatchRequestBase):
    passthrough_parameters: PassthroughParameters


@dataclass
class NewRuntimeBatchRequest(NewBatchRequestBase):
    data: GxData

