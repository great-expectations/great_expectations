import logging
from abc import ABC
from dataclasses import dataclass

from great_expectations.types import SerializableDictDot

logger = logging.getLogger(__name__)


class BatchIdentifierException(BaseException):
    # !!! Do we want to create a class for this? Is this the right name and inheritance?
    pass

class BatchIdentifiers(dict, SerializableDictDot):
    pass


class PassthroughParameters(dict, SerializableDictDot):
    pass
    """
    !!! For full feature parity with previous versions, this will need to support these 5 (or maybe 4) parameters.
    batch_filter_parameters : {
        "airflow_run_id": "string_airflow_run_id_that_was_provided",
        "other_key": "string_other_key_that_was_provided",
    limit: 10
    index: Optional[Union[int, list, tuple, slice, str]],  # examples: 0; "-1"; [3:7]; "[2:4]"
    partition_index #!!! Unclear if this is different from index. Needs investigation
    custom_filter_function: my_filter_fn #!!! In order for this to be serializable, we're probably going to need to implement a filter_fn registry.
    """


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

    def __str__(self) -> str:
        return f"""NewConfiguredBatchRequest(
    datasource_name='{self.datasource_name}',
    data_asset_name='{self.data_asset_name}',
    batch_identifiers='{self.batch_identifiers.__str__()}',
    passthrough_parameters='{self.passthrough_parameters.__str__()}',
)
"""
    
@dataclass
class NewRuntimeBatchRequest(NewBatchRequestBase):
    data: GxData

    def __eq__(self, other) -> str:
        return all(
            [
                self["datasource_name"] == other["datasource_name"],
                self["data_asset_name"] == other["data_asset_name"],
                self["batch_identifiers"] == other["batch_identifiers"],
                # self["data"] == other["data"], # !!!
            ]
        )

    def __str__(self) -> str:
        return f"""NewConfiguredBatchRequest(
    datasource_name='{self.datasource_name}',
    data_asset_name='{self.data_asset_name}',
    batch_identifiers='{self.batch_identifiers.__str__()}',
    data='{self.passthrough_parameters.__str__()}',
)
"""