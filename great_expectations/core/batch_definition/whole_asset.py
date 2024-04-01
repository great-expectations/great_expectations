from great_expectations.compatibility import pydantic

from great_expectations.core.batch_definition import BatchDefinition


class WholeAsset(BatchDefinition):
    partitioner: None = pydantic.PrivateAttr()
