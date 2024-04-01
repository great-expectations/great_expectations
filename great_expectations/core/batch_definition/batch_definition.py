from typing import Union

from great_expectations.core.batch_definition import Year
from great_expectations.core.batch_definition.date import Date
from great_expectations.core.batch_definition.whole_asset import WholeAsset

BatchDefinition = Union[WholeAsset, Date, Year]
