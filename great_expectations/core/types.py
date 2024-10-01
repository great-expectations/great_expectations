from datetime import date, datetime
from typing import Union

from great_expectations.core.suite_parameters import SuiteParameterDict

Comparable = Union[float, SuiteParameterDict, date, datetime]
