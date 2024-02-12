from typing import Union

from great_expectations.datasource.fluent.spark_generic_partitioners import (
    PartitionerColumnValue as SparkPartitionerColumnValue,
)
from great_expectations.datasource.fluent.spark_generic_partitioners import (
    PartitionerDatetimePart as SparkPartitionerDatetimePart,
)
from great_expectations.datasource.fluent.spark_generic_partitioners import (
    PartitionerDividedInteger as SparkPartitionerDividedInteger,
)
from great_expectations.datasource.fluent.spark_generic_partitioners import (
    PartitionerModInteger as SparkPartitionerModInteger,
)
from great_expectations.datasource.fluent.spark_generic_partitioners import (
    PartitionerMultiColumnValue as SparkPartitionerMultiColumnValue,
)
from great_expectations.datasource.fluent.spark_generic_partitioners import (
    PartitionerYear as SparkPartitionerYear,
)
from great_expectations.datasource.fluent.spark_generic_partitioners import (
    PartitionerYearAndMonth as SparkPartitionerYearAndMonth,
)
from great_expectations.datasource.fluent.spark_generic_partitioners import (
    PartitionerYearAndMonthAndDay as SparkPartitionerYearAndMonthAndDay,
)
from great_expectations.datasource.fluent.sql_datasource import (
    PartitionerColumnValue as SqlPartitionerColumnValue,
)
from great_expectations.datasource.fluent.sql_datasource import (
    PartitionerDatetimePart as SqlPartitionerDatetimePart,
)
from great_expectations.datasource.fluent.sql_datasource import (
    PartitionerDividedInteger as SqlPartitionerDividedInteger,
)
from great_expectations.datasource.fluent.sql_datasource import (
    PartitionerModInteger as SqlPartitionerModInteger,
)
from great_expectations.datasource.fluent.sql_datasource import (
    PartitionerMultiColumnValue as SqlPartitionerMultiColumnValue,
)
from great_expectations.datasource.fluent.sql_datasource import (
    PartitionerYear as SqlPartitionerYear,
)
from great_expectations.datasource.fluent.sql_datasource import (
    PartitionerYearAndMonth as SqlPartitionerYearAndMonth,
)
from great_expectations.datasource.fluent.sql_datasource import (
    PartitionerYearAndMonthAndDay as SqlPartitionerYearAndMonthAndDay,
)

Partitioner = Union[
    SqlPartitionerYear,
    SqlPartitionerYearAndMonth,
    SqlPartitionerYearAndMonthAndDay,
    SqlPartitionerDatetimePart,
    SqlPartitionerDividedInteger,
    SqlPartitionerModInteger,
    SqlPartitionerColumnValue,
    SqlPartitionerMultiColumnValue,
    SparkPartitionerYear,
    SparkPartitionerYearAndMonth,
    SparkPartitionerYearAndMonthAndDay,
    SparkPartitionerDatetimePart,
    SparkPartitionerDividedInteger,
    SparkPartitionerModInteger,
    SparkPartitionerColumnValue,
    SparkPartitionerMultiColumnValue,
]
