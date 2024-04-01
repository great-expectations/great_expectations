from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.partitioners import PartitionerYearAndMonthAndDay


class Date(BatchDefinition):
    column: str

    def __init__(self, column, **kwargs):
        super().__init__(
            **kwargs,
            column=column,
            partitioner=PartitionerYearAndMonthAndDay(column_name=column),
        )
