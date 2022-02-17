from great_expectations.rule_based_profiler.parameter_builder import (
    MetricMultiBatchParameterBuilder,
)


class ValueSetMultiBatchParameterBuilder(MetricMultiBatchParameterBuilder):
    """Build a set of unique values if they qualify as categorical.

    Compute existing values across a batch or a set of batches and determine
    whether they qualify as categorical based on cardinality setting.

    Attributes:
        cardinality_category: A CardinalityCategory to set the number of unique
            values allowed to be considered categorical.
    """

    # TODO AJB 20220216: add implementation
    raise NotImplementedError
