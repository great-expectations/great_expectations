from great_expectations.data_context import CloudDataContext
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.data_context.data_context import DataContext


def retrieve_suites(context: AbstractDataContext) -> None:
    suites = context.list_expectation_suites()
    assert suites

    for suite in suites:
        print(suite)


if __name__ == "__main__":
    # context = CloudDataContext()
    context = DataContext()
    retrieve_suites(context)
