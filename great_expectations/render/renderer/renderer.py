from functools import wraps
from typing import Any

from great_expectations.core._docs_decorators import public_api
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)


def renderer(renderer_type, **kwargs):
    def wrapper(renderer_fn):
        @wraps(renderer_fn)
        def inner_func(*args, **kwargs):
            return renderer_fn(*args, **kwargs)

        inner_func._renderer_type = renderer_type
        inner_func._renderer_definition_kwargs = kwargs
        return inner_func

    return wrapper


@public_api
class Renderer:
    """A convenience class to provide an explicit mechanism to instantiate any Renderer."""

    def __init__(self) -> None:
        # This is purely a convenience to provide an explicit mechanism to instantiate any Renderer, even ones that
        # used to be composed exclusively of classmethods
        pass

    @classmethod
    def _get_expectation_type(cls, ge_object):
        if isinstance(ge_object, ExpectationConfiguration):
            return ge_object.expectation_type

        elif isinstance(ge_object, ExpectationValidationResult):
            # This is a validation
            return ge_object.expectation_config.expectation_type

    # TODO: When we implement a ValidationResultSuite class, this method will move there.
    @classmethod
    def _find_evr_by_type(cls, evrs, type_):
        for evr in evrs:
            if evr.expectation_config.expectation_type == type_:
                return evr

    # TODO: When we implement a ValidationResultSuite class, this method will move there.
    @classmethod
    def _find_all_evrs_by_type(cls, evrs, type_, column_=None):
        ret = []
        for evr in evrs:
            if evr.expectation_config.expectation_type == type_ and (
                not column_ or column_ == evr.expectation_config.kwargs.get("column")
            ):
                ret.append(evr)

        return ret

    # TODO: When we implement a ValidationResultSuite class, this method will move there.
    @classmethod
    def _get_column_list_from_evrs(cls, evrs):
        """
        Get list of column names.

        If expect_table_columns_to_match_ordered_list EVR is present, use it as the list, including the order.

        Otherwise, get the list of all columns mentioned in the expectations and order it alphabetically.

        :param evrs:
        :return: list of columns with best effort sorting
        """
        evrs_ = evrs if isinstance(evrs, list) else evrs.results

        expect_table_columns_to_match_ordered_list_evr = cls._find_evr_by_type(
            evrs_, "expect_table_columns_to_match_ordered_list"
        )
        # Group EVRs by column
        sorted_columns = sorted(
            list(
                {
                    evr.expectation_config.kwargs["column"]
                    for evr in evrs_
                    if "column" in evr.expectation_config.kwargs
                }
            )
        )

        if expect_table_columns_to_match_ordered_list_evr:
            ordered_columns = expect_table_columns_to_match_ordered_list_evr.result[
                "observed_value"
            ]
        else:
            ordered_columns = []

        # only return ordered columns from expect_table_columns_to_match_ordered_list evr if they match set of column
        # names from entire evr
        if set(sorted_columns) == set(ordered_columns):
            return ordered_columns
        else:
            return sorted_columns

    # TODO: When we implement a ValidationResultSuite class, this method will move there.
    @classmethod
    def _group_evrs_by_column(cls, validation_results):
        columns = {}
        for evr in validation_results.results:
            if "column" in evr.expectation_config.kwargs:
                column = evr.expectation_config.kwargs["column"]
            else:
                column = "Table-level Expectations"

            if column not in columns:
                columns[column] = []
            columns[column].append(evr)

        return columns

    def render(self, **kwargs: dict) -> Any:
        """
        Render interface method.
        """
        raise NotImplementedError
