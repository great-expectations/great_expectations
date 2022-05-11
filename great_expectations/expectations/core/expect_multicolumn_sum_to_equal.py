
from typing import Optional
from great_expectations.core import ExpectationConfiguration
from great_expectations.expectations.expectation import MulticolumnMapExpectation
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer

class ExpectMulticolumnSumToEqual(MulticolumnMapExpectation):
    '\n    Expects that the sum of row values is the same for each row, summing only values in columns specified in\n    column_list, and equal to the specific value, sum_total.\n\n    Args:\n        column_list (tuple or list): Set of columns to be checked\n        sum_total (int):             expected sum of columns\n\n    Keyword Args:\n        ignore_row_if (str): "all_values_are_missing", "any_value_is_missing", "never"\n\n    Other Parameters:\n        result_format (str or None):             Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n        include_config (boolean):             If True, then include the expectation config as part of the result object.         catch_exceptions (boolean or None):             If True, then catch exceptions and include them as part of the result object.         meta (dict or None):             A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification.\n\n    Returns:\n        An ExpectationSuiteValidationResult\n    '
    library_metadata = {'maturity': 'production', 'tags': ['core expectation', 'column aggregate expectation'], 'contributors': ['@great_expectations'], 'requirements': [], 'has_full_test_suite': True, 'manually_reviewed_code': True}
    map_metric = 'multicolumn_sum.equal'
    success_keys = ('sum_total',)
    default_kwarg_values = {'row_condition': None, 'condition_parser': None, 'ignore_row_if': 'all_values_are_missing', 'result_format': 'BASIC', 'include_config': True, 'catch_exceptions': False}
    args_keys = ('column_list', 'sum_total')

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that\n        necessary configuration arguments have been provided for the validation of the expectation.\n\n        Args:\n            configuration (OPTIONAL[ExpectationConfiguration]):                 An optional Expectation Configuration entry that will be used to configure the expectation\n        Returns:\n            None. Raises InvalidExpectationConfigurationError if the config is not validated successfully\n        '
        super().validate_configuration(configuration)
        self.validate_metric_value_between_configuration(configuration=configuration)

    @classmethod
    @renderer(renderer_type='renderer.prescriptive')
    @render_evaluation_parameter_string
    def _prescriptive_renderer(cls, configuration=None, result=None, language=None, runtime_configuration=None, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        pass

    @classmethod
    @renderer(renderer_type='renderer.diagnostic.observed_value')
    def _diagnostic_observed_value_renderer(cls, configuration=None, result=None, language=None, runtime_configuration=None, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        pass
