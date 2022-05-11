
from typing import Dict, Optional
import numpy as np
import pandas as pd
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import ColumnMapExpectation, InvalidExpectationConfigurationError
from great_expectations.expectations.util import add_values_with_json_schema_from_list_in_params, render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import num_to_str, parse_row_condition_string_pandas_engine, substitute_none_for_missing

class ExpectColumnValuesToNotBeInSet(ColumnMapExpectation):
    'Expect column entries to not be in the set.\n\n    For example:\n    ::\n\n        # my_df.my_col = [1,2,2,3,3,3]\n        >>> my_df.expect_column_values_to_not_be_in_set(\n            "my_col",\n            [1,2]\n        )\n        {\n          "success": false\n          "result": {\n            "unexpected_count": 3\n            "unexpected_percent": 50.0,\n            "unexpected_percent_nonmissing": 50.0,\n            "partial_unexpected_list": [\n              1, 2, 2\n            ],\n          },\n        }\n\n    expect_column_values_to_not_be_in_set is a     :func:`column_map_expectation <great_expectations.execution_engine.execution_engine.MetaExecutionEngine\n    .column_map_expectation>`.\n\n    Args:\n        column (str):             The column name.\n        value_set (set-like):             A set of objects used for comparison.\n\n    Keyword Args:\n        mostly (None or a float between 0 and 1):             Return `"success": True` if at least mostly fraction of values match the expectation.             For more detail, see :ref:`mostly`.\n\n    Other Parameters:\n        result_format (str or None):             Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n            For more detail, see :ref:`result_format <result_format>`.\n        include_config (boolean):             If True, then include the expectation config as part of the result object.             For more detail, see :ref:`include_config`.\n        catch_exceptions (boolean or None):             If True, then catch exceptions and include them as part of the result object.             For more detail, see :ref:`catch_exceptions`.\n        meta (dict or None):             A JSON-serializable dictionary (nesting allowed) that will be included in the output without             modification. For more detail, see :ref:`meta`.\n\n    Returns:\n        An ExpectationSuiteValidationResult\n\n        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n    See Also:\n        :func:`expect_column_values_to_be_in_set         <great_expectations.execution_engine.execution_engine.ExecutionEngine.expect_column_values_to_be_in_set>`\n\n    '
    library_metadata = {'maturity': 'production', 'tags': ['core expectation', 'column map expectation'], 'contributors': ['@great_expectations'], 'requirements': [], 'has_full_test_suite': True, 'manually_reviewed_code': True}
    map_metric = 'column_values.not_in_set'
    success_keys = ('value_set', 'mostly', 'parse_strings_as_datetimes')
    default_kwarg_values = {'row_condition': None, 'condition_parser': None, 'mostly': 1, 'parse_strings_as_datetimes': False, 'result_format': 'BASIC', 'include_config': True, 'catch_exceptions': False}
    args_keys = ('column', 'value_set')

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().validate_configuration(configuration)
        if (configuration is None):
            configuration = self.configuration
        try:
            assert ('value_set' in configuration.kwargs), 'value_set is required'
            assert isinstance(configuration.kwargs['value_set'], (list, set, dict)), 'value_set must be a list or a set'
            if isinstance(configuration.kwargs['value_set'], dict):
                assert ('$PARAMETER' in configuration.kwargs['value_set']), 'Evaluation Parameter dict for value_set kwarg must have "$PARAMETER" key.'
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    @classmethod
    def _atomic_prescriptive_template(cls, configuration=None, result=None, language=None, runtime_configuration=None, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        runtime_configuration = (runtime_configuration or {})
        include_column_name = runtime_configuration.get('include_column_name', True)
        include_column_name = (include_column_name if (include_column_name is not None) else True)
        styling = runtime_configuration.get('styling')
        params = substitute_none_for_missing(configuration.kwargs, ['column', 'value_set', 'mostly', 'parse_strings_as_datetimes', 'row_condition', 'condition_parser'])
        params_with_json_schema = {'column': {'schema': {'type': 'string'}, 'value': params.get('column')}, 'value_set': {'schema': {'type': 'array'}, 'value': params.get('value_set')}, 'mostly': {'schema': {'type': 'number'}, 'value': params.get('mostly')}, 'mostly_pct': {'schema': {'type': 'string'}, 'value': params.get('mostly_pct')}, 'parse_strings_as_datetimes': {'schema': {'type': 'boolean'}, 'value': params.get('parse_strings_as_datetimes')}, 'row_condition': {'schema': {'type': 'string'}, 'value': params.get('row_condition')}, 'condition_parser': {'schema': {'type': 'string'}, 'value': params.get('condition_parser')}}
        if ((params['value_set'] is None) or (len(params['value_set']) == 0)):
            values_string = '[ ]'
        else:
            for (i, v) in enumerate(params['value_set']):
                params[f'v__{str(i)}'] = v
            values_string = ' '.join([f'$v__{str(i)}' for (i, v) in enumerate(params['value_set'])])
        template_str = f'values must not belong to this set: {values_string}'
        if ((params['mostly'] is not None) and (params['mostly'] < 1.0)):
            params_with_json_schema['mostly_pct']['value'] = num_to_str((params['mostly'] * 100), precision=15, no_scientific=True)
            template_str += ', at least $mostly_pct % of the time.'
        else:
            template_str += '.'
        if params.get('parse_strings_as_datetimes'):
            template_str += ' Values should be parsed as datetimes.'
        if include_column_name:
            template_str = f'$column {template_str}'
        if (params['row_condition'] is not None):
            (conditional_template_str, conditional_params) = parse_row_condition_string_pandas_engine(params['row_condition'], with_schema=True)
            template_str = f'{conditional_template_str}, then {template_str}'
            params_with_json_schema.update(conditional_params)
        params_with_json_schema = add_values_with_json_schema_from_list_in_params(params=params, params_with_json_schema=params_with_json_schema, param_key_with_list='value_set')
        return (template_str, params_with_json_schema, styling)

    @classmethod
    @renderer(renderer_type='renderer.prescriptive')
    @render_evaluation_parameter_string
    def _prescriptive_renderer(cls, configuration=None, result=None, language=None, runtime_configuration=None, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        runtime_configuration = (runtime_configuration or {})
        include_column_name = runtime_configuration.get('include_column_name', True)
        include_column_name = (include_column_name if (include_column_name is not None) else True)
        styling = runtime_configuration.get('styling')
        params = substitute_none_for_missing(configuration.kwargs, ['column', 'value_set', 'mostly', 'parse_strings_as_datetimes', 'row_condition', 'condition_parser'])
        if ((params['value_set'] is None) or (len(params['value_set']) == 0)):
            values_string = '[ ]'
        else:
            for (i, v) in enumerate(params['value_set']):
                params[f'v__{str(i)}'] = v
            values_string = ' '.join([f'$v__{str(i)}' for (i, v) in enumerate(params['value_set'])])
        template_str = f'values must not belong to this set: {values_string}'
        if ((params['mostly'] is not None) and (params['mostly'] < 1.0)):
            params['mostly_pct'] = num_to_str((params['mostly'] * 100), precision=15, no_scientific=True)
            template_str += ', at least $mostly_pct % of the time.'
        else:
            template_str += '.'
        if params.get('parse_strings_as_datetimes'):
            template_str += ' Values should be parsed as datetimes.'
        if include_column_name:
            template_str = f'$column {template_str}'
        if (params['row_condition'] is not None):
            (conditional_template_str, conditional_params) = parse_row_condition_string_pandas_engine(params['row_condition'])
            template_str = f'{conditional_template_str}, then {template_str}'
            params.update(conditional_params)
        return [RenderedStringTemplateContent(**{'content_block_type': 'string_template', 'string_template': {'template': template_str, 'params': params, 'styling': styling}})]

    def _pandas_column_values_not_in_set(self, series: pd.Series, metrics: Dict, metric_domain_kwargs: Dict, metric_value_kwargs: Dict, runtime_configuration: dict=None, filter_column_isnull: bool=True):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        value_set = metric_value_kwargs['value_set']
        if (value_set is None):
            return np.ones(len(series), dtype=np.bool_)
        if pd.api.types.is_datetime64_any_dtype(series):
            parsed_value_set = PandasExecutionEngine.parse_value_set(value_set=value_set)
        else:
            parsed_value_set = value_set
        return pd.DataFrame({'column_values.not_in_set': (~ series.isin(parsed_value_set))})
