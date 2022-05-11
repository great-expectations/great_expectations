
import logging
from great_expectations.exceptions import GreatExpectationsError
from great_expectations.render.types import RenderedStringTemplateContent
logger = logging.getLogger(__name__)

def render_evaluation_parameter_string(render_func):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')

    def inner_func(*args, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        rendered_string_template = render_func(*args, **kwargs)
        current_expectation_params = list()
        app_template_str = '\n - $eval_param = $eval_param_value (at time of validation).'
        configuration = kwargs.get('configuration', None)
        kwargs_dict = configuration.kwargs
        for (key, value) in kwargs_dict.items():
            if (isinstance(value, dict) and ('$PARAMETER' in value.keys())):
                current_expectation_params.append(value['$PARAMETER'])
        if (len(current_expectation_params) > 0):
            runtime_configuration = kwargs.get('runtime_configuration', None)
            if runtime_configuration:
                eval_params = runtime_configuration.get('evaluation_parameters', {})
                styling = runtime_configuration.get('styling')
                for (key, val) in eval_params.items():
                    for param in current_expectation_params:
                        if ((key == param) or (key in param)):
                            app_params = {}
                            app_params['eval_param'] = key
                            app_params['eval_param_value'] = val
                            to_append = RenderedStringTemplateContent(**{'content_block_type': 'string_template', 'string_template': {'template': app_template_str, 'params': app_params, 'styling': styling}})
                            rendered_string_template.append(to_append)
            else:
                raise GreatExpectationsError(f'''GE was not able to render the value of evaluation parameters.
                        Expectation {render_func} had evaluation parameters set, but they were not passed in.''')
        return rendered_string_template
    return inner_func

def add_values_with_json_schema_from_list_in_params(params: dict, params_with_json_schema: dict, param_key_with_list: str, list_values_type: str='string') -> dict:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    '\n    Utility function used in _atomic_prescriptive_template() to take list values from a given params dict key,\n    convert each value to a dict with JSON schema type info, then add it to params_with_json_schema (dict).\n    '
    target_list = params.get(param_key_with_list)
    if ((target_list is not None) and (len(target_list) > 0)):
        for (i, v) in enumerate(target_list):
            params_with_json_schema[f'v__{str(i)}'] = {'schema': {'type': list_values_type}, 'value': v}
    return params_with_json_schema
