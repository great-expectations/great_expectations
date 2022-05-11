
import copy
import datetime
import logging
import math
import operator
import traceback
from collections import namedtuple
from typing import Any, Dict, Optional, Tuple
from pyparsing import CaselessKeyword, Combine, Forward, Group, Literal, ParseException, Regex, Suppress, Word, alphanums, alphas, delimitedList, dictOf
from great_expectations.core.urn import ge_urn
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.exceptions import EvaluationParameterError
logger = logging.getLogger(__name__)
_epsilon = 1e-12

class EvaluationParameterParser():
    "\n    This Evaluation Parameter Parser uses pyparsing to provide a basic expression language capable of evaluating\n    parameters using values available only at run time.\n\n    expop   :: '^'\n    multop  :: '*' | '/'\n    addop   :: '+' | '-'\n    integer :: ['+' | '-'] '0'..'9'+\n    atom    :: PI | E | real | fn '(' expr ')' | '(' expr ')'\n    factor  :: atom [ expop factor ]*\n    term    :: factor [ multop factor ]*\n    expr    :: term [ addop term ]*\n\n    The parser is modified from: https://github.com/pyparsing/pyparsing/blob/master/examples/fourFn.py\n    "
    opn = {'+': operator.add, '-': operator.sub, '*': operator.mul, '/': operator.truediv, '^': operator.pow}
    fn = {'sin': math.sin, 'cos': math.cos, 'tan': math.tan, 'exp': math.exp, 'abs': abs, 'trunc': (lambda a: int(a)), 'round': round, 'sgn': (lambda a: ((- 1) if (a < (- _epsilon)) else (1 if (a > _epsilon) else 0))), 'now': datetime.datetime.now, 'datetime': datetime.datetime, 'timedelta': datetime.timedelta}

    def __init__(self) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.exprStack = []
        self._parser = None

    def push_first(self, toks) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.exprStack.append(toks[0])

    def push_unary_minus(self, toks) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        for t in toks:
            if (t == '-'):
                self.exprStack.append('unary -')
            else:
                break

    def clear_stack(self) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        del self.exprStack[:]

    def get_parser(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.clear_stack()
        if (not self._parser):
            e = CaselessKeyword('E')
            pi = CaselessKeyword('PI')
            fnumber = Regex('[+-]?(?:\\d+|\\.\\d+)(?:\\.\\d+)?(?:[eE][+-]?\\d+)?')
            ge_urn = Combine((Literal('urn:great_expectations:') + Word(alphas, f'{alphanums}_$:?=%.&')))
            variable = Word(alphas, f'{alphanums}_$')
            ident = (ge_urn | variable)
            (plus, minus, mult, div) = map(Literal, '+-*/')
            (lpar, rpar) = map(Suppress, '()')
            addop = (plus | minus)
            multop = (mult | div)
            expop = Literal('^')
            expr = Forward()
            expr_list = delimitedList(Group(expr))
            key = (Word(f'{alphas}_') + Suppress('='))
            value = expr
            keyval = dictOf(key.setParseAction(self.push_first), value)
            kwarglist = delimitedList(keyval)
            fn_call = (((ident + lpar) + rpar).setParseAction((lambda t: t.insert(0, (t.pop(0), 0, False)))) | ((((ident + lpar) - Group(expr_list)) + rpar).setParseAction((lambda t: t.insert(0, (t.pop(0), len(t[0]), False)))) ^ (((ident + lpar) - Group(kwarglist)) + rpar).setParseAction((lambda t: t.insert(0, (t.pop(0), len(t[0]), True))))))
            atom = (addop[...] + (((((fn_call | pi) | e) | fnumber) | ident).setParseAction(self.push_first) | Group(((lpar + expr) + rpar)))).setParseAction(self.push_unary_minus)
            factor = Forward()
            factor <<= (atom + (expop + factor).setParseAction(self.push_first)[...])
            term = (factor + (multop + factor).setParseAction(self.push_first)[...])
            expr <<= (term + (addop + term).setParseAction(self.push_first)[...])
            self._parser = expr
        return self._parser

    def evaluate_stack(self, s):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        (op, num_args, has_fn_kwargs) = (s.pop(), 0, False)
        if isinstance(op, tuple):
            (op, num_args, has_fn_kwargs) = op
        if (op == 'unary -'):
            return (- self.evaluate_stack(s))
        if (op in '+-*/^'):
            op2 = self.evaluate_stack(s)
            op1 = self.evaluate_stack(s)
            return self.opn[op](op1, op2)
        elif (op == 'PI'):
            return math.pi
        elif (op == 'E'):
            return math.e
        elif (op in self.fn):
            if has_fn_kwargs:
                kwargs = dict()
                for _ in range(num_args):
                    v = self.evaluate_stack(s)
                    k = s.pop()
                    kwargs.update({k: v})
                return self.fn[op](**kwargs)
            else:
                args = reversed([self.evaluate_stack(s) for _ in range(num_args)])
                return self.fn[op](*args)
        else:
            try:
                return int(op)
            except ValueError:
                return float(op)

def build_evaluation_parameters(expectation_args: dict, evaluation_parameters: Optional[dict]=None, interactive_evaluation: bool=True, data_context=None) -> Tuple[(dict, dict)]:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Build a dictionary of parameters to evaluate, using the provided evaluation_parameters,\n    AND mutate expectation_args by removing any parameter values passed in as temporary values during\n    exploratory work.\n    '
    evaluation_args = copy.deepcopy(expectation_args)
    substituted_parameters = {}
    for (key, value) in evaluation_args.items():
        if (isinstance(value, dict) and ('$PARAMETER' in value)):
            if (not interactive_evaluation):
                continue
            param_key = f"$PARAMETER.{value['$PARAMETER']}"
            if (param_key in value):
                evaluation_args[key] = evaluation_args[key][param_key]
                del expectation_args[key][param_key]
            else:
                raw_value = value['$PARAMETER']
                parameter_value = parse_evaluation_parameter(raw_value, evaluation_parameters=evaluation_parameters, data_context=data_context)
                evaluation_args[key] = parameter_value
                substituted_parameters[key] = parameter_value
    return (evaluation_args, substituted_parameters)
expr = EvaluationParameterParser()

def find_evaluation_parameter_dependencies(parameter_expression):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Parse a parameter expression to identify dependencies including GE URNs.\n\n    Args:\n        parameter_expression: the parameter to parse\n\n    Returns:\n        a dictionary including:\n          - "urns": set of strings that are valid GE URN objects\n          - "other": set of non-GE URN strings that are required to evaluate the parameter expression\n\n    '
    expr = EvaluationParameterParser()
    dependencies = {'urns': set(), 'other': set()}
    parser = expr.get_parser()
    try:
        _ = parser.parseString(parameter_expression, parseAll=True)
    except ParseException as err:
        raise EvaluationParameterError(f'Unable to parse evaluation parameter: {str(err)} at line {err.line}, column {err.column}')
    except AttributeError as err:
        raise EvaluationParameterError(f'Unable to parse evaluation parameter: {str(err)}')
    for word in expr.exprStack:
        if isinstance(word, (int, float)):
            continue
        if (not isinstance(word, str)):
            continue
        if ((word in expr.opn) or (word in expr.fn) or (word == 'unary -')):
            continue
        try:
            _ = float(word)
            continue
        except ValueError:
            pass
        try:
            _ = ge_urn.parseString(word)
            dependencies['urns'].add(word)
            continue
        except ParseException:
            pass
        dependencies['other'].add(word)
    return dependencies

def parse_evaluation_parameter(parameter_expression: str, evaluation_parameters: Optional[Dict[(str, Any)]]=None, data_context: Optional[Any]=None) -> Any:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Use the provided evaluation_parameters dict to parse a given parameter expression.\n\n    Args:\n        parameter_expression (str): A string, potentially containing basic arithmetic operations and functions,\n            and variables to be substituted\n        evaluation_parameters (dict): A dictionary of name-value pairs consisting of values to substitute\n        data_context (DataContext): A data context to use to obtain metrics, if necessary\n\n    The parser will allow arithmetic operations +, -, /, *, as well as basic functions, including trunc() and round() to\n    obtain integer values when needed for certain expectations (e.g. expect_column_value_length_to_be_between).\n\n    Valid variables must begin with an alphabetic character and may contain alphanumeric characters plus \'_\' and \'$\',\n    EXCEPT if they begin with the string "urn:great_expectations" in which case they may also include additional\n    characters to support inclusion of GE URLs (see :ref:`evaluation_parameters` for more information).\n    '
    if (evaluation_parameters is None):
        evaluation_parameters = {}
    parser = expr.get_parser()
    try:
        L = parser.parseString(parameter_expression, parseAll=True)
    except ParseException as err:
        L = ['Parse Failure', parameter_expression, (str(err), err.line, err.column)]
    if ((len(L) == 1) and isinstance(L[0], tuple) and (L[0][2] is False)):
        pass
    elif ((len(L) == 1) and (L[0] not in evaluation_parameters)):
        try:
            res = ge_urn.parseString(L[0])
            if (res['urn_type'] == 'stores'):
                store = data_context.stores.get(res['store_name'])
                return store.get_query_result(res['metric_name'], res.get('metric_kwargs', {}))
            else:
                logger.error("Unrecognized urn_type in ge_urn: must be 'stores' to use a metric store.")
                raise EvaluationParameterError(f'No value found for $PARAMETER {str(L[0])}')
        except ParseException as e:
            logger.debug(f'Parse exception while parsing evaluation parameter: {str(e)}')
            raise EvaluationParameterError(f'No value found for $PARAMETER {str(L[0])}')
        except AttributeError:
            logger.warning('Unable to get store for store-type valuation parameter.')
            raise EvaluationParameterError(f'No value found for $PARAMETER {str(L[0])}')
    elif (len(L) == 1):
        return evaluation_parameters[L[0]]
    elif ((len(L) == 0) or (L[0] != 'Parse Failure')):
        for (i, ob) in enumerate(expr.exprStack):
            if (isinstance(ob, str) and (ob in evaluation_parameters)):
                expr.exprStack[i] = str(evaluation_parameters[ob])
            elif (isinstance(ob, str) and (ob not in evaluation_parameters)):
                try:
                    res = ge_urn.parseString(ob)
                    if (res['urn_type'] == 'stores'):
                        store = data_context.stores.get(res['store_name'])
                        expr.exprStack[i] = str(store.get_query_result(res['metric_name'], res.get('metric_kwargs', {})))
                    else:
                        pass
                except ParseException:
                    pass
                except AttributeError:
                    pass
    else:
        (err_str, err_line, err_col) = L[(- 1)]
        raise EvaluationParameterError(f'''Parse Failure: {err_str}
Statement: {err_line}
Column: {err_col}''')
    try:
        result = expr.evaluate_stack(expr.exprStack)
        result = convert_to_json_serializable(result)
    except Exception as e:
        exception_traceback = traceback.format_exc()
        exception_message = f'{type(e).__name__}: "{str(e)}".  Traceback: "{exception_traceback}".'
        logger.debug(exception_message, e, exc_info=True)
        raise EvaluationParameterError(f'Error while evaluating evaluation parameter expression: {str(e)}')
    return result

def _deduplicate_evaluation_parameter_dependencies(dependencies: dict) -> dict:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    deduplicated = {}
    for (suite_name, required_metrics) in dependencies.items():
        deduplicated[suite_name] = []
        metrics = set()
        metric_kwargs = {}
        for metric in required_metrics:
            if isinstance(metric, str):
                metrics.add(metric)
            elif isinstance(metric, dict):
                for (kwargs_id, metric_list) in metric['metric_kwargs_id'].items():
                    if (kwargs_id not in metric_kwargs):
                        metric_kwargs[kwargs_id] = set()
                    for metric_name in metric_list:
                        metric_kwargs[kwargs_id].add(metric_name)
        deduplicated[suite_name] = list(metrics)
        if (len(metric_kwargs) > 0):
            deduplicated[suite_name] = (deduplicated[suite_name] + [{'metric_kwargs_id': {metric_kwargs: list(metrics_set) for (metric_kwargs, metrics_set) in metric_kwargs.items()}}])
    return deduplicated
EvaluationParameterIdentifier = namedtuple('EvaluationParameterIdentifier', ['expectation_suite_name', 'metric_name', 'metric_kwargs_id'])
