from __future__ import annotations

import copy
import datetime
import logging
import math
import operator
import traceback
from collections import namedtuple
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Union

import dateutil
from pyparsing import (
    CaselessKeyword,
    Forward,
    Group,
    Literal,
    ParseException,
    ParseResults,
    Regex,
    Suppress,
    Word,
    alphanums,
    alphas,
    delimitedList,
    dictOf,
)

from great_expectations.exceptions import SuiteParameterError
from great_expectations.util import convert_to_json_serializable  # noqa: TID251

if TYPE_CHECKING:
    from typing_extensions import TypeAlias, TypeGuard

    from great_expectations.data_context import AbstractDataContext

logger = logging.getLogger(__name__)
_epsilon = 1e-12

# NOTE: Temporary alias - to be converted to a rich type
SuiteParameterDict: TypeAlias = dict


def is_suite_parameter(value: Any) -> TypeGuard[SuiteParameterDict]:
    """Typeguard to check if a value is an suite parameter."""
    return isinstance(value, dict) and "$PARAMETER" in value


def get_suite_parameter_key(suite_parameter: SuiteParameterDict) -> str:
    """Get the key of a suite parameter.

    e.g. if the suite parameter is {"$PARAMETER": "foo"}, this function will return "foo".
    When evaluating the runtime configuration of an expectation, we will look for
    a runtime value for "foo".

    Args:
        suite_parameter: The suite parameter to get the key of

    Returns:
        The key of the suite parameter
    """
    return suite_parameter["$PARAMETER"]


class SuiteParameterParser:
    """
    This Suite Parameter Parser uses pyparsing to provide a basic expression language capable of evaluating
    parameters using values available only at run time.

    expop   :: '^'
    multop  :: '*' | '/'
    addop   :: '+' | '-'
    integer :: ['+' | '-'] '0'..'9'+
    atom    :: PI | E | real | fn '(' expr ')' | '(' expr ')'
    factor  :: atom [ expop factor ]*
    term    :: factor [ multop factor ]*
    expr    :: term [ addop term ]*

    The parser is modified from: https://github.com/pyparsing/pyparsing/blob/master/examples/fourFn.py
    """  # noqa: E501

    # map operator symbols to corresponding arithmetic operations
    opn = {
        "+": operator.add,
        "-": operator.sub,
        "*": operator.mul,
        "/": operator.truediv,
        "^": operator.pow,
    }

    fn = {
        "sin": math.sin,
        "cos": math.cos,
        "tan": math.tan,
        "exp": math.exp,
        "abs": abs,
        "trunc": lambda a: int(a),
        "round": round,
        "sgn": lambda a: -1 if a < -_epsilon else 1 if a > _epsilon else 0,
        "now": datetime.datetime.now,
        "datetime": datetime.datetime,
        "timedelta": datetime.timedelta,
    }

    def __init__(self) -> None:
        self.exprStack: list = []
        self._parser = None

    def push_first(self, toks) -> None:
        self.exprStack.append(toks[0])

    def push_unary_minus(self, toks) -> None:
        for t in toks:
            if t == "-":
                self.exprStack.append("unary -")
            else:
                break

    def clear_stack(self) -> None:
        del self.exprStack[:]

    def get_parser(self):
        self.clear_stack()
        if not self._parser:
            # use CaselessKeyword for e and pi, to avoid accidentally matching
            # functions that start with 'e' or 'pi' (such as 'exp'); Keyword
            # and CaselessKeyword only match whole words
            e = CaselessKeyword("E")
            pi = CaselessKeyword("PI")
            # fnumber = Combine(Word("+-"+nums, nums) +
            #                    Optional("." + Optional(Word(nums))) +
            #                    Optional(e + Word("+-"+nums, nums)))
            # or use provided pyparsing_common.number, but convert back to str:
            # fnumber = ppc.number().addParseAction(lambda t: str(t[0]))
            fnumber = Regex(r"[+-]?(?:\d+|\.\d+)(?:\.\d+)?(?:[eE][+-]?\d+)?")
            variable = Word(alphas, f"{alphanums}_$")

            plus, minus, mult, div = map(Literal, "+-*/")
            lpar, rpar = map(Suppress, "()")
            addop = plus | minus
            multop = mult | div
            expop = Literal("^")

            expr = Forward()
            expr_list = delimitedList(Group(expr))

            # We will allow functions either to accept *only* keyword
            # expressions or *only* non-keyword expressions
            # define function keyword arguments
            key = Word(f"{alphas}_") + Suppress("=")
            # value = (fnumber | Word(alphanums))
            value = expr
            keyval = dictOf(key.setParseAction(self.push_first), value)
            kwarglist = delimitedList(keyval)

            # add parse action that replaces the function identifier with a (name, number of args, has_fn_kwargs) tuple  # noqa: E501
            # 20211009 - JPC - Note that it's important that we consider kwarglist
            # first as part of disabling backtracking for the function's arguments
            fn_call = (variable + lpar + rpar).setParseAction(
                lambda t: t.insert(0, (t.pop(0), 0, False))
            ) | (
                (variable + lpar - Group(expr_list) + rpar).setParseAction(
                    lambda t: t.insert(0, (t.pop(0), len(t[0]), False))
                )
                ^ (variable + lpar - Group(kwarglist) + rpar).setParseAction(
                    lambda t: t.insert(0, (t.pop(0), len(t[0]), True))
                )
            )
            atom = (
                addop[...]
                + (
                    (fn_call | pi | e | fnumber | variable).setParseAction(self.push_first)
                    | Group(lpar + expr + rpar)
                )
            ).setParseAction(self.push_unary_minus)

            # by defining exponentiation as "atom [ ^ factor ]..." instead of "atom [ ^ atom ]...", we get right-to-left  # noqa: E501
            # exponents, instead of left-to-right that is, 2^3^2 = 2^(3^2), not (2^3)^2.
            factor = Forward()
            factor <<= atom + (expop + factor).setParseAction(self.push_first)[...]
            term = factor + (multop + factor).setParseAction(self.push_first)[...]
            expr <<= term + (addop + term).setParseAction(self.push_first)[...]
            self._parser = expr
        return self._parser

    def evaluate_stack(self, s):  # noqa: C901, PLR0911, PLR0912
        op, num_args, has_fn_kwargs = s.pop(), 0, False
        if isinstance(op, tuple):
            op, num_args, has_fn_kwargs = op
        if op == "unary -":
            return -self.evaluate_stack(s)
        if op in "+-*/^":
            # note: operands are pushed onto the stack in reverse order
            op2 = self.evaluate_stack(s)
            op1 = self.evaluate_stack(s)
            return self.opn[op](op1, op2)
        elif op == "PI":
            return math.pi  # 3.1415926535
        elif op == "E":
            return math.e  # 2.718281828
        elif op in self.fn:
            # note: args are pushed onto the stack in reverse order
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
            # Require that the *entire* expression evaluates to number or datetime UNLESS there is *exactly one*  # noqa: E501
            # expression to substitute (see cases where len(parse_results) == 1 in the parse_suite_parameter  # noqa: E501
            # method).
            evaluated: Union[int, float, datetime.datetime]
            try:
                evaluated = int(op)
                logger.info("Suite parameter operand successfully parsed as integer.")
            except ValueError:
                logger.info("Parsing suite parameter operand as integer failed.")
                try:
                    evaluated = float(op)
                    logger.info("Suite parameter operand successfully parsed as float.")
                except ValueError:
                    logger.info("Parsing suite parameter operand as float failed.")
                    try:
                        evaluated = dateutil.parser.parse(op)
                        logger.info("Suite parameter operand successfully parsed as datetime.")
                    except ValueError as e:
                        logger.info("Parsing suite parameter operand as datetime failed.")
                        raise e  # noqa: TRY201
            return evaluated


def build_suite_parameters(
    expectation_args: dict,
    suite_parameters: Optional[dict] = None,
    interactive_evaluation: bool = True,
    data_context=None,
) -> Tuple[dict, dict]:
    """Build a dictionary of parameters to evaluate, using the provided suite_parameters,
    AND mutate expectation_args by removing any parameter values passed in as temporary values during
    exploratory work.
    """  # noqa: E501
    suite_args = copy.deepcopy(expectation_args)
    substituted_parameters = {}

    # Iterate over arguments, and replace $PARAMETER-defined args with their
    # specified parameters.
    for key, value in suite_args.items():
        if isinstance(value, dict) and "$PARAMETER" in value:
            # We do not even need to search for a value if we are not going to do interactive evaluation  # noqa: E501
            if not interactive_evaluation:
                continue

            # First, check to see whether an argument was supplied at runtime
            # If it was, use that one, but remove it from the stored config
            param_key = f"$PARAMETER.{value['$PARAMETER']}"
            if param_key in value:
                suite_args[key] = suite_args[key][param_key]
                del expectation_args[key][param_key]

            # If not, try to parse the suite parameter and substitute, which will raise
            # an exception if we do not have a value
            else:
                raw_value = value["$PARAMETER"]
                parameter_value = parse_suite_parameter(
                    raw_value,
                    suite_parameters=suite_parameters,
                    data_context=data_context,
                )
                suite_args[key] = parameter_value
                # Once we've substituted, we also track that we did so
                substituted_parameters[key] = parameter_value

    return suite_args, substituted_parameters


EXPR = SuiteParameterParser()


def parse_suite_parameter(  # noqa: C901
    parameter_expression: str,
    suite_parameters: Optional[Dict[str, Any]] = None,
    data_context: Optional[AbstractDataContext] = None,
) -> Any:
    """Use the provided suite_parameters dict to parse a given parameter expression.

    Args:
        parameter_expression (str): A string, potentially containing basic arithmetic operations and functions,
            and variables to be substituted
        suite_parameters (dict): A dictionary of name-value pairs consisting of values to substitute
        data_context (DataContext): A data context to use to obtain metrics, if necessary

    The parser will allow arithmetic operations +, -, /, *, as well as basic functions, including trunc() and round() to
    obtain integer values when needed for certain expectations (e.g. expect_column_value_length_to_be_between).

    Valid variables must begin with an alphabetic character and may contain alphanumeric characters plus '_' and '$'.
    """  # noqa: E501
    if suite_parameters is None:
        suite_parameters = {}

    parse_results: Union[ParseResults, list] = _get_parse_results(parameter_expression)

    if _is_single_function_no_args(parse_results):
        # Necessary to catch `now()` (which only needs to be evaluated with `expr.exprStack`)
        # NOTE: 20211122 - Chetan - Any future built-ins that are zero arity functions will match this behavior  # noqa: E501
        pass

    elif len(parse_results) == 1 and parse_results[0] not in suite_parameters:
        # In this special case there were no operations to find, so only one value, but we don't have something to  # noqa: E501
        # substitute for that value
        raise SuiteParameterError(  # noqa: TRY003
            f"No value found for $PARAMETER {parse_results[0]!s}"
        )

    elif len(parse_results) == 1:
        # In this case, we *do* have a substitution for a single type. We treat this specially because in this  # noqa: E501
        # case, we allow complex type substitutions (i.e. do not coerce to string as part of parsing)  # noqa: E501
        # NOTE: 20201023 - JPC - to support MetricDefinition as an suite parameter type, we need to handle that  # noqa: E501
        # case here; is the suite parameter provided here in fact a metric definition?
        return suite_parameters[parse_results[0]]

    elif len(parse_results) == 0 or parse_results[0] != "Parse Failure":
        # we have a stack to evaluate and there was no parse failure.
        for i, ob in enumerate(EXPR.exprStack):
            if isinstance(ob, str) and ob in suite_parameters:
                EXPR.exprStack[i] = str(suite_parameters[ob])

    else:
        err_str, err_line, err_col = parse_results[-1]
        raise SuiteParameterError(  # noqa: TRY003
            f"Parse Failure: {err_str}\nStatement: {err_line}\nColumn: {err_col}"
        )

    try:
        result = EXPR.evaluate_stack(EXPR.exprStack)
        result = convert_to_json_serializable(result)
    except Exception as e:
        exception_traceback = traceback.format_exc()
        exception_message = f'{type(e).__name__}: "{e!s}".  Traceback: "{exception_traceback}".'
        logger.debug(exception_message, e, exc_info=True)
        raise SuiteParameterError(  # noqa: TRY003
            f"Error while evaluating suite parameter expression: {e!s}"
        ) from e

    return result


def _get_parse_results(
    parameter_expression: str,
) -> Union[ParseResults, list]:
    # Calling get_parser clears the stack
    parser = EXPR.get_parser()
    try:
        parse_results = parser.parseString(parameter_expression, parseAll=True)
    except ParseException as err:
        parse_results = [
            "Parse Failure",
            parameter_expression,
            (str(err), err.line, err.column),
        ]
    return parse_results


def _is_single_function_no_args(parse_results: Union[ParseResults, list]) -> bool:
    # Represents a valid parser result of a single function that has no arguments
    return (
        len(parse_results) == 1
        and isinstance(parse_results[0], tuple)
        and parse_results[0][2] is False
    )


def _deduplicate_suite_parameter_dependencies(dependencies: dict) -> dict:  # noqa: C901 - too complex
    deduplicated: dict = {}
    for suite_name, required_metrics in dependencies.items():
        deduplicated[suite_name] = []
        metrics = set()
        metric_kwargs: dict = {}
        for metric in required_metrics:
            if isinstance(metric, str):
                metrics.add(metric)
            elif isinstance(metric, dict):
                # There is a single metric_kwargs_id object in this construction
                for kwargs_id, metric_list in metric["metric_kwargs_id"].items():
                    if kwargs_id not in metric_kwargs:
                        metric_kwargs[kwargs_id] = set()
                    for metric_name in metric_list:
                        metric_kwargs[kwargs_id].add(metric_name)
        deduplicated[suite_name] = list(metrics)
        if len(metric_kwargs) > 0:
            deduplicated[suite_name] = deduplicated[suite_name] + [
                {
                    "metric_kwargs_id": {
                        metric_kwargs: list(metrics_set)
                        for (metric_kwargs, metrics_set) in metric_kwargs.items()
                    }
                }
            ]

    return deduplicated


SuiteParameterIdentifier = namedtuple(  # noqa: PYI024 # this class is not used
    "SuiteParameterIdentifier",
    ["expectation_suite_name", "metric_name", "metric_kwargs_id"],
)
