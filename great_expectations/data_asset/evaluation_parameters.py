import copy
import logging
import math
import operator
import traceback

from great_expectations.exceptions import EvaluationParameterError
from pyparsing import (
    CaselessKeyword,
    Combine,
    Forward,
    Group,
    Literal,
    ParseException,
    Regex,
    Suppress,
    Word,
    alphanums,
    alphas,
    delimitedList,
)

logger = logging.getLogger(__name__)
_epsilon = 1e-12


class EvaluationParameterParser:
    """
    This Evaluation Parameter Parser uses pyparsing to provide a basic expression language capable of evaluating
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
    """

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
    }

    def __init__(self):
        self.exprStack = []
        self._parser = None

    def push_first(self, toks):
        self.exprStack.append(toks[0])

    def push_unary_minus(self, toks):
        for t in toks:
            if t == "-":
                self.exprStack.append("unary -")
            else:
                break

    def clear_stack(self):
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
            fnumber = Regex(r"[+-]?\d+(?:\.\d*)?(?:[eE][+-]?\d+)?")
            ge_urn = Combine(
                Literal("urn:great_expectations:") + Word(alphas, alphanums + "_$:?=%.")
            )
            variable = Word(alphas, alphanums + "_$")
            ident = ge_urn | variable

            plus, minus, mult, div = map(Literal, "+-*/")
            lpar, rpar = map(Suppress, "()")
            addop = plus | minus
            multop = mult | div
            expop = Literal("^")

            expr = Forward()
            expr_list = delimitedList(Group(expr))
            # add parse action that replaces the function identifier with a (name, number of args) tuple
            fn_call = (ident + lpar - Group(expr_list) + rpar).setParseAction(
                lambda t: t.insert(0, (t.pop(0), len(t[0])))
            )
            atom = (
                addop[...]
                + (
                    (fn_call | pi | e | fnumber | ident).setParseAction(self.push_first)
                    | Group(lpar + expr + rpar)
                )
            ).setParseAction(self.push_unary_minus)

            # by defining exponentiation as "atom [ ^ factor ]..." instead of "atom [ ^ atom ]...", we get right-to-left
            # exponents, instead of left-to-right that is, 2^3^2 = 2^(3^2), not (2^3)^2.
            factor = Forward()
            factor <<= atom + (expop + factor).setParseAction(self.push_first)[...]
            term = factor + (multop + factor).setParseAction(self.push_first)[...]
            expr <<= term + (addop + term).setParseAction(self.push_first)[...]
            self._parser = expr
        return self._parser

    def evaluate_stack(self, s):
        op, num_args = s.pop(), 0
        if isinstance(op, tuple):
            op, num_args = op
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
            args = reversed([self.evaluate_stack(s) for _ in range(num_args)])
            return self.fn[op](*args)
        else:
            # try to evaluate as int first, then as float if int fails
            # NOTE: JPC - 20200403 - Originally I considered returning the raw op here if parsing as float also
            # fails, but I decided against it to instead require that the *entire* expression evaluates
            # numerically UNLESS there is *exactly one* expression to substitute (see cases where len(L) == 1 in the
            # parse_evaluation_parameter method
            try:
                return int(op)
            except ValueError:
                return float(op)


def build_evaluation_parameters(
    expectation_args, evaluation_parameters=None, interactive_evaluation=True
):
    """Build a dictionary of parameters to evaluate, using the provided evaluation_parameters,
    AND mutate expectation_args by removing any parameter values passed in as temporary values during
    exploratory work.
    """
    evaluation_args = copy.deepcopy(expectation_args)

    # Iterate over arguments, and replace $PARAMETER-defined args with their
    # specified parameters.
    for key, value in evaluation_args.items():
        if isinstance(value, dict) and "$PARAMETER" in value:
            # We do not even need to search for a value if we are not going to do interactive evaluation
            if not interactive_evaluation:
                continue

            # First, check to see whether an argument was supplied at runtime
            # If it was, use that one, but remove it from the stored config
            if "$PARAMETER." + value["$PARAMETER"] in value:
                evaluation_args[key] = evaluation_args[key][
                    "$PARAMETER." + value["$PARAMETER"]
                ]
                del expectation_args[key]["$PARAMETER." + value["$PARAMETER"]]

            elif evaluation_parameters is not None:
                # parse_evaluation_parameter will raise EvaluationParameterError if we cannot find a suitable value
                evaluation_args[key] = parse_evaluation_parameter(
                    value["$PARAMETER"], evaluation_parameters
                )

            else:
                # If we haven't been able to continue already, we failed to find a parameter
                raise EvaluationParameterError(
                    "No value found for $PARAMETER " + value["$PARAMETER"]
                )

    return evaluation_args


expr = EvaluationParameterParser()


def parse_evaluation_parameter(parameter_expression, evaluation_parameters=None):
    """Use the provided evaluation_parameters dict to parse a given parameter expression.

    Args:
        parameter_expression (str): A string, potentially containing basic arithmetic operations and functions,
            and variables to be substituted
        evaluation_parameters (dict): A dictionary of name-value pairs consisting of values to substitute

    The parser will allow arithmetic operations +, -, /, *, as well as basic functions, including trunc() and round() to
    obtain integer values when needed for certain expectations (e.g. expect_column_value_length_to_be_between).

    Valid variables must begin with an alphabetic character and may contain alphanumeric characters plus '_' and '$',
    EXCEPT if they begin with the string "urn:great_expectations" in which case they may also include additional
    characters to support inclusion of GE URLs (see :ref:`evaluation_parameters` for more information).
    """
    if evaluation_parameters is None:
        evaluation_parameters = {}

    # Calling get_parser clears the stack
    parser = expr.get_parser()
    try:
        L = parser.parseString(parameter_expression, parseAll=True)
    except ParseException as err:
        L = ["Parse Failure", parameter_expression, (str(err), err.line, err.column)]

    if len(L) == 1 and L[0] not in evaluation_parameters:
        # In this special case there were no operations to find, so only one value, but we don't have something to
        # substitute for that value
        raise EvaluationParameterError("No value found for $PARAMETER " + str(L[0]))

    elif len(L) == 1:
        # In this case, we *do* have a substitution for a single type. We treat this specially because in this
        # case, we allow complex type substitutions (i.e. do not coerce to string as part of parsing)
        return evaluation_parameters[L[0]]

    elif len(L) == 0 or L[0] != "Parse Failure":
        for i, ob in enumerate(expr.exprStack):
            if isinstance(ob, str) and ob in evaluation_parameters:
                expr.exprStack[i] = str(evaluation_parameters[ob])

    else:
        err_str, err_line, err_col = L[-1]
        raise EvaluationParameterError(
            f"Parse Failure: {err_str}\nStatement: {err_line}\nColumn: {err_col}"
        )

    try:
        result = expr.evaluate_stack(expr.exprStack)
    except Exception as e:
        exception_traceback = traceback.format_exc()
        exception_message = (
            f'{type(e).__name__}: "{str(e)}".  Traceback: "{exception_traceback}".'
        )
        logger.debug(exception_message, e, exc_info=True)
        raise EvaluationParameterError(
            "Error while evaluating evaluation parameter expression: " + str(e)
        )

    return result
