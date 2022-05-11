
import enum
from dataclasses import dataclass
from pyparsing import CaselessLiteral, Combine, Literal, ParseException, Regex, Suppress, Word, alphanums, alphas
import great_expectations.exceptions as ge_exceptions
try:
    import pyspark.sql.functions as F
except ImportError:
    F = None
try:
    import sqlalchemy as sa
except ImportError:
    sa = None

def _set_notnull(s, l, t) -> None:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    t['notnull'] = True
column_name = Combine(((Suppress(Literal('col("')) + Word(alphas, f'{alphanums}_.').setResultsName('column')) + Suppress(Literal('")'))))
gt = Literal('>')
lt = Literal('<')
ge = Literal('>=')
le = Literal('<=')
eq = Literal('==')
ops = ((((gt ^ lt) ^ ge) ^ le) ^ eq).setResultsName('op')
fnumber = Regex('[+-]?\\d+(?:\\.\\d*)?(?:[eE][+-]?\\d+)?').setResultsName('fnumber')
condition_value = (((Suppress('"') + Word(f'{alphanums}.').setResultsName('condition_value')) + Suppress('"')) ^ ((Suppress("'") + Word(f'{alphanums}.').setResultsName('condition_value')) + Suppress("'")))
not_null = CaselessLiteral('.notnull()').setResultsName('notnull')
condition = ((column_name + not_null).setParseAction(_set_notnull) ^ ((column_name + ops) + (fnumber ^ condition_value)))

class ConditionParserError(ge_exceptions.GreatExpectationsError):
    pass

class RowConditionParserType(enum.Enum):
    'Type of condition or parser to be used to interpret a RowCondition\n\n    Note that many of these are forward looking and are not yet implemented.\n    In the future `GE` can replace the `great_expectations__experimental__`\n    name for the condition_parser and this enum can be used internally\n    instead of strings for the condition_parser user input.\n    '
    GE = 'ge'
    SPARK = 'spark'
    SPARK_SQL = 'spark_sql'
    PANDAS = 'pandas'
    PYTHON = 'python'
    SQL = 'sql'

@dataclass
class RowCondition():
    'Condition that can be used to filter rows in a data set.\n\n    Attributes:\n        condition: String of the condition\n        condition_type: Format of the condition e.g. for parsing\n    '
    condition: str
    condition_type: RowConditionParserType

def _parse_great_expectations_condition(row_condition: str):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    try:
        return condition.parseString(row_condition)
    except ParseException:
        raise ConditionParserError(f'unable to parse condition: {row_condition}')

def parse_condition_to_spark(row_condition: str) -> 'pyspark.sql.Column':
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    parsed = _parse_great_expectations_condition(row_condition)
    column = parsed['column']
    if ('condition_value' in parsed):
        if (parsed['op'] == '=='):
            return (F.col(column) == parsed['condition_value'])
        else:
            raise ConditionParserError(f"Invalid operator: {parsed['op']} for string literal spark condition.")
    elif ('fnumber' in parsed):
        try:
            num = int(parsed['fnumber'])
        except ValueError:
            num = float(parsed['fnumber'])
        op = parsed['op']
        if (op == '>'):
            return (F.col(column) > num)
        elif (op == '<'):
            return (F.col(column) < num)
        elif (op == '>='):
            return (F.col(column) >= num)
        elif (op == '<='):
            return (F.col(column) <= num)
        elif (op == '=='):
            return (F.col(column) == num)
    elif (('notnull' in parsed) and (parsed['notnull'] is True)):
        return F.col(column).isNotNull()
    else:
        raise ConditionParserError(f'unrecognized column condition: {row_condition}')

def parse_condition_to_sqlalchemy(row_condition: str) -> 'sqlalchemy.sql.expression.ColumnElement':
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    parsed = _parse_great_expectations_condition(row_condition)
    column = parsed['column']
    if ('condition_value' in parsed):
        if (parsed['op'] == '=='):
            return (sa.column(column) == parsed['condition_value'])
        else:
            raise ConditionParserError(f"Invalid operator: {parsed['op']} for string literal spark condition.")
    elif ('fnumber' in parsed):
        try:
            num = int(parsed['fnumber'])
        except ValueError:
            num = float(parsed['fnumber'])
        op = parsed['op']
        if (op == '>'):
            return (sa.column(column) > num)
        elif (op == '<'):
            return (sa.column(column) < num)
        elif (op == '>='):
            return (sa.column(column) >= num)
        elif (op == '<='):
            return (sa.column(column) <= num)
        elif (op == '=='):
            return (sa.column(column) == num)
    elif (('notnull' in parsed) and (parsed['notnull'] is True)):
        return sa.not_(sa.column(column).is_(None))
    else:
        raise ConditionParserError(f'unrecognized column condition: {row_condition}')
