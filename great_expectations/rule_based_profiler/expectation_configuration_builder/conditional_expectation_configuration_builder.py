from typing import Any, Dict, List, Optional, Set

from pyparsing import Combine
from pyparsing import Optional as ppOptional
from pyparsing import (
    ParseException,
    ParseResults,
    Suppress,
    Word,
    alphanums,
    alphas,
    nums,
    oneOf,
)

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer
from great_expectations.rule_based_profiler.util import (
    get_parameter_value,
    get_parameter_value_and_validate_return_type,
)

text = Suppress("'") + Word(alphas, alphanums) + Suppress("'")
integer = Word(nums).setParseAction(lambda t: int(t[0]))
var = Combine(Word("$" + alphas, alphanums + "_.") + ppOptional("[" + integer + "]"))
comparison_operator = oneOf(">= <= != > < ==")
bitwise_operator = oneOf("~ & |")

condition_parser = (
    var
    + comparison_operator
    + integer
    + ppOptional(bitwise_operator + var + comparison_operator + integer)
)

term_parser = integer + comparison_operator + integer


class ExpectationConfigurationConditionParserError(
    ge_exceptions.GreatExpectationsError
):
    pass


class ConditionalExpectationConfigurationBuilder(ExpectationConfigurationBuilder):
    """
    Class which creates ExpectationConfiguration out of a given Expectation type and
    parameter_name-to-parameter_fully_qualified_parameter_name map (name-value pairs supplied in the kwargs dictionary)
    if, and only if, the supplied condition is met.
    """

    include_field_names: Set[str] = {
        "expectation_type",
    }

    def __init__(
        self,
        expectation_type: str,
        condition: str,
        meta: Optional[Dict[str, Any]] = None,
        success_on_last_run: Optional[bool] = None,
        **kwargs,
    ):
        super().__init__(expectation_type=expectation_type)

        self._expectation_kwargs = kwargs

        if meta is None:
            meta = {}
        if not isinstance(meta, dict):
            raise ge_exceptions.ProfilerExecutionError(
                message=f"""Argument "{meta}" in "{self.__class__.__name__}" must be of type "dictionary" \
(value of type "{str(type(meta))}" was encountered).
"""
            )

        if not isinstance(condition, str):
            raise ge_exceptions.ProfilerExecutionError(
                message=f"""Argument "{condition}" in "{self.__class__.__name__}" must be of type "string" \
(value of type "{str(type(condition))}" was encountered).
"""
            )

        self._condition = condition
        self._meta = meta
        self._success_on_last_run = success_on_last_run

    def _parse_condition(self) -> ParseResults:
        """
        Using the grammer defined by "condition", provides the parsing of collection (list, dictionary) access syntax:
        List: variable[index: int]
        Dictionary: variable[key: str]
        Nested List/Dictionary: variable[index_0: int][key_0: str][index_1: int][key_1: str][key_2: str][index_2: int]...

        Applicability: To be used as part of configuration (e.g., YAML-based files or text strings).
        Extendability: Readily extensible to include "slice" and other standard accessors (as long as no dynamic elements).
        """

        try:
            return condition_parser.parseString(self._condition)
        except ParseException:
            raise ExpectationConfigurationConditionParserError(
                f'Unable to parse Expectation Configuration Condition: "{self._condition}".'
            )

    def _evaluate_condition(
        self,
        parsed_condition: ParseResults,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> bool:
        substituted_parameters_condition: ParseResults = parsed_condition.copy()
        token: str
        boolean_condition: List[bool, str] = []
        term_count: int = 1
        for i, token in enumerate(parsed_condition):
            if isinstance(token, str) and token.startswith("$"):
                substituted_parameters_condition[i]: Dict[
                    str, Any
                ] = get_parameter_value(
                    domain=domain,
                    parameter_reference=token,
                    variables=variables,
                    parameters=parameters,
                )

            bitwise_operator_offset: int = i + term_count
            if (i > 0) and (bitwise_operator_offset % 4 == 3):
                term: List[str, int, float] = substituted_parameters_condition[
                    bitwise_operator_offset - 3 : bitwise_operator_offset
                ]
                term_str: str = "".join([str(t) for t in term])
                term_boolean = eval(term_str)
                boolean_condition.append(term_boolean)
                try:
                    # check to see if there is a bitwise operator after the boolean expression
                    term_bitwise: bitwise_operator = substituted_parameters_condition[
                        bitwise_operator_offset
                    ]
                    boolean_condition.append(term_bitwise)
                    term_count += 1
                except:
                    pass

        return eval("".join([str(t) for t in boolean_condition]))

    def _build_expectation_configuration(
        self,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> ExpectationConfiguration:
        parameter_name: str
        fully_qualified_parameter_name: str
        expectation_kwargs: Dict[str, Any] = {
            parameter_name: get_parameter_value_and_validate_return_type(
                domain=domain,
                parameter_reference=fully_qualified_parameter_name,
                expected_return_type=None,
                variables=variables,
                parameters=parameters,
            )
            for parameter_name, fully_qualified_parameter_name in self._expectation_kwargs.items()
        }
        meta: Dict[str, Any] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self._meta,
            expected_return_type=dict,
            variables=variables,
            parameters=parameters,
        )

        parsed_condition: ParseResults = self._parse_condition()
        condition: bool = self._evaluate_condition(
            parsed_condition=parsed_condition,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

        if condition:
            return ExpectationConfiguration(
                expectation_type=self._expectation_type,
                kwargs=expectation_kwargs,
                meta=meta,
                success_on_last_run=self._success_on_last_run,
            )
        else:
            return None

    @property
    def condition(self) -> str:
        return self._condition
