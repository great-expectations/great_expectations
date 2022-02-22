from typing import Any, Dict, List, Optional, Set, Union

from pyparsing import Combine
from pyparsing import Optional as ppOptional
from pyparsing import (
    ParseException,
    ParseResults,
    Suppress,
    Word,
    alphanums,
    alphas,
    infixNotation,
    nums,
    oneOf,
    opAssoc,
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
operand = text | integer | var

expr = infixNotation(
    operand,
    [
        (comparison_operator, 2, opAssoc.LEFT),
        (bitwise_operator, 2, opAssoc.LEFT),
    ],
)


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
            return expr.parseString(self._condition)
        except ParseException as e:
            print(str(e))
            raise ExpectationConfigurationConditionParserError(
                f'Unable to parse Expectation Configuration Condition: "{self._condition}".'
            )

    def _substitute_parameters_and_variables(
        self,
        term_list: Union[str, ParseResults],
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> ParseResults:
        """Recursively substitute all parameters and variables"""
        token: Union[str, ParseResults]
        for i, token in enumerate(term_list):
            if isinstance(token, str) and token.startswith("$"):
                term_list[i]: Dict[str, Any] = get_parameter_value(
                    domain=domain,
                    parameter_reference=token,
                    variables=variables,
                    parameters=parameters,
                )
            elif isinstance(token, ParseResults):
                self._substitute_parameters_and_variables(
                    term_list=token,
                    domain=domain,
                    variables=variables,
                    parameters=parameters,
                )
        return term_list

    def _build_bitwise_list(
        self,
        substituted_terms: Union[str, list],
    ) -> ParseResults:
        """Recursively build bitwise list from substituted terms"""
        token: Union[str, list]
        for i, token in enumerate(substituted_terms):
            if (not any([isinstance(t, ParseResults) for t in token])) and len(
                token
            ) > 1:
                substituted_terms[i] = eval("".join([str(t) for t in token]))
            elif isinstance(token, ParseResults):
                self._build_bitwise_list(substituted_terms=token)

        return substituted_terms

    def _build_boolean_result(
        self,
        bitwise_list: List[Union[bool, str]],
    ) -> bool:
        """Recursively build boolean result from bitwise list"""
        token: Union[str, list]
        for i, token in enumerate(bitwise_list):
            if (
                (not isinstance(token, bool))
                and (not any([isinstance(t, ParseResults) for t in token]))
                and (len(token) > 1)
            ):
                bitwise_list[i] = eval("".join([str(t) for t in token]))
                return self._build_boolean_result(bitwise_list=bitwise_list)
            elif isinstance(token, ParseResults):
                return self._build_boolean_result(bitwise_list=token)

        return eval("".join([str(t) for t in bitwise_list]))

    def _evaluate_condition(
        self,
        parsed_condition: ParseResults,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> bool:
        """Evaluates the parsed condition to True/False and returns the boolean result"""
        substituted_terms: ParseResults = self._substitute_parameters_and_variables(
            term_list=parsed_condition,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

        bitwise_list: ParseResults = self._build_bitwise_list(
            substituted_terms=substituted_terms
        )
        boolean_result: bool = self._build_boolean_result(bitwise_list=bitwise_list)

        return boolean_result

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
