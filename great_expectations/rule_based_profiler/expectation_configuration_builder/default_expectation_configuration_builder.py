from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Dict, List, Optional, Set, Union

from pyparsing import (
    Combine,
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
from pyparsing import Optional as ppOptional

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.domain import Domain  # noqa: TCH001
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.rule_based_profiler.config import (
    ParameterBuilderConfig,  # noqa: TCH001
)
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.helpers.util import (
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,  # noqa: TCH001
)

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )


text = Suppress("'") + Word(alphas, alphanums) + Suppress("'")
integer = Word(nums).setParseAction(lambda t: int(t[0]))
var = Combine(Word("$" + alphas, alphanums + "_.") + ppOptional("[" + integer + "]"))
comparison_operator = oneOf(">= <= != > < ==")
binary_operator = oneOf("& |")
operand = text | integer | var

expr = infixNotation(
    operand,
    [
        (comparison_operator, 2, opAssoc.LEFT),
        (binary_operator, 2, opAssoc.LEFT),
    ],
)


class ExpectationConfigurationConditionParserError(
    gx_exceptions.GreatExpectationsError
):
    pass


class DefaultExpectationConfigurationBuilder(ExpectationConfigurationBuilder):
    """
    Class which creates ExpectationConfiguration out of a given Expectation type and
    parameter_name-to-parameter_fully_qualified_parameter_name map (name-value pairs supplied in the kwargs dictionary).

    ExpectationConfigurations can be optionally filtered if a supplied condition is met.
    """

    exclude_field_names: ClassVar[
        Set[str]
    ] = ExpectationConfigurationBuilder.exclude_field_names | {
        "kwargs",
    }

    def __init__(  # noqa: PLR0913
        self,
        expectation_type: str,
        meta: Optional[Dict[str, Any]] = None,
        condition: Optional[str] = None,
        validation_parameter_builder_configs: Optional[
            List[ParameterBuilderConfig]
        ] = None,
        data_context: Optional[AbstractDataContext] = None,
        **kwargs,
    ) -> None:
        """
        Args:
            expectation_type: the "expectation_type" argument of "ExpectationConfiguration" object to be emitted.
            meta: the "meta" argument of "ExpectationConfiguration" object to be emitted
            condition: Boolean statement (expressed as string and following specified grammar), which controls whether
            or not underlying logic should be executed and thus resulting "ExpectationConfiguration" emitted
            validation_parameter_builder_configs: ParameterBuilder configurations, having whose outputs available (as
            fully-qualified parameter names) is pre-requisite for present ExpectationConfigurationBuilder instance
            These "ParameterBuilder" configurations help build kwargs needed for this "ExpectationConfigurationBuilder"
            data_context: AbstractDataContext associated with this ExpectationConfigurationBuilder
            kwargs: additional arguments
        """

        super().__init__(
            expectation_type=expectation_type,
            validation_parameter_builder_configs=validation_parameter_builder_configs,
            data_context=data_context,
            **kwargs,
        )

        if meta is None:
            meta = {}

        self._meta = meta

        if not isinstance(meta, dict):
            raise gx_exceptions.ProfilerExecutionError(
                message=f"""Argument "{meta}" in "{self.__class__.__name__}" must be of type "dictionary" \
(value of type "{str(type(meta))}" was encountered).
"""
            )

        if condition and (not isinstance(condition, str)):
            raise gx_exceptions.ProfilerExecutionError(
                message=f"""Argument "{condition}" in "{self.__class__.__name__}" must be of type "string" \
(value of type "{str(type(condition))}" was encountered).
"""
            )

        self._condition = condition

        self._validation_parameter_builder_configs = (
            validation_parameter_builder_configs
        )

        self._kwargs = kwargs

    @property
    def expectation_type(self) -> str:
        return self._expectation_type

    @property
    def condition(self) -> Optional[str]:
        return self._condition

    @property
    def validation_parameter_builder_configs(
        self,
    ) -> Optional[List[ParameterBuilderConfig]]:
        return self._validation_parameter_builder_configs

    @property
    def kwargs(self) -> dict:
        return self._kwargs

    @property
    def meta(self) -> dict:
        return self._meta

    def _parse_condition(self) -> ParseResults:
        """
        Using the grammer defined by expr, provides the condition to the parser.

        Applicability: To be used as part of configuration (e.g., YAML-based files or text strings).
        Extendability: Readily extensible to include "slice" and other standard accessors (as long as no dynamic elements).
        """

        try:
            return expr.parseString(self._condition)
        except ParseException:
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
        """Recursively substitute all parameters and variables in term list

        Given a list of terms created by parsing a provided condition, recursively substitute all parameters and
        variables in the term list, regardless of depth of groupings.

        Example:
            condition: "($variables.max_user_id>0 & $variables.answer==42) | $parameter.my_min_user_id.value[0]<0" will
            return the following term list from self._parse_condition:
                parsed_condition = [[
                                      [
                                        ['$variables.max_user_id', '>', 0],
                                        '&',
                                        ['$variables.answer', '==', 42]
                                      ],
                                      '|',
                                      ['$parameter.my_min_user_id.value[0]', '<', 0]
                                   ]]

            This method will then take that term list and recursively search for parameters and variables that need to
            be substituted and return this ParseResults object:
                return [[
                          [
                             [999999999999, '>', 0],
                             '&',
                             [42, '==', 42]
                          ],
                          '|',
                          [397433, '<', 0]
                       ]]

        Args:
            term_list (Union[str, ParseResults): the ParseResults object returned from self._parse_condition
            domain (Domain): The domain of the ExpectationConfiguration
            variables (Optional[ParameterContainer]): The variables set for this ExpectationConfiguration
            parameters (Optional[Dict[str, ParameterContainer]]): The parameters set for this ExpectationConfiguration

        Returns:
            ParseResults: a ParseResults object identical to the one returned by self._parse_condition except with
                          substituted parameters and variables.
        """
        idx: int
        token: Union[str, ParseResults]
        for idx, token in enumerate(term_list):
            if isinstance(token, str) and token.startswith("$"):
                term_list[idx]: Dict[
                    str, Any
                ] = get_parameter_value_and_validate_return_type(
                    domain=domain,
                    parameter_reference=token,
                    expected_return_type=None,
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

    def _build_binary_list(
        self,
        substituted_term_list: Union[str, ParseResults],
    ) -> ParseResults:
        """Recursively build binary list from substituted terms

        Given a list of substituted terms created by parsing a provided condition and substituting parameters and
        variables, recursively build binary condition ParseResults object, regardless of depth of groupings.

        Example:
            substituted_term_list = [[
                                        [
                                            [999999999999, '>', 0],
                                            '&',
                                            [42, '==', 42]
                                        ],
                                        '|',
                                        [397433, '<', 0]
                                     ]]

            This method will then take that term list and recursively evaluate the terms between the top-level binary
            conditions and return this ParseResults object:
                return [True, '&' True]

        Args:
            substituted_term_list (Union[str, ParseResults]): the ParseResults object returned from
                                                              self._substitute_parameters_and_variables

        Returns:
            ParseResults: a ParseResults object with all terms evaluated except for binary operations.

        """
        idx: int
        token: Union[str, list]
        for idx, token in enumerate(substituted_term_list):
            if (not any(isinstance(t, ParseResults) for t in token)) and len(token) > 1:
                substituted_term_list[idx] = eval("".join([str(t) for t in token]))
            elif isinstance(token, ParseResults):
                self._build_binary_list(substituted_term_list=token)

        return substituted_term_list

    def _build_boolean_result(
        self,
        binary_list: Union[ParseResults, List[Union[bool, str]]],
    ) -> bool:
        """Recursively build boolean result from binary list

        Given a list of binary terms created by parsing a provided condition, substituting parameters and
        variables, and building binary condition ParseResults object, recursively evaluate remaining conditions and
        return boolean result of condition.

        Example:
            binary_list = [True, '&' True]

            This method will then take that term list and recursively evaluate the remaining and return a boolean result
            for the provided condition:
                return True

        Args:
            binary_list (List[Union[bool, str]]): the ParseResults object returned from
                                                   self._substitute_parameters_and_variables

        Returns:
            bool: a boolean representing the evaluation of the entire provided condition.

        """
        idx: int
        token: Union[str, list]
        for idx, token in enumerate(binary_list):
            if (
                (not isinstance(token, bool))
                and (not any(isinstance(t, ParseResults) for t in token))
                and (len(token) > 1)
            ):
                binary_list[idx] = eval("".join([str(t) for t in token]))
                return self._build_boolean_result(binary_list=binary_list)
            elif isinstance(token, ParseResults):
                return self._build_boolean_result(binary_list=token)

        return eval("".join([str(t) for t in binary_list]))

    def _evaluate_condition(
        self,
        parsed_condition: ParseResults,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
    ) -> bool:
        """Evaluates the parsed condition to True/False and returns the boolean result"""
        substituted_term_list: ParseResults = self._substitute_parameters_and_variables(
            term_list=parsed_condition,
            domain=domain,
            variables=variables,
            parameters=parameters,
        )

        binary_list: ParseResults = self._build_binary_list(
            substituted_term_list=substituted_term_list
        )
        boolean_result: bool = self._build_boolean_result(binary_list=binary_list)

        return boolean_result

    def _build_expectation_configuration(
        self,
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[str, ParameterContainer]] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> Optional[ExpectationConfiguration]:
        """Returns either and ExpectationConfiguration object or None depending on evaluation of condition"""
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
            for parameter_name, fully_qualified_parameter_name in self.kwargs.items()
        }
        meta: Dict[str, Any] = get_parameter_value_and_validate_return_type(
            domain=domain,
            parameter_reference=self.meta,
            expected_return_type=dict,
            variables=variables,
            parameters=parameters,
        )

        if self._condition:
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
                )
            else:
                return None
        else:
            return ExpectationConfiguration(
                expectation_type=self._expectation_type,
                kwargs=expectation_kwargs,
                meta=meta,
            )
