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
from great_expectations.rule_based_profiler.config import ParameterBuilderConfig
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    ExpectationConfigurationBuilder,
)
from great_expectations.rule_based_profiler.helpers.util import (
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.types import Domain, ParameterContainer

text = (Suppress("'") + Word(alphas, alphanums)) + Suppress("'")
integer = Word(nums).setParseAction(lambda t: int(t[0]))
var = Combine(
    Word(("$" + alphas), (alphanums + "_.")) + ppOptional(("[" + integer) + "]")
)
comparison_operator = oneOf(">= <= != > < ==")
binary_operator = oneOf("~ & |")
operand = (text | integer) | var
expr = infixNotation(
    operand,
    [(comparison_operator, 2, opAssoc.LEFT), (binary_operator, 2, opAssoc.LEFT)],
)


class ExpectationConfigurationConditionParserError(
    ge_exceptions.GreatExpectationsError
):
    pass


class DefaultExpectationConfigurationBuilder(ExpectationConfigurationBuilder):
    "\n    Class which creates ExpectationConfiguration out of a given Expectation type and\n    parameter_name-to-parameter_fully_qualified_parameter_name map (name-value pairs supplied in the kwargs dictionary).\n\n    ExpectationConfigurations can be optionally filtered if a supplied condition is met.\n"
    exclude_field_names: Set[
        str
    ] = ExpectationConfigurationBuilder.exclude_field_names | {"kwargs"}

    def __init__(
        self,
        expectation_type: str,
        meta: Optional[Dict[(str, Any)]] = None,
        condition: Optional[str] = None,
        validation_parameter_builder_configs: Optional[
            List[ParameterBuilderConfig]
        ] = None,
        data_context: Optional["BaseDataContext"] = None,
        **kwargs,
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        Args:\n            expectation_type: the "expectation_type" argument of "ExpectationConfiguration" object to be emitted.\n            meta: the "meta" argument of "ExpectationConfiguration" object to be emitted\n            condition: Boolean statement (expressed as string and following specified grammar), which controls whether\n            or not underlying logic should be executed and thus resulting "ExpectationConfiguration" emitted\n            validation_parameter_builder_configs: ParameterBuilder configurations, having whose outputs available (as\n            fully-qualified parameter names) is pre-requisite for present ExpectationConfigurationBuilder instance\n            These "ParameterBuilder" configurations help build kwargs needed for this "ExpectationConfigurationBuilder"\n            data_context: BaseDataContext associated with this ExpectationConfigurationBuilder\n            kwargs: additional arguments\n        '
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
            raise ge_exceptions.ProfilerExecutionError(
                message=f"""Argument "{meta}" in "{self.__class__.__name__}" must be of type "dictionary" (value of type "{str(type(meta))}" was encountered).
"""
            )
        if condition and (not isinstance(condition, str)):
            raise ge_exceptions.ProfilerExecutionError(
                message=f"""Argument "{condition}" in "{self.__class__.__name__}" must be of type "string" (value of type "{str(type(condition))}" was encountered).
"""
            )
        self._condition = condition
        self._validation_parameter_builder_configs = (
            validation_parameter_builder_configs
        )
        self._kwargs = kwargs

    @property
    def expectation_type(self) -> str:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._expectation_type

    @property
    def condition(self) -> Optional[str]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._condition

    @property
    def validation_parameter_builder_configs(
        self,
    ) -> Optional[List[ParameterBuilderConfig]]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._validation_parameter_builder_configs

    @property
    def kwargs(self) -> dict:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._kwargs

    @property
    def meta(self) -> dict:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._meta

    def _parse_condition(self) -> ParseResults:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        Using the grammer defined by expr, provides the condition to the parser.\n\n        Applicability: To be used as part of configuration (e.g., YAML-based files or text strings).\n        Extendability: Readily extensible to include "slice" and other standard accessors (as long as no dynamic elements).\n        '
        try:
            return expr.parseString(self._condition)
        except ParseException:
            raise ExpectationConfigurationConditionParserError(
                f'Unable to parse Expectation Configuration Condition: "{self._condition}".'
            )

    def _substitute_parameters_and_variables(
        self,
        term_list: Union[(str, ParseResults)],
        domain: Domain,
        variables: Optional[ParameterContainer] = None,
        parameters: Optional[Dict[(str, ParameterContainer)]] = None,
    ) -> ParseResults:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Recursively substitute all parameters and variables in term list\n\n        Given a list of terms created by parsing a provided condition, recursively substitute all parameters and\n        variables in the term list, regardless of depth of groupings.\n\n        Example:\n            condition: \"($variables.max_user_id>0 & $variables.answer==42) | $parameter.my_min_user_id.value[0]<0\" will\n            return the following term list from self._parse_condition:\n                parsed_condition = [[\n                                      [\n                                        ['$variables.max_user_id', '>', 0],\n                                        '&',\n                                        ['$variables.answer', '==', 42]\n                                      ],\n                                      '|',\n                                      ['$parameter.my_min_user_id.value[0]', '<', 0]\n                                   ]]\n\n            This method will then take that term list and recursively search for parameters and variables that need to\n            be substituted and return this ParseResults object:\n                return [[\n                          [\n                             [999999999999, '>', 0],\n                             '&',\n                             [42, '==', 42]\n                          ],\n                          '|',\n                          [397433, '<', 0]\n                       ]]\n\n        Args:\n            term_list (Union[str, ParseResults): the ParseResults object returned from self._parse_condition\n            domain (Domain): The domain of the ExpectationConfiguration\n            variables (Optional[ParameterContainer]): The variables set for this ExpectationConfiguration\n            parameters (Optional[Dict[str, ParameterContainer]]): The parameters set for this ExpectationConfiguration\n\n        Returns:\n            ParseResults: a ParseResults object identical to the one returned by self._parse_condition except with\n                          substituted parameters and variables.\n        "
        idx: int
        token: Union[(str, ParseResults)]
        for (idx, token) in enumerate(term_list):
            if isinstance(token, str) and token.startswith("$"):
                term_list[idx]: Dict[
                    (str, Any)
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
        self, substituted_term_list: Union[(str, ParseResults)]
    ) -> ParseResults:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Recursively build binary list from substituted terms\n\n        Given a list of substituted terms created by parsing a provided condition and substituting parameters and\n        variables, recursively build binary condition ParseResults object, regardless of depth of groupings.\n\n        Example:\n            substituted_term_list = [[\n                                        [\n                                            [999999999999, '>', 0],\n                                            '&',\n                                            [42, '==', 42]\n                                        ],\n                                        '|',\n                                        [397433, '<', 0]\n                                     ]]\n\n            This method will then take that term list and recursively evaluate the terms between the top-level binary\n            conditions and return this ParseResults object:\n                return [True, '&' True]\n\n        Args:\n            substituted_term_list (Union[str, ParseResults]): the ParseResults object returned from\n                                                              self._substitute_parameters_and_variables\n\n        Returns:\n            ParseResults: a ParseResults object with all terms evaluated except for binary operations.\n\n        "
        idx: int
        token: Union[(str, list)]
        for (idx, token) in enumerate(substituted_term_list):
            if (not any([isinstance(t, ParseResults) for t in token])) and (
                len(token) > 1
            ):
                substituted_term_list[idx] = eval("".join([str(t) for t in token]))
            elif isinstance(token, ParseResults):
                self._build_binary_list(substituted_term_list=token)
        return substituted_term_list

    def _build_boolean_result(
        self, binary_list: Union[(ParseResults, List[Union[(bool, str)]])]
    ) -> bool:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Recursively build boolean result from binary list\n\n        Given a list of binary terms created by parsing a provided condition, substituting parameters and\n        variables, and building binary condition ParseResults object, recursively evaluate remaining conditions and\n        return boolean result of condition.\n\n        Example:\n            binary_list = [True, '&' True]\n\n            This method will then take that term list and recursively evaluate the remaining and return a boolean result\n            for the provided condition:\n                return True\n\n        Args:\n            binary_list (List[Union[bool, str]]): the ParseResults object returned from\n                                                   self._substitute_parameters_and_variables\n\n        Returns:\n            bool: a boolean representing the evaluation of the entire provided condition.\n\n        "
        idx: int
        token: Union[(str, list)]
        for (idx, token) in enumerate(binary_list):
            if (
                (not isinstance(token, bool))
                and (not any([isinstance(t, ParseResults) for t in token]))
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
        parameters: Optional[Dict[(str, ParameterContainer)]] = None,
    ) -> bool:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Evaluates the parsed condition to True/False and returns the boolean result"
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
        parameters: Optional[Dict[(str, ParameterContainer)]] = None,
    ) -> Optional[ExpectationConfiguration]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Returns either and ExpectationConfiguration object or None depending on evaluation of condition"
        parameter_name: str
        fully_qualified_parameter_name: str
        expectation_kwargs: Dict[(str, Any)] = {
            parameter_name: get_parameter_value_and_validate_return_type(
                domain=domain,
                parameter_reference=fully_qualified_parameter_name,
                expected_return_type=None,
                variables=variables,
                parameters=parameters,
            )
            for (parameter_name, fully_qualified_parameter_name) in self.kwargs.items()
        }
        meta: Dict[(str, Any)] = get_parameter_value_and_validate_return_type(
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
