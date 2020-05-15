from typing import Dict, List, Union

from great_expectations.core import ExpectationSuiteValidationResult
from great_expectations.core.id_dict import BatchKwargs
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)
from great_expectations.types import DictDot


class ValidationOperatorResult(DictDot):
    def __init__(
        self,
        run_id: str,
        run_results: Dict[
            ValidationResultIdentifier,
            Dict[str, Union[ExpectationSuiteValidationResult, dict, str]],
        ],
        validation_operator_config,
        evaluation_parameters: dict = None,
        success: bool = None,
    ) -> None:
        self._run_id = run_id
        self._run_results = run_results
        self._evaluation_parameters = evaluation_parameters
        self._validation_operator_config = validation_operator_config
        self._success = success

        self._validation_results = None
        self._data_assets_validated = None
        self._data_assets_validated_by_batch_id = None
        self._validation_result_identifiers = None
        self._expectation_suite_names = None
        self._data_asset_names = None
        self._actions_results_by_validation_result_identifier = None
        self._validation_results_by_expectation_suite_name = None
        self._validation_results_by_data_asset_name = None
        self._batch_identifiers = None

    @property
    def batch_identifiers(self) -> List[str]:
        if self._batch_identifiers is None:
            self._batch_identifiers = list(
                set(
                    [
                        validation_result_identifier.batch_identifier
                        for validation_result_identifier in self.validation_result_identifiers
                    ]
                )
            )
        return self.batch_identifiers

    @property
    def validation_operator_config(self) -> dict:
        return self._validation_operator_config

    @property
    def data_asset_names(self) -> List[str]:
        if self._data_asset_names is None:
            self._data_asset_names = list(
                set(
                    [
                        data_asset["batch_kwargs"].get("data_asset_name") or "__none__"
                        for data_asset in self.data_assets_validated
                    ]
                )
            )
        return self._data_asset_names

    @property
    def actions_results_by_validation_result_identifier(self) -> dict:
        if self._actions_results_by_validation_result_identifier is None:
            self._actions_results_by_validation_result_identifier = {
                validation_result_identifier: run_result["actions_results"]
                for (
                    validation_result_identifier,
                    run_result,
                ) in self.run_results.items()
            }
        return self._actions_results_by_validation_result_identifier

    @property
    def expectation_suite_names(self) -> List[str]:
        if self._expectation_suite_names is None:
            self._expectation_suite_names = list(
                set(
                    [
                        validation_result_identifier.expectation_suite_identifier.expectation_suite_name
                        for validation_result_identifier in self._run_results.keys()
                    ]
                )
            )
        return self._expectation_suite_names

    @property
    def validation_result_identifiers(self) -> List[ValidationResultIdentifier]:
        if self._validation_result_identifiers is None:
            self._validation_result_identifiers = list(self._run_results.keys())
        return self._validation_result_identifiers

    @property
    def run_results(
        self,
    ) -> Dict[
        ValidationResultIdentifier,
        Dict[str, Union[ExpectationSuiteValidationResult, dict]],
    ]:
        return self._run_results

    @property
    def validation_results(self) -> List[ExpectationSuiteValidationResult]:
        if self._validation_results is None:
            self._validation_results = [
                run_result["validation_result"]
                for run_result in self.run_results.values()
            ]
        return self._validation_results

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def evaluation_parameters(self) -> Union[dict, None]:
        return self._evaluation_parameters

    @property
    def data_assets_validated(self) -> List[dict]:
        if self._data_assets_validated is None:
            self._data_assets_validated = list(
                self.data_assets_validated_by_batch_id.values()
            )
        return self._data_assets_validated

    @property
    def data_assets_validated_by_batch_id(self) -> dict:
        if self._data_assets_validated_by_batch_id is None:
            assets_validated_by_batch_id = {}

            for validation_result in self.validation_results:
                batch_kwargs = validation_result.meta["batch_kwargs"]
                batch_id = BatchKwargs(batch_kwargs).to_id()
                expectation_suite_name = validation_result.meta[
                    "expectation_suite_name"
                ]
                if batch_id not in assets_validated_by_batch_id:
                    assets_validated_by_batch_id[batch_id] = {
                        "batch_kwargs": batch_kwargs,
                        "validation_results": [validation_result],
                        "expectation_suite_names": [expectation_suite_name],
                    }
                else:
                    assets_validated_by_batch_id[batch_id]["validation_results"].append(
                        validation_result
                    )
                    assets_validated_by_batch_id[batch_id][
                        "expectation_suite_names"
                    ].append(expectation_suite_name)
            self._data_assets_validated_by_batch_id = assets_validated_by_batch_id
        return self._data_assets_validated_by_batch_id

    @property
    def success(self) -> bool:
        if self._success is None:
            self._success = all(
                [
                    run_result["validation_result"].success
                    for run_result in self.run_results.values()
                ]
            )
        return self._success

    @property
    def validation_results_by_expectation_suite_name(self) -> dict:
        if self._validation_results_by_expectation_suite_name is None:
            self._validation_results_by_expectation_suite_name = {
                expectation_suite_name: [
                    run_result["validation_result"]
                    for run_result in self.run_results.values()
                    if run_result["validation_result"].meta["expectation_suite_name"]
                    == expectation_suite_name
                ]
                for expectation_suite_name in self.expectation_suite_names
            }
        return self._validation_results_by_expectation_suite_name

    @property
    def validation_results_by_data_asset_name(self) -> dict:
        if self._validation_results_by_data_asset_name is None:
            validation_results_by_data_asset_name = {}
            for data_asset_name in self.data_asset_names:
                if data_asset_name == "__none__":
                    validation_results_by_data_asset_name[data_asset_name] = [
                        data_asset["validation_results"]
                        for data_asset in self.data_assets_validated
                        if data_asset["batch_kwargs"].get("data_asset_name") is None
                    ]
                else:
                    validation_results_by_data_asset_name[data_asset_name] = [
                        data_asset["validation_results"]
                        for data_asset in self.data_assets_validated
                        if data_asset["batch_kwargs"].get("data_asset_name")
                        == data_asset_name
                    ]
            self._validation_results_by_data_asset_name = (
                validation_results_by_data_asset_name
            )
        return self._validation_results_by_data_asset_name

    @property
    def statistics(self) -> dict:
        raise NotImplementedError
