from typing import Dict, List, Union

from great_expectations.core import ExpectationSuiteValidationResult
from great_expectations.core.id_dict import BatchKwargs
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)


class ValidationOperatorResult(object):
    def __init__(
        self,
        run_id: str,
        run_results: Dict[
            ValidationResultIdentifier,
            Dict[str, Union[ExpectationSuiteValidationResult, dict]],
        ],
        evaluation_parameters: dict = None,
    ) -> None:
        self._run_id = run_id
        self._run_results = run_results
        self._evaluation_parameters = evaluation_parameters
        self._success = all(
            [
                run_result["validation_result"].success
                for run_result in run_results.values()
            ]
        )

        self._validation_results = None
        self._data_assets_validated = None
        self._data_asset_validated_by_batch_id = None
        self._validation_result_identifiers = None
        self._expectation_suite_names = None

    @property
    def expectation_suite_names(self) -> List[str]:
        if self._expectation_suite_names is None:
            self._expectation_suite_names = [
                validation_result_identifier.expectation_suite_identifier.expectation_suite_name
                for validation_result_identifier in self._run_results.keys()
            ]
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
        if self._data_asset_validated_by_batch_id is None:
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
            self._data_asset_validated_by_batch_id = assets_validated_by_batch_id
        return self._data_asset_validated_by_batch_id

    @property
    def statistics(self) -> dict:
        raise NotImplementedError

    @property
    def actions_results(self) -> dict:
        raise NotImplementedError

    @property
    def success(self) -> bool:
        raise NotImplementedError

    @property
    def details(self):
        # TODO I'm not sure what type this should return
        raise NotImplementedError

    @property
    def validation_result_by_expectation_suite_identifier(self) -> dict:
        raise NotImplementedError
