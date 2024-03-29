from typing import TYPE_CHECKING, List, Union
from great_expectations.checkpoint.actions import ValidationAction
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)
from great_expectations.compatibility.typing_extensions import override
from great_expectations.data_context.types.resource_identifiers import (
    GXCloudIdentifier,
    ValidationResultIdentifier,
)

if TYPE_CHECKING:
    from great_expectations.core.expectation_validation_result import (
        ExpectationSuiteValidationResult,
    )
    from great_expectations.data_context import AbstractDataContext


class DocsAction(ValidationAction):
    def __init__(
        self,
        data_context: AbstractDataContext,
        site_names: Union[List[str], str, None] = None,
    ) -> None:
        """
        :param data_context: Data Context
        :param site_names: *optional* List of site names for building data docs
        """
        super().__init__(data_context)
        self._site_names = site_names

    @override
    def _run(  # type: ignore[override] # signature does not match parent  # noqa: PLR0913
        self,
        validation_result_suite: ExpectationSuiteValidationResult,
        validation_result_suite_identifier: Union[
            ValidationResultIdentifier, GXCloudIdentifier
        ],
        data_asset,
        payload=None,
        expectation_suite_identifier=None,
        checkpoint_identifier=None,
    ):
        # <snippet name="version-0.17.23 great_expectations/checkpoint/actions.py empty dict">
        data_docs_validation_results: dict = {}
        # </snippet>
        if self._using_cloud_context:
            return data_docs_validation_results

        # get the URL for the validation result
        # <snippet name="version-0.17.23 great_expectations/checkpoint/actions.py get_docs_sites_urls">
        docs_site_urls_list = self.data_context.get_docs_sites_urls(
            resource_identifier=validation_result_suite_identifier,
            site_names=self._site_names,  # type: ignore[arg-type] # could be a `str`
        )
        # </snippet>
        # process payload
        # <snippet name="version-0.17.23 great_expectations/checkpoint/actions.py iterate">
        for sites in docs_site_urls_list:
            data_docs_validation_results[sites["site_name"]] = sites["site_url"]
        # </snippet>
        return data_docs_validation_results
