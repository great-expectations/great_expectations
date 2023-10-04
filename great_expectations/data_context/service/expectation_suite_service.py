from __future__ import annotations

import warnings
from typing import (
    TYPE_CHECKING,
    Callable,
    List,
    Optional,
    Sequence,
    Type,
    Union,
    cast,
)

import great_expectations.exceptions as gx_exceptions
from great_expectations.core import ExpectationSuite
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
)

if TYPE_CHECKING:
    from great_expectations.core.expectation_configuration import (
        ExpectationConfiguration,
    )
    from great_expectations.data_context.store.expectations_store import (
        ExpectationsStore,
    )
    from great_expectations.data_context.types.base import IncludeRenderedContentConfig
    from great_expectations.data_context.types.resource_identifiers import (
        GXCloudIdentifier,
    )
    from great_expectations.execution_engine import ExecutionEngine


class ExpectationService:
    def __init__(
        self,
        expectations_store: ExpectationsStore,
        include_rendered_content: IncludeRenderedContentConfig,
    ) -> None:
        self._expectations_store = expectations_store
        self._include_rendered_content = include_rendered_content

    def save_expectation_suite(
        self,
        expectation_suite: ExpectationSuite,
        expectation_suite_name: Optional[str] = None,
        overwrite_existing: bool = True,
        include_rendered_content: Optional[bool] = None,
        **kwargs: Optional[dict],
    ) -> None:
        # deprecated-v0.15.48
        warnings.warn(
            "save_expectation_suite is deprecated as of v0.15.48 and will be removed in v0.18. "
            "Please use update_expectation_suite or add_or_update_expectation_suite instead.",
            DeprecationWarning,
        )
        return self._save_expectation_suite(
            expectation_suite=expectation_suite,
            expectation_suite_name=expectation_suite_name,
            overwrite_existing=overwrite_existing,
            include_rendered_content=include_rendered_content,
            **kwargs,
        )

    def _save_expectation_suite(
        self,
        expectation_suite: ExpectationSuite,
        expectation_suite_name: Optional[str] = None,
        overwrite_existing: bool = True,
        include_rendered_content: Optional[bool] = None,
        **kwargs: Optional[dict],
    ) -> None:
        if expectation_suite_name is None:
            key = ExpectationSuiteIdentifier(
                expectation_suite_name=expectation_suite.expectation_suite_name
            )
        else:
            expectation_suite.expectation_suite_name = expectation_suite_name
            key = ExpectationSuiteIdentifier(
                expectation_suite_name=expectation_suite_name
            )
        if self._expectations_store.has_key(key) and not overwrite_existing:  # : @601
            raise gx_exceptions.DataContextError(
                "expectation_suite with name {} already exists. If you would like to overwrite this "
                "expectation_suite, set overwrite_existing=True.".format(
                    expectation_suite_name
                )
            )
        include_rendered_content = (
            self._determine_if_expectation_suite_include_rendered_content(
                include_rendered_content=include_rendered_content
            )
        )
        if include_rendered_content:
            expectation_suite.render()
        return self._expectations_store.set(key, expectation_suite, **kwargs)

    def list_expectation_suite_names(self) -> List[str]:
        sorted_expectation_suite_names = [
            i.expectation_suite_name for i in self.list_expectation_suites()  # type: ignore[union-attr]
        ]
        sorted_expectation_suite_names.sort()
        return sorted_expectation_suite_names

    def list_expectation_suites(
        self,
    ) -> Optional[Union[List[str], List[GXCloudIdentifier]]]:
        try:
            keys = self._expectations_store.list_keys()
        except KeyError as e:
            raise gx_exceptions.InvalidConfigError(
                f"Unable to find configured store: {e!s}"
            )
        return keys  # type: ignore[return-value]

    def create_expectation_suite(
        self,
        expectation_suite_name: str,
        overwrite_existing: bool = False,
        **kwargs: Optional[dict],
    ) -> ExpectationSuite:
        # deprecated-v0.15.48
        warnings.warn(
            "create_expectation_suite is deprecated as of v0.15.48 and will be removed in v0.18. "
            "Please use add_expectation_suite or add_or_update_expectation_suite instead.",
            DeprecationWarning,
        )
        return self._add_expectation_suite(
            expectation_suite_name=expectation_suite_name,
            overwrite_existing=overwrite_existing,
        )

    def add_expectation_suite(  # noqa: PLR0913
        self,
        expectation_suite_name: str | None = None,
        id: str | None = None,
        expectations: list[dict | ExpectationConfiguration] | None = None,
        evaluation_parameters: dict | None = None,
        data_asset_type: str | None = None,
        execution_engine_type: Type[ExecutionEngine] | None = None,
        meta: dict | None = None,
        expectation_suite: ExpectationSuite | None = None,
    ) -> ExpectationSuite:
        return self._add_expectation_suite(
            expectation_suite_name=expectation_suite_name,
            id=id,
            expectations=expectations,
            evaluation_parameters=evaluation_parameters,
            data_asset_type=data_asset_type,
            execution_engine_type=execution_engine_type,
            meta=meta,
            expectation_suite=expectation_suite,
            overwrite_existing=False,  # `add` does not resolve collisions
        )

    def _add_expectation_suite(  # noqa: PLR0913
        self,
        expectation_suite_name: str | None = None,
        id: str | None = None,
        expectations: Sequence[dict | ExpectationConfiguration] | None = None,
        evaluation_parameters: dict | None = None,
        data_asset_type: str | None = None,
        execution_engine_type: Type[ExecutionEngine] | None = None,
        meta: dict | None = None,
        overwrite_existing: bool = False,
        expectation_suite: ExpectationSuite | None = None,
        **kwargs,
    ) -> ExpectationSuite:
        if not isinstance(overwrite_existing, bool):
            raise ValueError("overwrite_existing must be of type bool.")

        self._validate_expectation_suite_xor_expectation_suite_name(
            expectation_suite, expectation_suite_name
        )

        if not expectation_suite:
            # type narrowing
            assert isinstance(
                expectation_suite_name, str
            ), "expectation_suite_name must be specified."

            expectation_suite = ExpectationSuite(
                expectation_suite_name=expectation_suite_name,
                data_context=self,
                ge_cloud_id=id,
                expectations=expectations,
                evaluation_parameters=evaluation_parameters,
                data_asset_type=data_asset_type,
                execution_engine_type=execution_engine_type,
                meta=meta,
            )

        return self._persist_suite_with_store(
            expectation_suite=expectation_suite,
            overwrite_existing=overwrite_existing,
            **kwargs,
        )

    def _persist_suite_with_store(
        self,
        expectation_suite: ExpectationSuite,
        overwrite_existing: bool,
        **kwargs,
    ) -> ExpectationSuite:
        key = ExpectationSuiteIdentifier(
            expectation_suite_name=expectation_suite.expectation_suite_name
        )

        persistence_fn: Callable
        if overwrite_existing:
            persistence_fn = self._expectations_store.add_or_update
        else:
            persistence_fn = self._expectations_store.add

        persistence_fn(key=key, value=expectation_suite, **kwargs)
        return expectation_suite

    def update_expectation_suite(
        self,
        expectation_suite: ExpectationSuite,
    ) -> ExpectationSuite:
        return self._update_expectation_suite(expectation_suite=expectation_suite)

    def _update_expectation_suite(
        self,
        expectation_suite: ExpectationSuite,
    ) -> ExpectationSuite:
        name = expectation_suite.expectation_suite_name
        id = expectation_suite.ge_cloud_id
        key = self._determine_key_for_suite_update(name=name, id=id)
        self._expectations_store.update(key=key, value=expectation_suite)
        return expectation_suite

    def _determine_key_for_suite_update(
        self, name: str, id: str | None
    ) -> Union[ExpectationSuiteIdentifier, GXCloudIdentifier]:
        return ExpectationSuiteIdentifier(name)

    def add_or_update_expectation_suite(  # noqa: PLR0913
        self,
        expectation_suite_name: str | None = None,
        id: str | None = None,
        expectations: list[dict | ExpectationConfiguration] | None = None,
        evaluation_parameters: dict | None = None,
        data_asset_type: str | None = None,
        execution_engine_type: Type[ExecutionEngine] | None = None,
        meta: dict | None = None,
        expectation_suite: ExpectationSuite | None = None,
    ) -> ExpectationSuite:
        self._validate_expectation_suite_xor_expectation_suite_name(
            expectation_suite, expectation_suite_name
        )

        if not expectation_suite:
            # type narrowing
            assert isinstance(
                expectation_suite_name, str
            ), "expectation_suite_name must be specified."

            expectation_suite = ExpectationSuite(
                expectation_suite_name=expectation_suite_name,
                data_context=self,
                ge_cloud_id=id,
                expectations=expectations,
                evaluation_parameters=evaluation_parameters,
                data_asset_type=data_asset_type,
                execution_engine_type=execution_engine_type,
                meta=meta,
            )

        try:
            existing = self.get_expectation_suite(
                expectation_suite_name=expectation_suite.name
            )
        except gx_exceptions.DataContextError:
            # not found
            return self._add_expectation_suite(expectation_suite=expectation_suite)

        # The suite object must have an ID in order to request a PUT to GX Cloud.
        expectation_suite.ge_cloud_id = existing.ge_cloud_id
        return self._update_expectation_suite(expectation_suite=expectation_suite)

    def delete_expectation_suite(
        self,
        expectation_suite_name: str | None = None,
        ge_cloud_id: str | None = None,
        id: str | None = None,
    ) -> None:
        key = ExpectationSuiteIdentifier(expectation_suite_name)  # type: ignore[arg-type]
        if not self._expectations_store.has_key(key):
            raise gx_exceptions.DataContextError(
                f"expectation_suite with name {expectation_suite_name} does not exist."
            )
        self._expectations_store.remove_key(key)

    def get_expectation_suite(
        self,
        expectation_suite_name: str | None = None,
        include_rendered_content: bool | None = None,
        ge_cloud_id: str | None = None,
    ) -> ExpectationSuite:
        if ge_cloud_id is not None:
            # deprecated-v0.15.45
            warnings.warn(
                "ge_cloud_id is deprecated as of v0.15.45 and will be removed in v0.16. Please use"
                "expectation_suite_name instead",
                DeprecationWarning,
            )

        if expectation_suite_name:
            key = ExpectationSuiteIdentifier(
                expectation_suite_name=expectation_suite_name
            )
        else:
            raise ValueError("expectation_suite_name must be provided")

        if include_rendered_content is None:
            include_rendered_content = (
                self._determine_if_expectation_suite_include_rendered_content()
            )

        if self._expectations_store.has_key(key):
            expectations_schema_dict: dict = cast(
                dict, self._expectations_store.get(key)
            )
            # create the ExpectationSuite from constructor
            expectation_suite = ExpectationSuite(
                **expectations_schema_dict, data_context=self
            )
            if include_rendered_content:
                expectation_suite.render()
            return expectation_suite

        else:
            raise gx_exceptions.DataContextError(
                f"expectation_suite {expectation_suite_name} not found"
            )

    @staticmethod
    def _validate_expectation_suite_xor_expectation_suite_name(
        expectation_suite: Optional[ExpectationSuite] = None,
        expectation_suite_name: Optional[str] = None,
    ) -> None:
        if expectation_suite_name is not None and expectation_suite is not None:
            raise TypeError(
                "Only one of expectation_suite_name or expectation_suite may be specified."
            )
        if expectation_suite_name is None and expectation_suite is None:
            raise TypeError(
                "One of expectation_suite_name or expectation_suite must be specified."
            )

    def _determine_if_expectation_suite_include_rendered_content(
        self, include_rendered_content: Optional[bool] = None
    ) -> bool:
        if include_rendered_content is None:
            if (
                self._include_rendered_content.expectation_suite is True
                or self._include_rendered_content.globally is True
            ):
                return True
            else:
                return False
        return include_rendered_content
