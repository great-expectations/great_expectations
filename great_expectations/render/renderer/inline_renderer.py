import logging
from typing import Callable, List, Optional, Union

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.expectations.registry import (
    get_renderer_impl,
    get_renderer_names_with_renderer_prefix,
)
from great_expectations.render.exceptions import InvalidRenderedContentError
from great_expectations.render.renderer.renderer import Renderer
from great_expectations.render.types import (
    AtomicRendererPrefix,
    FailedAtomicRendererName,
    RenderedAtomicContent,
)

try:
    from typing import TypedDict  # type: ignore[attr-defined]
except ImportError:
    from typing_extensions import TypedDict

logger = logging.getLogger(__name__)


class InlineRendererConfig(TypedDict):
    class_name: str
    render_object: Union[ExpectationConfiguration, ExpectationValidationResult]


class InlineRenderer(Renderer):
    def __init__(
        self,
        render_object: Union[ExpectationConfiguration, ExpectationValidationResult],
    ) -> None:
        super().__init__()

        if isinstance(
            render_object, (ExpectationConfiguration, ExpectationValidationResult)
        ):
            self._render_object = render_object
        else:
            raise InvalidRenderedContentError(
                f"InlineRenderer can only be used with an ExpectationConfiguration or ExpectationValidationResult, but {type(render_object)} was used."
            )

    def _get_atomic_rendered_content_for_object(
        self,
        render_object: Union[ExpectationConfiguration, ExpectationValidationResult],
    ) -> List[RenderedAtomicContent]:
        """Gets RenderedAtomicContent for a given ExpectationConfiguration or ExpectationValidationResult.

        Args:
            render_object: The object to render.

        Returns:
            A list of RenderedAtomicContent objects for a given ExpectationConfiguration or ExpectationValidationResult.
        """
        expectation_type: str
        renderer_prefix: AtomicRendererPrefix
        if isinstance(render_object, ExpectationConfiguration):
            expectation_type = render_object.expectation_type
            renderer_prefix = AtomicRendererPrefix.PRESCRIPTIVE
        elif isinstance(render_object, ExpectationValidationResult):
            expectation_type = render_object.expectation_config.expectation_type
            renderer_prefix = AtomicRendererPrefix.DIAGNOSTIC
        else:
            raise InvalidRenderedContentError(
                f"InlineRenderer._get_atomic_rendered_content_for_object can only be used with an ExpectationConfiguration or ExpectationValidationResult, but {type(render_object)} was used."
            )

        renderer_names: List[str] = get_renderer_names_with_renderer_prefix(
            object_name=expectation_type,
            renderer_prefix=renderer_prefix.value,
        )

        rendered_content: List[
            RenderedAtomicContent
        ] = self._get_atomic_rendered_content_from_renderer_names(
            render_object=render_object,
            renderer_names=renderer_names,
            expectation_type=expectation_type,
        )

        return rendered_content

    def _get_atomic_rendered_content_from_renderer_names(
        self,
        render_object: Union[ExpectationConfiguration, ExpectationValidationResult],
        renderer_names: List[str],
        expectation_type: str,
    ) -> List[RenderedAtomicContent]:
        try_renderer_names: List[str] = [
            renderer_name
            for renderer_name in renderer_names
            if renderer_name
            not in [
                FailedAtomicRendererName.PRESCRIPTIVE,
                FailedAtomicRendererName.DIAGNOSTIC,
            ]
        ]

        renderer_rendered_content: Optional[RenderedAtomicContent]
        rendered_content: List[RenderedAtomicContent] = []
        for renderer_name in try_renderer_names:
            try:
                renderer_rendered_content = self._get_renderer_atomic_rendered_content(
                    render_object=render_object,
                    renderer_name=renderer_name,
                    expectation_type=expectation_type,
                )
                rendered_content.append(renderer_rendered_content)
            except Exception:
                logger.info(
                    f'Renderer "{renderer_name}" failed to render Expectation "{expectation_type}".'
                )
                failed_renderer_name: FailedAtomicRendererName
                if isinstance(render_object, ExpectationConfiguration):
                    failed_renderer_name = FailedAtomicRendererName.PRESCRIPTIVE
                    logger.info(
                        f'Renderer "{failed_renderer_name}" will be used to render prescriptive content for ExpectationConfiguration.'
                    )
                else:
                    failed_renderer_name = FailedAtomicRendererName.DIAGNOSTIC
                    logger.info(
                        f'Renderer "{failed_renderer_name}" will be used to render diagnostic content for ExpectationValidationResult.'
                    )

                renderer_rendered_content = (
                    InlineRenderer._get_renderer_atomic_rendered_content(
                        render_object=render_object,
                        renderer_name=failed_renderer_name,
                        expectation_type=expectation_type,
                    )
                )
                rendered_content.append(renderer_rendered_content)

        return rendered_content

    @staticmethod
    def _get_renderer_atomic_rendered_content(
        render_object: Union[ExpectationConfiguration, ExpectationValidationResult],
        renderer_name: Union[str, FailedAtomicRendererName],
        expectation_type: str,
    ) -> Optional[RenderedAtomicContent]:
        renderer_rendered_content: Optional[RenderedAtomicContent] = None
        renderer_tuple: Optional[tuple] = get_renderer_impl(
            object_name=expectation_type, renderer_type=renderer_name
        )
        if renderer_tuple is not None:
            # index 0 is expectation class-name and index 1 is implementation of renderer
            renderer_fn: Callable = renderer_tuple[1]
            if isinstance(render_object, ExpectationConfiguration):
                renderer_rendered_content = renderer_fn(configuration=render_object)
            else:
                renderer_rendered_content = renderer_fn(result=render_object)

        return renderer_rendered_content

    def get_rendered_content(
        self,
    ) -> List[RenderedAtomicContent]:
        """Gets RenderedAtomicContent for a given object.

        Returns:
            RenderedAtomicContent for a given object.
        """
        render_object: Union[
            ExpectationConfiguration, ExpectationValidationResult
        ] = self._render_object

        return self._get_atomic_rendered_content_for_object(render_object=render_object)

    @staticmethod
    def replace_or_keep_existing_rendered_content(
        existing_rendered_content: Optional[List[RenderedAtomicContent]],
        new_rendered_content: List[RenderedAtomicContent],
        failed_renderer_name: str,
    ) -> List[RenderedAtomicContent]:
        new_rendered_content_block_names: List[str] = [
            rendered_content_block.name
            for rendered_content_block in new_rendered_content
        ]

        existing_rendered_content_block_names: List[str] = []
        if existing_rendered_content is not None:
            existing_rendered_content_block_names = [
                rendered_content_block.name
                for rendered_content_block in existing_rendered_content
            ]

        if (
            (existing_rendered_content is None)
            or (failed_renderer_name not in new_rendered_content_block_names)
            or (failed_renderer_name in existing_rendered_content_block_names)
        ):
            return new_rendered_content
        else:
            return existing_rendered_content
