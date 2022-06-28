from typing import Callable, List, Optional, Union

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.expectations.registry import (
    get_renderer_impl,
    get_renderer_names_with_renderer_prefix,
)
from great_expectations.render.renderer.renderer import Renderer
from great_expectations.render.types import RenderedAtomicContent


class InlineRenderer(Renderer):
    def __init__(
        self,
        render_object: ExpectationValidationResult,
    ) -> None:
        super().__init__()

        self._render_object = render_object

    def get_atomic_rendered_content_for_object(
        self,
        render_object: Union[ExpectationConfiguration, ExpectationValidationResult],
    ) -> List[RenderedAtomicContent]:
        """Gets RenderedAtomicContent for a given ExpectationConfiguration or ExpectationValidationResult.

        Args:
            render_object: The object to render

        Returns:
            A list of RenderedAtomicContent objects for a given ExpectationConfiguration or ExpectationValidationResult.
        """
        expectation_type: str
        atomic_renderer_prefix: str
        legacy_renderer_prefix: str
        if isinstance(render_object, ExpectationConfiguration):
            expectation_type = render_object.expectation_type
            atomic_renderer_prefix = "atomic.prescriptive"
            legacy_renderer_prefix = "renderer.prescriptive"
        elif isinstance(render_object, ExpectationValidationResult):
            expectation_type = render_object.expectation_config.expectation_type
            atomic_renderer_prefix = "atomic.diagnostic"
            legacy_renderer_prefix = "renderer.diagnostic"
        else:
            raise ValueError(
                f"object must be of type ExpectationConfiguration or ExpectationValidationResult, but an object of type {type(render_object)} was passed"
            )

        renderer_names: List[str] = get_renderer_names_with_renderer_prefix(
            object_name=expectation_type,
            renderer_prefix=atomic_renderer_prefix,
        )
        if len(renderer_names) == 0:
            renderer_names = get_renderer_names_with_renderer_prefix(
                object_name=expectation_type,
                renderer_prefix=legacy_renderer_prefix,
            )

        rendered_content: List[
            "RenderedAtomicContent"
        ] = self._get_rendered_content_from_renderer_names(
            render_object=render_object,
            renderer_names=renderer_names,
            expectation_type=expectation_type,
        )

        return rendered_content

    def _get_rendered_content_from_renderer_names(
        self,
        render_object: Union[ExpectationConfiguration, ExpectationValidationResult],
        renderer_names: List[str],
        expectation_type: str,
    ) -> List[RenderedAtomicContent]:
        renderer_tuple: Optional[tuple]
        renderer_fn: Callable
        renderer_rendered_content: Union[
            RenderedAtomicContent, List[RenderedAtomicContent]
        ]
        rendered_content: List[RenderedAtomicContent] = []
        for renderer_name in renderer_names:
            renderer_tuple = get_renderer_impl(
                object_name=expectation_type, renderer_type=renderer_name
            )
            if renderer_tuple is not None:
                # index 0 is expectation class-name and index 1 is implementation of renderer
                renderer_fn = renderer_tuple[1]
                if isinstance(render_object, ExpectationConfiguration):
                    renderer_rendered_content = renderer_fn(configuration=render_object)
                else:
                    renderer_rendered_content = renderer_fn(result=render_object)

                if isinstance(renderer_rendered_content, list):
                    rendered_content.extend(renderer_rendered_content)
                else:
                    rendered_content.append(renderer_rendered_content)

        return rendered_content

    def render(self) -> List["RenderedAtomicContent"]:
        render_object: "ExpectationValidationResult" = self._render_object

        return self.get_atomic_rendered_content_for_object(
            render_object=render_object.expectation_config
        ), self.get_atomic_rendered_content_for_object(render_object=render_object)
