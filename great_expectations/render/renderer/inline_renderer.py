from typing import Callable, List, Optional, Union

from great_expectations.expectations.registry import (
    get_renderer_impl,
    get_renderer_names_with_renderer_prefix,
)
from great_expectations.render.renderer.renderer import Renderer
from great_expectations.render.types import RenderedContent


class InlineRenderer(Renderer):
    def __init__(
        self,
        render_object: Union["ExpectationConfiguration", "ExpectationValidationResult"],
    ) -> None:
        super().__init__()

        self._render_object = render_object

    def get_atomic_rendered_content_for_object(self) -> List[RenderedContent]:
        """Gets RenderedAtomicContent for a given ExpectationConfiguration or ExpectationValidationResult.

        Returns:
            A list of RenderedAtomicContent objects for a given ExpectationConfiguration or ExpectationValidationResult.
        """
        render_object: Union[
            "ExpectationConfiguration", "ExpectationValidationResult"
        ] = self._render_object

        # This workaround was required to avoid circular imports
        render_object_type: str
        if hasattr(render_object, "expectation_type"):
            render_object_type = "ExpectationConfiguration"
        elif hasattr(render_object, "expectation_config"):
            render_object_type = "ExpectationValidationResult"
        else:
            raise ValueError(
                f"object must be of type ExpectationConfiguration or ExpectationValidationResult, but an object of type {type(render_object)} was passed"
            )

        expectation_type: str
        atomic_renderer_prefix: str
        legacy_renderer_prefix: str
        if render_object_type == "ExpectationConfiguration":
            expectation_type = render_object.expectation_type
            atomic_renderer_prefix = "atomic.prescriptive"
            legacy_renderer_prefix = "renderer.prescriptive"
        else:
            expectation_type = render_object.expectation_config.expectation_type
            atomic_renderer_prefix = "atomic.diagnostic"
            legacy_renderer_prefix = "renderer.diagnostic"

        renderer_names: List[str] = get_renderer_names_with_renderer_prefix(
            object_name=expectation_type,
            renderer_prefix=atomic_renderer_prefix,
        )
        if len(renderer_names) == 0:
            renderer_names = get_renderer_names_with_renderer_prefix(
                object_name=expectation_type,
                renderer_prefix=legacy_renderer_prefix,
            )

        renderer_tuple: Optional[tuple]
        renderer_fn: Callable
        renderer_rendered_content: Optional[
            Union[RenderedContent, List[RenderedContent]]
        ]
        rendered_content: List[RenderedContent] = []
        for renderer_name in renderer_names:
            renderer_tuple = get_renderer_impl(
                object_name=expectation_type, renderer_type=renderer_name
            )
            if renderer_tuple is not None:
                # index 0 is expectation class-name and index 1 is implementation of renderer
                renderer_fn = renderer_tuple[1]
                if render_object_type == "ExpectationConfiguration":
                    renderer_rendered_content = renderer_fn(configuration=render_object)
                else:
                    renderer_rendered_content = renderer_fn(result=render_object)

                if isinstance(renderer_rendered_content, list):
                    rendered_content.extend(renderer_rendered_content)
                else:
                    rendered_content.append(renderer_rendered_content)

        return rendered_content

    def render(self) -> List[RenderedContent]:
        return self.get_atomic_rendered_content_for_object()
