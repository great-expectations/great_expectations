from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from great_expectations.render import (
    RenderedBulletListContent,
    RenderedStringTemplateContent,
)
from great_expectations.render.renderer.content_block.content_block import (
    ContentBlockRenderer,
)

if TYPE_CHECKING:
    from great_expectations.core import (
        ExpectationValidationResult,
    )
    from great_expectations.expectations.expectation_configuration import (
        ExpectationConfiguration,
    )


class ExceptionListContentBlockRenderer(ContentBlockRenderer):
    """Render a bullet list of exception messages raised for provided EVRs"""

    _rendered_component_type = RenderedBulletListContent
    _content_block_type = "bullet_list"

    _default_header = 'Failed expectations <span class="mr-3 triangle"></span>'

    _default_content_block_styling = {
        "classes": ["col-12"],
        "styles": {"margin-top": "20px"},
        "header": {
            "classes": ["collapsed"],
            "attributes": {
                "data-toggle": "collapse",
                "href": "#{{content_block_id}}-body",
                "role": "button",
                "aria-expanded": "true",
                "aria-controls": "collapseExample",
            },
            "styles": {
                "cursor": "pointer",
            },
        },
        "body": {
            "classes": ["list-group", "collapse"],
        },
    }

    _default_element_styling = {
        "classes": [
            "list-group-item"
        ],  # "d-flex", "justify-content-between", "align-items-center"],
        "params": {
            "column": {"classes": ["badge", "badge-primary"]},
            "expectation_type": {"classes": ["text-monospace"]},
            "exception_message": {"classes": ["text-monospace"]},
        },
    }

    @classmethod
    def render(cls, render_object, **kwargs):
        return super().render(render_object=render_object, exception_list_content_block=True)

    @classmethod
    def _missing_content_block_fn(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name") is not False
        styling = runtime_configuration.get("styling")
        # Only render EVR objects for which an exception was raised
        if result.exception_info["raised_exception"] is True:
            template_str = "$expectation_type raised an exception: $exception_message"
            if include_column_name:
                template_str = f"$column: {template_str}"

            try:
                column = result.expectation_config.kwargs["column"]
            except KeyError:
                column = None
            return [
                RenderedStringTemplateContent(
                    **{
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": template_str,
                            "params": {
                                "column": column,
                                "expectation_type": result.expectation_config.type,
                                "exception_message": result.exception_info["exception_message"],
                            },
                            "styling": styling,
                        },
                    }
                )
            ]
