from great_expectations.render.renderer.content_block.content_block import (
    ContentBlockRenderer,
)
from great_expectations.render.types import RenderedStringTemplateContent


class ExpectationStringRenderer(ContentBlockRenderer):
    @classmethod
    def _missing_content_block_fn(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "styling": {"parent": {"classes": ["alert", "alert-warning"]}},
                    "string_template": {
                        "template": "$expectation_type(**$kwargs)",
                        "params": {
                            "expectation_type": configuration.expectation_type,
                            "kwargs": configuration.kwargs,
                        },
                        "styling": {
                            "params": {
                                "expectation_type": {
                                    "classes": ["badge", "badge-warning"],
                                }
                            }
                        },
                    },
                }
            )
        ]
