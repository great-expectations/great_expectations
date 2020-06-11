from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.render.renderer.content_block import (
    ExceptionListContentBlockRenderer,
)


def test_exception_list_content_block_renderer():
    # We should grab the exception message and add default formatting
    result = ExceptionListContentBlockRenderer.render(
        [
            ExpectationValidationResult(
                success=False,
                exception_info={
                    "raised_exception": True,
                    "exception_message": "Invalid partition object.",
                    "exception_traceback": 'Traceback (most recent call last):\n  File "/home/user/great_expectations/great_expectations/data_asset/data_asset.py", line 186, in wrapper\n    return_obj = func(self, **evaluation_args)\n  File " /home/user/great_expectations/great_expectations/dataset/dataset.py", line 106, in inner_wrapper\n    evaluation_result = func(self, column, *args, **kwargs)\n  File "/home/user/great_expectations/great_expectations/dataset/dataset.py", line 3388, in expect_column_kl_divergence_to_be_less_than\n    raise ValueError("Invalid partition object.")\nValueError: Invalid partition object.\n',
                },
                expectation_config=ExpectationConfiguration(
                    expectation_type="expect_column_kl_divergence_to_be_less_than",
                    kwargs={
                        "column": "answer",
                        "partition_object": None,
                        "threshold": None,
                        "result_format": "SUMMARY",
                    },
                    meta={"BasicDatasetProfiler": {"confidence": "very low"}},
                ),
            )
        ]
    )

    assert result.to_json_dict() == {
        "content_block_type": "bullet_list",
        "bullet_list": [
            {
                "content_block_type": "string_template",
                "string_template": {
                    "template": "$column: $expectation_type raised an exception: $exception_message",
                    "params": {
                        "column": "answer",
                        "expectation_type": "expect_column_kl_divergence_to_be_less_than",
                        "exception_message": "Invalid partition object.",
                    },
                    "styling": {
                        "classes": ["list-group-item"],
                        "params": {
                            "column": {"classes": ["badge", "badge-primary"]},
                            "expectation_type": {"classes": ["text-monospace"]},
                            "exception_message": {"classes": ["text-monospace"]},
                        },
                    },
                },
            }
        ],
        "styling": {
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
                "styles": {"cursor": "pointer"},
            },
            "body": {"classes": ["list-group", "collapse"]},
        },
        "header": 'Failed expectations <span class="mr-3 triangle"></span>',
    }
