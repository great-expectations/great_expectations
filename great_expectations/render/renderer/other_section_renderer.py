import json
from string import Template

from .renderer import Renderer
from .content_block import(
    ValueListContentBlockRenderer,
    GraphContentBlockRenderer,
    TableContentBlockRenderer,
    PrescriptiveBulletListContentBlockRenderer
)


class DescriptiveOverviewSectionRenderer(Renderer):

    @classmethod
    def render(cls, evrs, column=None):

        content_blocks = []
        # NOTE: I don't love the way this builds content_blocks as a side effect.
        # The top-level API is clean and scannable, but the function internals are counterintutitive and hard to test.
        # I wonder if we can enable something like jquery chaining for this. Tha would be concise AND testable.
        # Pressing on for now...
        cls._render_header(evrs, content_blocks)
        cls._render_dataset_info(evrs, content_blocks)
        cls._render_variable_types(evrs, content_blocks)
        cls._render_warnings(evrs, content_blocks)

        return {
            "section_name": column,
            "content_blocks": content_blocks
        }

    @classmethod
    def _render_header(cls, evrs, content_blocks):
        content_blocks.append({
            "content_block_type": "header",
            "header": "Overview",
            "styling": {
                "classes": ["col-12"]
            }
        })

    @classmethod
    def _render_dataset_info(cls, evrs, content_blocks):

        table_rows = [
            ["Number of variables", "12", ],
            ["Number of observations", "891", ],
            ["Missing cells", "866 (8.1%)", ],
            ["Duplicate rows", "0 (0.0%)", ],
            ["Total size in memory",	"83.6 KiB", ],
            ["Average record size in memory", "96.1 B", ],
        ]

        content_blocks.append({
            "content_block_type": "table",
            "header": "Dataset info",
            "table_rows": table_rows,
            "styling": {
                "classes": ["col-6"]
            },
        })

    @classmethod
    def _render_variable_types(cls, evrs, content_blocks):

        table_rows = [
            ["Numeric", "5", ],
            ["Categorical", "5", ],
            ["Boolean", "1", ],
            ["Date", "0", ],
            ["URL", "0", ],
            ["Text (Unique)", "1", ],
            ["Rejected", "0", ],
            ["Unsupported", "0", ],
        ]

        content_blocks.append({
            "content_block_type": "table",
            "header": "Variable types",
            "table_rows": table_rows,
            "styling": {
                "classes": ["col-6"]
            },
        })

    @classmethod
    def _render_warnings(cls, evrs, content_blocks):

        def render_warning_row(template, column, n, p, badge_label):
            return [{
                "template": template,
                "params": {
                    "column": column,
                    "n": n,
                    "p": p,
                },
                "styling": {
                    "params": {
                        "column": {
                            "classes": ["badge", "badge-primary", ]
                        }
                    }
                }
            }, {
                "template": "$badge_label",
                "params": {
                    "badge_label": badge_label,
                },
                "styling": {
                    "params": {
                        "badge_label": {
                            "classes": ["badge", "badge-warning", ]
                        }
                    }
                }
            }]

        table_rows = [
            render_warning_row(
                "$column has $n ($p%) missing values", "Age", 177, 19.9, "Missing"),
            render_warning_row(
                "$column has a high cardinality: $n distinct values", "Cabin", 148, None, "Warning"),
            render_warning_row(
                "$column has $n ($p%) missing values", "Cabin", 687, 77.1, "Missing"),
            render_warning_row(
                "$column has $n (< $p%) zeros", "Fare", 15, "0.1", "Zeros"),
            render_warning_row(
                "$column has $n (< $p%) zeros", "Parch", 678, "76.1", "Zeros"),
            render_warning_row(
                "$column has $n (< $p%) zeros", "SibSp", 608, "68.2", "Zeros"),
        ]

        content_blocks.append({
            "content_block_type": "table",
            "header": "Warnings",
            "table_rows": table_rows,
            "styling": {
                "classes": ["col-12"],
                "styles": {
                    "margin-top": "20px"
                }
            },
        })
