import warnings
from collections import defaultdict, Counter

from .renderer import Renderer
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.render.types import (
    RenderedSectionContent,
    RenderedHeaderContent, RenderedStringTemplateContent, RenderedTableContent, RenderedBulletListContent)


class ProfilingResultsOverviewSectionRenderer(Renderer):

    @classmethod
    def render(cls, evrs, section_name=None):

        content_blocks = []
        # NOTE: I don't love the way this builds content_blocks as a side effect.
        # The top-level API is clean and scannable, but the function internals are counterintutitive and hard to test.
        # I wonder if we can enable something like jquery chaining for this. Tha would be concise AND testable.
        # Pressing on for now...
        cls._render_header(evrs, content_blocks)
        cls._render_dataset_info(evrs, content_blocks)
        cls._render_variable_types(evrs, content_blocks)
        cls._render_warnings(evrs, content_blocks)
        cls._render_expectation_types(evrs, content_blocks)

        return RenderedSectionContent(**{
            "section_name": section_name,
            "content_blocks": content_blocks
        })

    @classmethod
    def _render_header(cls, evrs, content_blocks):
        content_blocks.append(RenderedHeaderContent(**{
            "content_block_type": "header",
            "header": "Overview",
            "styling": {
                "classes": ["col-12", ],
                "header": {
                    "classes": ["alert", "alert-secondary"]
                }
            }
        }))

    @classmethod
    def _render_dataset_info(cls, evrs, content_blocks):
        expect_table_row_count_to_be_between_evr = cls._find_evr_by_type(evrs['results'],
                                                                         "expect_table_row_count_to_be_between")

        table_rows = []
        table_rows.append(["Number of variables", len(cls._get_column_list_from_evrs(evrs)), ])

        table_rows.append([
            RenderedStringTemplateContent(**{
                "content_block_type": "string_template",
                "string_template": {
                    "template": "Number of observations",
                    "tooltip": {
                        "content": "expect_table_row_count_to_be_between"
                    },
                    "params": {
                        "tooltip_text": "Number of observations"
                    }
                }
            }),
            "--" if not expect_table_row_count_to_be_between_evr else expect_table_row_count_to_be_between_evr.result["observed_value"]
        ])

        table_rows += [
            ["Missing cells", cls._get_percentage_missing_cells_str(evrs), ],
            # ["Duplicate rows", "0 (0.0%)", ], #TODO: bring back when we have an expectation for this
        ]

        content_blocks.append(RenderedTableContent(**{
            "content_block_type": "table",
            "header": "Dataset info",
            "table": table_rows,
            "styling": {
                "classes": ["col-6"],
                "styles": {
                    "margin-top": "20px"
                },
                "body": {
                    "classes": ["table", "table-sm"]
                }
            },
        }))

    @classmethod
    def _render_variable_types(cls, evrs, content_blocks):

        column_types = cls._get_column_types(evrs)
        # TODO: check if we have the information to make this statement. Do all columns have type expectations?
        column_type_counter = Counter(column_types.values())
        table_rows = [[type, str(column_type_counter[type])] for type in ["int", "float", "string", "unknown"]]

        content_blocks.append(RenderedTableContent(**{
            "content_block_type": "table",
            "header": "Variable types",
            "table": table_rows,
            "styling": {
                "classes": ["col-6", "table-responsive", ],
                "styles": {
                    "margin-top": "20px"
                },
                "body": {
                    "classes": ["table", "table-sm"]
                }
            },
        }))

    @classmethod
    def _render_expectation_types(cls, evrs, content_blocks):

        type_counts = defaultdict(int)

        for evr in evrs.results:
            type_counts[evr.expectation_config.expectation_type] += 1

        # table_rows = sorted(type_counts.items(), key=lambda kv: -1*kv[1])
        bullet_list = sorted(type_counts.items(), key=lambda kv: -1 * kv[1])

        bullet_list = [
            RenderedStringTemplateContent(**{
                "content_block_type": "string_template",
                "string_template": {
                    "template": "$expectation_type $expectation_count",
                    "params": {
                        "expectation_type": tr[0],
                        "expectation_count": tr[1],
                    },
                    "styling": {
                        "classes": ["list-group-item", "d-flex", "justify-content-between", "align-items-center"],
                        "params": {
                            "expectation_count": {
                                "classes": ["badge", "badge-secondary", "badge-pill"],
                            }
                        }
                    }
                }
            }) for tr in bullet_list]

        content_blocks.append(RenderedBulletListContent(**{
            "content_block_type": "bullet_list",
            "header": 'Expectation types <span class="mr-3 triangle"></span>',
            "bullet_list": bullet_list,
            "styling": {
                "classes": ["col-12"],
                "styles": {
                    "margin-top": "20px"
                },
                "header": {
                    "classes": ["collapsed"],
                    "attributes": {
                        "data-toggle": "collapse",
                        "href": "#{{content_block_id}}-body",
                        "aria-expanded": "true",
                        "aria-controls": "collapseExample",
                    },
                    "styles": {
                        "cursor": "pointer"
                    }
                },
                "body": {
                    "classes": ["list-group", "collapse"],
                },
            },
        }))

    @classmethod
    def _render_warnings(cls, evrs, content_blocks):
        return

        # def render_warning_row(template, column, n, p, badge_label):
        #     return [{
        #         "template": template,
        #         "params": {
        #             "column": column,
        #             "n": n,
        #             "p": p,
        #         },
        #         "styling": {
        #             "params": {
        #                 "column": {
        #                     "classes": ["badge", "badge-primary", ]
        #                 }
        #             }
        #         }
        #     }, {
        #         "template": "$badge_label",
        #         "params": {
        #             "badge_label": badge_label,
        #         },
        #         "styling": {
        #             "params": {
        #                 "badge_label": {
        #                     "classes": ["badge", "badge-warning", ]
        #                 }
        #             }
        #         }
        #     }]

        # table_rows = [
        #     render_warning_row(
        #         "$column has $n ($p%) missing values", "Age", 177, 19.9, "Missing"),
        #     render_warning_row(
        #         "$column has a high cardinality: $n distinct values", "Cabin", 148, None, "Warning"),
        #     render_warning_row(
        #         "$column has $n ($p%) missing values", "Cabin", 687, 77.1, "Missing"),
        #     render_warning_row(
        #         "$column has $n (< $p%) zeros", "Fare", 15, "0.1", "Zeros"),
        #     render_warning_row(
        #         "$column has $n (< $p%) zeros", "Parch", 678, "76.1", "Zeros"),
        #     render_warning_row(
        #         "$column has $n (< $p%) zeros", "SibSp", 608, "68.2", "Zeros"),
        # ]

        # content_blocks.append({
        #     "content_block_type": "table",
        #     "header": "Warnings",
        #     "table": table_rows,
        #     "styling": {
        #         "classes": ["col-12"],
        #         "styles": {
        #             "margin-top": "20px"
        #         },
        #         "body": {
        #             "classes": ["table", "table-sm"]
        #         }
        #     },
        # })

    @classmethod
    def _get_percentage_missing_cells_str(cls, evrs):

        columns = cls._get_column_list_from_evrs(evrs)
        if not columns or len(columns) == 0:
            warnings.warn("Cannot get % of missing cells - column list is empty")
            return "?"

        expect_column_values_to_not_be_null_evrs = cls._find_all_evrs_by_type(evrs.results,
                                                                              "expect_column_values_to_not_be_null")

        if len(columns) > len(expect_column_values_to_not_be_null_evrs):
            warnings.warn(
                "Cannot get % of missing cells - not all columns have expect_column_values_to_not_be_null expectations")
            return "?"

        # assume 100.0 missing for columns where ["result"]["unexpected_percent"] is not available
        return "{0:.2f}%".format(sum([evr.result["unexpected_percent"] if "unexpected_percent" in evr.result and
                                                                             evr.result["unexpected_percent"] is not None else 100.0
                                      for evr in expect_column_values_to_not_be_null_evrs]) / len(columns))

    @classmethod
    def _get_column_types(cls, evrs):
        columns = cls._get_column_list_from_evrs(evrs)

        type_evrs = cls._find_all_evrs_by_type(evrs.results, "expect_column_values_to_be_in_type_list") + \
                    cls._find_all_evrs_by_type(evrs.results, "expect_column_values_to_be_of_type")

        column_types = {}
        for column in columns:
            column_types[column] = "unknown"

        for evr in type_evrs:
            column = evr.expectation_config.kwargs["column"]
            if evr.expectation_config.expectation_type == "expect_column_values_to_be_in_type_list":
                if evr.expectation_config.kwargs["type_list"] is None:
                    column_types[column] = "unknown"
                    continue
                else:
                    expected_types = set(evr.expectation_config.kwargs["type_list"])
            else:  # assuming expect_column_values_to_be_of_type
                expected_types = {[evr.expectation_config.kwargs["type_"]]}

            if expected_types.issubset(BasicDatasetProfiler.INT_TYPE_NAMES):
                column_types[column] = "int"
            elif expected_types.issubset(BasicDatasetProfiler.FLOAT_TYPE_NAMES):
                column_types[column] = "float"
            elif expected_types.issubset(BasicDatasetProfiler.STRING_TYPE_NAMES):
                column_types[column] = "string"
            elif expected_types.issubset(BasicDatasetProfiler.DATETIME_TYPE_NAMES):
                column_types[column] = "datetime"
            elif expected_types.issubset(BasicDatasetProfiler.BOOLEAN_TYPE_NAMES):
                column_types[column] = "bool"
            else:
                warnings.warn("The expected type list is not a subset of any of the profiler type sets: {0:s}".format(
                    str(expected_types)))
                column_types[column] = "unknown"

        return column_types
