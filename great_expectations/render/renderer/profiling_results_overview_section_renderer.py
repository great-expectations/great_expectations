import warnings
from collections import Counter, defaultdict

from great_expectations.core.profiler_types_mapping import ProfilerTypeMapping
from great_expectations.render.renderer.renderer import Renderer
from great_expectations.render.types import (
    CollapseContent,
    RenderedBulletListContent,
    RenderedHeaderContent,
    RenderedSectionContent,
    RenderedStringTemplateContent,
    RenderedTableContent,
)


class ProfilingResultsOverviewSectionRenderer(Renderer):
    @classmethod
    def render(cls, evrs, section_name=None):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        content_blocks = []
        cls._render_header(evrs, content_blocks)
        cls._render_dataset_info(evrs, content_blocks)
        cls._render_variable_types(evrs, content_blocks)
        cls._render_warnings(evrs, content_blocks)
        cls._render_expectation_types(evrs, content_blocks)
        return RenderedSectionContent(
            **{"section_name": section_name, "content_blocks": content_blocks}
        )

    @classmethod
    def _render_header(cls, evrs, content_blocks) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        content_blocks.append(
            RenderedHeaderContent(
                **{
                    "content_block_type": "header",
                    "header": RenderedStringTemplateContent(
                        **{
                            "content_block_type": "string_template",
                            "string_template": {
                                "template": "Overview",
                                "tag": "h5",
                                "styling": {"classes": ["m-0"]},
                            },
                        }
                    ),
                    "styling": {
                        "classes": ["col-12", "p-0"],
                        "header": {"classes": ["alert", "alert-secondary"]},
                    },
                }
            )
        )

    @classmethod
    def _render_dataset_info(cls, evrs, content_blocks) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        expect_table_row_count_to_be_between_evr = cls._find_evr_by_type(
            evrs["results"], "expect_table_row_count_to_be_between"
        )
        table_rows = []
        table_rows.append(
            ["Number of variables", len(cls._get_column_list_from_evrs(evrs))]
        )
        table_rows.append(
            [
                RenderedStringTemplateContent(
                    **{
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "Number of observations",
                            "tooltip": {
                                "content": "expect_table_row_count_to_be_between"
                            },
                            "params": {"tooltip_text": "Number of observations"},
                        },
                    }
                ),
                (
                    "--"
                    if (not expect_table_row_count_to_be_between_evr)
                    else expect_table_row_count_to_be_between_evr.result[
                        "observed_value"
                    ]
                ),
            ]
        )
        table_rows += [["Missing cells", cls._get_percentage_missing_cells_str(evrs)]]
        content_blocks.append(
            RenderedTableContent(
                **{
                    "content_block_type": "table",
                    "header": RenderedStringTemplateContent(
                        **{
                            "content_block_type": "string_template",
                            "string_template": {
                                "template": "Dataset info",
                                "tag": "h6",
                            },
                        }
                    ),
                    "table": table_rows,
                    "styling": {
                        "classes": ["col-6", "mt-1", "p-1"],
                        "body": {"classes": ["table", "table-sm"]},
                    },
                }
            )
        )

    @classmethod
    def _render_variable_types(cls, evrs, content_blocks) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        column_types = cls._get_column_types(evrs)
        column_type_counter = Counter(column_types.values())
        table_rows = [
            [type, str(column_type_counter[type])]
            for type in ["int", "float", "string", "datetime", "bool", "unknown"]
        ]
        content_blocks.append(
            RenderedTableContent(
                **{
                    "content_block_type": "table",
                    "header": RenderedStringTemplateContent(
                        **{
                            "content_block_type": "string_template",
                            "string_template": {
                                "template": "Variable types",
                                "tag": "h6",
                            },
                        }
                    ),
                    "table": table_rows,
                    "styling": {
                        "classes": ["col-6", "table-responsive", "mt-1", "p-1"],
                        "body": {"classes": ["table", "table-sm"]},
                    },
                }
            )
        )

    @classmethod
    def _render_expectation_types(cls, evrs, content_blocks) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        type_counts = defaultdict(int)
        for evr in evrs.results:
            type_counts[evr.expectation_config.expectation_type] += 1
        bullet_list_items = sorted(type_counts.items(), key=(lambda kv: ((-1) * kv[1])))
        bullet_list_items = [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "$expectation_type $expectation_count",
                        "params": {
                            "expectation_type": tr[0],
                            "expectation_count": tr[1],
                        },
                        "styling": {
                            "classes": [
                                "list-group-item",
                                "d-flex",
                                "justify-content-between",
                                "align-items-center",
                            ],
                            "params": {
                                "expectation_count": {
                                    "classes": [
                                        "badge",
                                        "badge-secondary",
                                        "badge-pill",
                                    ]
                                }
                            },
                        },
                    },
                    "styling": {"parent": {"styles": {"list-style-type": "none"}}},
                }
            )
            for tr in bullet_list_items
        ]
        bullet_list = RenderedBulletListContent(
            **{
                "content_block_type": "bullet_list",
                "bullet_list": bullet_list_items,
                "styling": {
                    "classes": ["col-12", "mt-1"],
                    "body": {"classes": ["list-group"]},
                },
            }
        )
        bullet_list_collapse = CollapseContent(
            **{
                "collapse_toggle_link": "Show Expectation Types...",
                "collapse": [bullet_list],
                "styling": {"classes": ["col-12", "p-1"]},
            }
        )
        content_blocks.append(bullet_list_collapse)

    @classmethod
    def _render_warnings(cls, evrs, content_blocks):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return

    @classmethod
    def _get_percentage_missing_cells_str(cls, evrs):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        columns = cls._get_column_list_from_evrs(evrs)
        if (not columns) or (len(columns) == 0):
            warnings.warn("Cannot get % of missing cells - column list is empty")
            return "?"
        expect_column_values_to_not_be_null_evrs = cls._find_all_evrs_by_type(
            evrs.results, "expect_column_values_to_not_be_null"
        )
        if len(columns) > len(expect_column_values_to_not_be_null_evrs):
            warnings.warn(
                "Cannot get % of missing cells - not all columns have expect_column_values_to_not_be_null expectations"
            )
            return "?"
        return "{:.2f}%".format(
            sum(
                (
                    evr.result["unexpected_percent"]
                    if (
                        ("unexpected_percent" in evr.result)
                        and (evr.result["unexpected_percent"] is not None)
                    )
                    else 100.0
                )
                for evr in expect_column_values_to_not_be_null_evrs
            )
            / len(columns)
        )

    @classmethod
    def _get_column_types(cls, evrs):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        columns = cls._get_column_list_from_evrs(evrs)
        type_evrs = cls._find_all_evrs_by_type(
            evrs.results, "expect_column_values_to_be_in_type_list"
        ) + cls._find_all_evrs_by_type(
            evrs.results, "expect_column_values_to_be_of_type"
        )
        column_types = {}
        for column in columns:
            column_types[column] = "unknown"
        for evr in type_evrs:
            column = evr.expectation_config.kwargs["column"]
            if (
                evr.expectation_config.expectation_type
                == "expect_column_values_to_be_in_type_list"
            ):
                if evr.expectation_config.kwargs["type_list"] is None:
                    column_types[column] = "unknown"
                    continue
                else:
                    expected_types = set(evr.expectation_config.kwargs["type_list"])
            else:
                expected_types = {evr.expectation_config.kwargs["type_"]}
            if expected_types.issubset(ProfilerTypeMapping.INT_TYPE_NAMES):
                column_types[column] = "int"
            elif expected_types.issubset(ProfilerTypeMapping.FLOAT_TYPE_NAMES):
                column_types[column] = "float"
            elif expected_types.issubset(ProfilerTypeMapping.STRING_TYPE_NAMES):
                column_types[column] = "string"
            elif expected_types.issubset(ProfilerTypeMapping.DATETIME_TYPE_NAMES):
                column_types[column] = "datetime"
            elif expected_types.issubset(ProfilerTypeMapping.BOOLEAN_TYPE_NAMES):
                column_types[column] = "bool"
            else:
                warnings.warn(
                    "The expected type list is not a subset of any of the profiler type sets: {:s}".format(
                        str(expected_types)
                    )
                )
                column_types[column] = "unknown"
        return column_types
