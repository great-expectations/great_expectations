from great_expectations.expectations.registry import get_renderer_impl
from great_expectations.render import (
    LegacyDescriptiveRendererType,
    RenderedTableContent,
)
from great_expectations.render.renderer.content_block.content_block import (
    ContentBlockRenderer,
)


class ProfilingColumnPropertiesTableContentBlockRenderer(ContentBlockRenderer):
    expectation_renderers = {
        "expect_column_values_to_not_match_regex": [
            LegacyDescriptiveRendererType.COLUMN_PROPERTIES_TABLE_REGEX_COUNT_ROW
        ],
        "expect_column_unique_value_count_to_be_between": [
            LegacyDescriptiveRendererType.COLUMN_PROPERTIES_TABLE_DISTINCT_COUNT_ROW
        ],
        "expect_column_proportion_of_unique_values_to_be_between": [
            LegacyDescriptiveRendererType.COLUMN_PROPERTIES_TABLE_DISTINCT_PERCENT_ROW
        ],
        "expect_column_values_to_not_be_null": [
            LegacyDescriptiveRendererType.COLUMN_PROPERTIES_TABLE_MISSING_COUNT_ROW,
            LegacyDescriptiveRendererType.COLUMN_PROPERTIES_TABLE_MISSING_PERCENT_ROW,
        ],
    }

    @classmethod
    def render(cls, ge_object, header_row=None):
        """Each expectation method should return a list of rows"""
        if header_row is None:
            header_row = []

        table_rows = []

        if isinstance(ge_object, list):
            for sub_object in ge_object:
                expectation_type = cls._get_expectation_type(sub_object)
                if expectation_type in cls.expectation_renderers:
                    new_rows = [
                        get_renderer_impl(expectation_type, renderer_type)[1](
                            result=sub_object
                        )
                        for renderer_type in cls.expectation_renderers.get(
                            expectation_type
                        )
                    ]
                    table_rows.extend(new_rows)
        else:
            expectation_type = cls._get_expectation_type(ge_object)
            if expectation_type in cls.expectation_renderers:
                new_rows = [
                    get_renderer_impl(expectation_type, renderer_type)[1](
                        result=ge_object
                    )
                    for renderer_type in cls.expectation_renderers.get(expectation_type)
                ]
                table_rows.extend(new_rows)

        return RenderedTableContent(
            **{
                "content_block_type": "table",
                "header_row": header_row,
                "table": table_rows,
            }
        )
