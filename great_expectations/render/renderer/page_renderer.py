from .renderer import Renderer
from .column_section_renderer import (
    DescriptiveColumnSectionRenderer,
    PrescriptiveColumnSectionRenderer,
)
from .other_section_renderer import (
    DescriptiveOverviewSectionRenderer,
)


class PrescriptivePageRenderer(Renderer):

    @classmethod
    def render(cls, expectations):
        # Group expectations by column
        columns = {}
        ordered_columns = None
        for expectation in expectations["expectations"]:
            if "column" in expectation["kwargs"]:
                column = expectation["kwargs"]["column"]
            else:
                column = "_nocolumn"
            if column not in columns:
                columns[column] = []
            columns[column].append(expectation)

            # if possible, get the order of columns from expect_table_columns_to_match_ordered_list
            if expectation["expectation_type"] == "expect_table_columns_to_match_ordered_list":
                exp_column_list = expectation["kwargs"]["column_list"]
                if exp_column_list and len(exp_column_list) > 0:
                    ordered_columns = exp_column_list

        # if no order of colums is expected, sort alphabetically
        if not ordered_columns:
            ordered_columns = sorted(list(columns.keys()))

        return {
            "renderer_type": "PrescriptivePageRenderer",
            "sections": [
                PrescriptiveColumnSectionRenderer.render(columns[column]) for column in ordered_columns
            ]
        }


class DescriptivePageRenderer(Renderer):

    @classmethod
    def render(cls, validation_results):
        # Group EVRs by column
        columns = {}
        for evr in validation_results["results"]:
            if "column" in evr["expectation_config"]["kwargs"]:
                column = evr["expectation_config"]["kwargs"]["column"]
            else:
                column = "Table-level Expectations"

            if column not in columns:
                columns[column] = []
            columns[column].append(evr)

        ordered_columns = Renderer._get_column_list_from_evrs(validation_results)
        column_types = DescriptiveOverviewSectionRenderer._get_column_types(validation_results)

        if "data_asset_name" in validation_results["meta"] and validation_results["meta"]["data_asset_name"]:
            data_asset_name = validation_results["meta"]["data_asset_name"].split(
                '/')[-1]
        else:
            data_asset_name = None

        return {
            "renderer_type": "DescriptivePageRenderer",
            "data_asset_name": data_asset_name,
            "sections":
                [
                    DescriptiveOverviewSectionRenderer.render(
                        validation_results,
                        section_name="Overview"
                    )
                ] +
                [
                    DescriptiveColumnSectionRenderer.render(
                        columns[column],
                        section_name=column,
                        column_type=column_types.get(column),
                    ) for column in ordered_columns
                ]
        }
