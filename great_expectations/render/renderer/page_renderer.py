from .renderer import Renderer
from .column_section_renderer import (
    DescriptiveColumnSectionRenderer,
    PrescriptiveColumnSectionRenderer,
)
from .fancy_column_section_renderer import (
    FancyDescriptiveColumnSectionRenderer,
)
from .other_section_renderer import (
    DescriptiveOverviewSectionRenderer,
)


class PrescriptivePageRenderer(Renderer):

    @classmethod
    def render(cls, expectations):
        # Group expectations by column
        columns = {}
        for expectation in expectations["expectations"]:
            if "column" in expectation["kwargs"]:
                column = expectation["kwargs"]["column"]
            else:
                column = "_nocolumn"
            if column not in columns:
                columns[column] = []
            columns[column].append(expectation)

        # TODO: in general, there should be a mechanism for imposing order here.
        ordered_columns = list(columns.keys())

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

        # TODO: in general, there should be a mechanism for imposing order here.
        ordered_columns = list(columns.keys())

        column_types = DescriptiveOverviewSectionRenderer._get_column_types(validation_results)

        # FIXME: This is a hack to limit output on one training file
        # if "Reporting Area" in ordered_columns:
        #     ordered_columns = ["Reporting Area"]

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
                    FancyDescriptiveColumnSectionRenderer.render(
                        columns[column],
                        section_name=column,
                        column_type=column_types.get(column),
                    ) for column in ordered_columns
                ]
        }
