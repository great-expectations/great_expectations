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
        full_data_asset_identifier = expectations.get("data_asset_name") or ""
        short_data_asset_name = full_data_asset_identifier.split('/')[-1]
        data_asset_type = expectations.get("data_asset_type")
        expectation_suite_name = expectations.get("expectation_suite_name")
        ge_version = expectations["meta"]["great_expectations.__version__"]
        
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

        sections = [
            {
                "section_name": "Overview",
                "content_blocks": [
                    {
                        "content_block_type": "header",
                        "header": "Expectation Suite Overview",
                        "styling": {
                            "classes": ["col-12"],
                            "header": {
                                "classes": ["alert", "alert-secondary"]
                            }
                        }
                    },
                    {
                        "content_block_type": "table",
                        "header": "Info",
                        "table_rows": [
                            ["Full Data Asset Identifier", full_data_asset_identifier],
                            ["Data Asset Type", data_asset_type],
                            ["Expectation Suite Name", expectation_suite_name],
                            ["Great Expectations Version", ge_version]
                        ],
                        "styling": {
                            "classes": ["col-12", "table-responsive"],
                            "styles": {
                                "margin-top": "20px"
                            },
                            "body": {
                                "classes": ["table", "table-sm"]
                            }
                        },
                    }
                ]
            }
        ]

        sections += [
                PrescriptiveColumnSectionRenderer.render(columns[column]) for column in ordered_columns
            ]

        return {
            "renderer_type": "PrescriptivePageRenderer",
            "data_asset_name": short_data_asset_name,
            "full_data_asset_identifier": full_data_asset_identifier,
            "page_title": expectation_suite_name,
            "utm_medium": "prescriptive-expectation-suite-page",
            "sections": sections
        }


class DescriptivePageRenderer(Renderer):

    @classmethod
    def render(cls, validation_results):
        run_id = validation_results['meta']['run_id']
        full_data_asset_identifier = validation_results['meta']['data_asset_name'] or ""
        expectation_suite_name = validation_results['meta']['expectation_suite_name']
        short_data_asset_name = full_data_asset_identifier.split('/')[-1]

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
            data_asset_name = short_data_asset_name
        else:
            data_asset_name = None

        return {
            "renderer_type": "DescriptivePageRenderer",
            "data_asset_name": data_asset_name,
            "full_data_asset_identifier": full_data_asset_identifier,
            "page_title": run_id + "-" + expectation_suite_name,
            "utm_medium": "descriptive-validation-page",
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
