from great_expectations.data_asset import DataAsset
from great_expectations.dataset import SqlAlchemyDataset
from great_expectations.render.renderer.content_block.validation_results_table_content_block import (
    ValidationResultsTableContentBlockRenderer,
)
from great_expectations.render.types import RenderedStringTemplateContent


class SqlAlchemyDatasetWithCustomExpectations(SqlAlchemyDataset):
    @DataAsset.expectation(["table_name"])
    def expect_table_to_exist(self, table_name):
        table_found = self.engine.dialect.has_table(self.engine, table_name)
        return {
            "success": table_found,
            "result": {
                "observed_value": f"Table{' not ' if not table_found else ' '}found"
            },
        }


class ValidationResultsTableContentBlockRendererWithCustomExpectations(
    ValidationResultsTableContentBlockRenderer
):
    @classmethod
    def expect_table_to_exist(
        cls, expectation, styling=None, include_column_name: bool = True
    ):
        return [
            RenderedStringTemplateContent(
                content_block_type="string_template",
                string_template={
                    "template": "Table is required.",
                    "params": expectation.kwargs,
                    "styling": styling,
                },
            )
        ]
