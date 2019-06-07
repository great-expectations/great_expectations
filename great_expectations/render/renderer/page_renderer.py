from .renderer import Renderer
from .column_section_renderer import DescriptiveColumnSectionRenderer, PrescriptiveColumnSectionRenderer


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
                column = "_nocolumn"
        
            if column not in columns:
                columns[column] = []
            columns[column].append(evr)
    
        # TODO: in general, there should be a mechanism for imposing order here.
        ordered_columns = list(columns.keys())

        return {
            "renderer_type": "DescriptivePageRenderer",
            "sections": [
                DescriptiveColumnSectionRenderer.render(columns[column]) for column in ordered_columns
            ]
        }