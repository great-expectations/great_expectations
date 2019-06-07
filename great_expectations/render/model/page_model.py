from .model import Model
from .column_section_model import DescriptiveColumnSectionModel, PrescriptiveColumnSectionModel


class PrescriptivePageModel(Model):
    
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
            "model_type": "PrescriptivePageModel",
            "sections": [
                PrescriptiveColumnSectionModel.render(columns[column]) for column in ordered_columns
            ]
        }


class DescriptivePageModel(Model):
    
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
            "model_type": "DescriptivePageModel",
            "sections": [
                DescriptiveColumnSectionModel.render(columns[column]) for column in ordered_columns
            ]
        }