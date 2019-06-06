from .model import Model
from .column_section_model import DescriptiveColumnSectionModel

class DescriptivePageModel(Model):
    
    @classmethod
    def render(cls, validation_results):
        # Group EVRs by column
        evrs = {}
        for evr in validation_results["results"]:
            try:
                column = evr["expectation_config"]["kwargs"]["column"]
                if column not in evrs:
                    evrs[column] = []
                evrs[column].append(evr)
            except KeyError:
                column = "_nocolumn"
                if column not in evrs:
                    evrs[column] = []
                evrs[column].append(evr)

        return {
            "model_type": "DescriptivePageModel",
            "sections": [
                DescriptiveColumnSectionModel.render(evrs[column]) for column in evrs.keys()
            ]
        }