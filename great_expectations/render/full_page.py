from .base import Renderer
from .single_expectation import SingleExpectationRenderer

#FullPageExpectationHtmlRenderer
#FullPageValidationResultHtmlRenderer

class FullPageHtmlRenderer(Renderer):
    def __init__(self, expectations, inspectable):
        self.expectations = expectations

    def validate_input(self, expectations):
        return True

    def render(self):
        results = []
        for expectation in self.expectations:
            expectation_renderer = SingleExpectationRenderer(
                expectation=expectation,
            )
            results.append(expectation_renderer.render())
        
        print(results)
        return results