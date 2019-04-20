from .base import Renderer

class FullPageHtmlRenderer(Renderer):
    def __init__(self, expectations, inspectable):
        self.expectations = expectations

    def validate_input(self, expectations):
        return True
