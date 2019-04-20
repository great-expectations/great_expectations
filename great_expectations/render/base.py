class Renderer(object):
    def __init__(self, *args, **kwargs):
        """
        Note: This shouldn't get used much other than input validation. We expect most renderers to be stateless, single-use classes.
        """
        self.validate_input(*args, **kwargs)

    def validate_input(self, *args, **kwargs):
        raise NotImplementedError

    def render(self):
        pass

class SingleExpectationRenderer(Renderer):
    def __init__(self, expectation):
        self.expectation = expectation

    def validate_input(self, expectation):
        return True

    def render(self):
        raise NotImplementedError

