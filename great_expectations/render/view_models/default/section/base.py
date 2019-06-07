### PORTED


from ....base import Renderer


class SectionRenderer(Renderer):
    @classmethod
    def _validate_input(cls, expectations):
        # raise NotImplementedError
        #!!! Need to fix this
        return True

    @classmethod
    def _get_template(cls):
        raise NotImplementedError

    @classmethod
    def render(cls):
        raise NotImplementedError
