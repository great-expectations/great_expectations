from .base import Renderer

class SectionRenderer(Renderer):
    def __init__(self, expectations, inspectable):
        self.expectations = expectations

    def _validate_input(self, expectations):
        # raise NotImplementedError
        #!!! Need to fix this
        return True

    def _get_template(self):
        raise NotImplementedError

    def render(self):
        raise NotImplementedError


class EvrSectionRenderer(SectionRenderer):
    def __init__(self, column_name, evrs):
        self.column_name = column_name
        self.evrs = evrs

    def render(self):
        description = {
            "content_block_type" : "text",
            "content" : []
        }
#         bullet_list = {
#             "content_block_type" : "bullet_list",
#             "content" : []
#         }
#         if random.random() > .5:
#             graph = {
#                 "content_block_type" : "graph",
#                 "content" : []
#             }
#         else:
#             graph = {}

#         graph2 = {
#             "content_block_type" : "graph",
#             "content" : []
#         }
#         table = {
#             "content_block_type" : "table",
#             "content" : []
#         }
#         example_list = {
#             "content_block_type" : "example_list",
#             "content" : []
#         }
#         more_description = {
#             "content_block_type" : "text",
#             "content" : []
#         }

#         for expectation in expectations_list:
#             try:
#                 expectation_renderer = SingleExpectationRenderer(
#                     expectation=expectation,
#                 )
#                 # print(expectation)
#                 bullet_point = expectation_renderer.render()
#                 assert bullet_point != None
#                 bullet_list["content"].append(bullet_point)
#             except Exception as e:
#                 bullet_list["content"].append("""
# <div class="alert alert-danger" role="alert">
#   Failed to render Expectation:<br/><pre>"""+json.dumps(expectation, indent=2)+"""</pre>
#   <p>"""+str(e)+"""
# </div>
#                 """)

        section = {
            "section_name" : self.column_name,
            "content_blocks" : [
                # graph,
                # # graph2,
                description,
                # table,
                # bullet_list,
                # example_list,
                # more_description,
            ]
        }

        return section
