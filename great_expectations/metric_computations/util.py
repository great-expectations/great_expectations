import inflect
import inflection

# TODO: <Alex>ALEX</Alex>
# from mercury.domain import time
# TODO: <Alex>ALEX</Alex>

pluralize = inflect.engine().plural

singularize = inflect.engine().singular_noun

underscore = inflection.underscore

camelize = inflection.camelize


# TODO: <Alex>ALEX</Alex>
# def get_onupdate_timestamp_callback(attribute_name):
#     def generate_conditional_timestamp(context):
#         if context.get_current_parameters().get(attribute_name) is True:
#             return time.now()
#
#     return generate_conditional_timestamp
# TODO: <Alex>ALEX</Alex>


# TODO: <Alex>ALEX</Alex>
# def is_property(model, val):
#     """
#     Checks SqlAlchemy model for attribute val and returns
#     True if it's a property, False if it's another type of field.
#     """
#     field = getattr(model, val)
#     return isinstance(field, property)
# TODO: <Alex>ALEX</Alex>
