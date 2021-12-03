import copy

from great_expectations.marshmallow__shade import Schema, post_dump


class GESchema(Schema):

    REMOVE_KEYS_IF_NONE = ["ge_cloud_id"]

    @post_dump
    def clean_null_attrs(self, data: dict, **kwargs):
        data = copy.deepcopy(data)
        for key in GESchema.REMOVE_KEYS_IF_NONE:
            if key in data and data[key] is None:
                data.pop(key)
        return data
