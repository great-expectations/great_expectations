import json
import base64
import hashlib


class Renderer(object):
    @classmethod
    def render(cls, ge_object):
        return ge_object

    @classmethod
    def _id_from_configuration(cls, expectation_type, expectation_kwargs, data_asset_name=None):
        urn_hash = hashlib.md5()
        urn_hash.update(expectation_type.encode('utf-8'))
        urn_hash.update(json.dumps(expectation_kwargs).encode('utf-8'))
        urn_hash.update(json.dumps(data_asset_name).encode('utf-8')) # Dump first even though this is a string in case it is null;
        return base64.b64encode(urn_hash.digest()).decode('utf-8')

    @classmethod
    def _get_expectation_type(cls, ge_object):
        if "expectation_type" in ge_object:
            # This is an expectation (prescriptive)
            return ge_object["expectation_type"]

        elif "expectation_config" in ge_object:
            # This is a validation (descriptive)
            return ge_object["expectation_config"]["expectation_type"]

    @classmethod
    def _find_ge_object_type(cls, ge_object):
        # Decide whether this is a Validation Report or an Expectation Configuration
        if "results" in ge_object:
            objects_type = "validation"    
            try:
                data_asset_name = ge_object["meta"]["data_asset_name"]
            except KeyError:
                data_asset_name = None
        elif "expectations" in ge_object:
            objects_type = "configuration"
            try:
                data_asset_name = ge_object["data_asset_name"]
            except KeyError:
                data_asset_name = None
        else:
            raise ValueError("Unrecognized great expectations object. Provide either an expectations config or a validation report.")

        return objects_type, data_asset_name

    @classmethod
    def _find_evr_by_type(cls, evrs, type_):
        for evr in evrs:
            if evr["expectation_config"]["expectation_type"] == type_:
                return evr
