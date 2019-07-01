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
        """We want upstream systems to have flexibility in what they provide
        Options include an expectations config, a list of expectations, a single expectation,
        a validation report, a list of evrs, or a single evr"""

        if isinstance(ge_object, list):
            if "result" in ge_object[0]:
                return "evr_list"
            elif "expectation_type" in ge_object[0]:
                return "expectation_list"
        else:
            if "results" in ge_object:
                return "validation_report"
            elif "expectations" in ge_object:
                return "expectations"
            elif "result" in ge_object:
                return "evr"
            elif "kwargs" in ge_object:
                return "expectation"
        
        raise ValueError("Unrecognized great expectations object.")

    @classmethod
    def _find_evr_by_type(cls, evrs, type_):
        for evr in evrs:
            if evr["expectation_config"]["expectation_type"] == type_:
                return evr


    @classmethod
    def _get_column_list_from_evrs(cls, evrs):
        # Group EVRs by column
        columns = {}
        for evr in evrs["results"]:
            if "column" in evr["expectation_config"]["kwargs"]:
                column = evr["expectation_config"]["kwargs"]["column"]
            else:
                column = "_nocolumn"

            if column not in columns:
                columns[column] = []
            columns[column].append(evr)

        # TODO: in general, there should be a mechanism for imposing order here.
        ordered_columns = list(columns.keys())

        return  ordered_columns

