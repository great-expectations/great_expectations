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
            # This is an expectation
            return ge_object["expectation_type"]

        elif "expectation_config" in ge_object:
            # This is a validation
            return ge_object["expectation_config"]["expectation_type"]

    @classmethod
    def _find_ge_object_type(cls, ge_object):
        """We want upstream systems to have flexibility in what they provide
        Options include an expectations config, a list of expectations, a single expectation,
        a validation report, a list of evrs, or a single evr"""

        if isinstance(ge_object, list):
            if "result" in ge_object[0] or "exception_info" in ge_object[0]:
                return "evr_list"
            elif "expectation_type" in ge_object[0]:
                return "expectation_list"
        else:
            if "results" in ge_object:
                return "validation_report"
            elif "expectations" in ge_object:
                return "expectations"
            elif "result" in ge_object or "exception_info" in ge_object:
                return "evr"
            elif "kwargs" in ge_object:
                return "expectation"

        # print(json.dumps(ge_object, indent=2))
        raise ValueError("Unrecognized great expectations object.")

    #TODO: When we implement a ValidationResultSuite class, this method will move there.
    @classmethod
    def _find_evr_by_type(cls, evrs, type_):
        for evr in evrs:
            if evr["expectation_config"]["expectation_type"] == type_:
                return evr

    #TODO: When we implement a ValidationResultSuite class, this method will move there.
    @classmethod
    def _find_all_evrs_by_type(cls, evrs, type_, column_=None):
        ret = []
        for evr in evrs:
            if evr["expectation_config"]["expectation_type"] == type_\
                    and (not column_ or column_ == evr["expectation_config"]["kwargs"].get("column")):
                ret.append(evr)

        return ret

    #TODO: When we implement a ValidationResultSuite class, this method will move there.
    @classmethod
    def _get_column_list_from_evrs(cls, evrs):
        """
        Get list of column names.

        If expect_table_columns_to_match_ordered_list EVR is present, use it as the list, including the order.

        Otherwise, get the list of all columns mentioned in the expectations and order it alphabetically.

        :param evrs:
        :return: list of columns with best effort sorting
        """
        evrs_ = evrs["results"] if "results" in evrs else evrs

        expect_table_columns_to_match_ordered_list_evr = cls._find_evr_by_type(evrs_, "expect_table_columns_to_match_ordered_list")
        if expect_table_columns_to_match_ordered_list_evr:
            ordered_columns = expect_table_columns_to_match_ordered_list_evr["result"]["observed_value"]
        else:
            # Group EVRs by column
            columns = list(set([evr["expectation_config"]["kwargs"]["column"] for evr in evrs_ if "column" in evr["expectation_config"]["kwargs"]]))

            ordered_columns = sorted(columns)

        return ordered_columns

    #TODO: When we implement a ValidationResultSuite class, this method will move there.
    @classmethod
    def _group_evrs_by_column(cls, validation_results):
        columns = {}
        for evr in validation_results["results"]:
            if "column" in evr["expectation_config"]["kwargs"]:
                column = evr["expectation_config"]["kwargs"]["column"]
            else:
                column = "Table-level Expectations"

            if column not in columns:
                columns[column] = []
            columns[column].append(evr)

        return columns

    #TODO: When we implement an ExpectationSuite class, this method will move there.
    @classmethod
    def _group_and_order_expectations_by_column(cls, expectations):
        # Group expectations by column
        columns = {}
        ordered_columns = None

        for expectation in expectations["expectations"]:
            if "column" in expectation["kwargs"]:
                column = expectation["kwargs"]["column"]
            else:
                column = "_nocolumn"
            if column not in columns:
                columns[column] = []
            columns[column].append(expectation)

            # if possible, get the order of columns from expect_table_columns_to_match_ordered_list
            if expectation["expectation_type"] == "expect_table_columns_to_match_ordered_list":
                exp_column_list = expectation["kwargs"]["column_list"]
                if exp_column_list and len(exp_column_list) > 0:
                    ordered_columns = exp_column_list

        # if no order of columns is expected, sort alphabetically
        if not ordered_columns:
            ordered_columns = sorted(list(columns.keys()))
        
        return columns, ordered_columns

    #TODO: When we implement an ExpectationSuite class, this method will move there.
    @classmethod
    def _get_expectation_suite_name(cls, expectations):
        if "expectation_suite_name" in expectations:
            expectation_suite_name = expectations["expectation_suite_name"]
        else:
            expectation_suite_name = None
                
        return expectation_suite_name
