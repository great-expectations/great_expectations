from .content_block import ContentBlock

class TableContentBlock(ContentBlock):

    @classmethod
    def render(cls, ge_object):
        # First, we add the generic ones...
        if isinstance(ge_object, list):
            table_entries = {}
            for sub_object in ge_object:
                table_entries = cls._add_common_table_entries(sub_object, table_entries)
                expectation_type = cls._get_expectation_type(sub_object)
                extra_rows_fn = getattr(cls, expectation_type, None)
                if extra_rows_fn is not None:
                    key, val = extra_rows_fn(sub_object)
                    table_entries[key] = val
        else:
            table_entries = cls._add_common_table_entries(ge_object)
            expectation_type = cls._get_expectation_type(ge_object)
            extra_rows_fn = getattr(cls, expectation_type, None)
            if extra_rows_fn is not None:
                    key, val = extra_rows_fn(ge_object)
                    table_entries[key] = val
        return {
            "content_block_type": "table",
            "table_rows": [[key, val] for key, val in table_entries.items()]
        }

    @classmethod
    def expect_column_values_to_not_match_regex(cls, ge_object):
        regex = ge_object["expectation_config"]["kwargs"]["regex"]
        unexpected_count = ge_object["result"]["unexpected_count"]
        if regex == '^\\s+|\\s+$':
            return ("Leading or trailing whitespace (n)", unexpected_count)
        else:
            return ("Regex: %s" % regex, unexpected_count)

    @classmethod
    def expect_column_unique_value_count_to_be_between(cls, ge_object):
        observed_value = ge_object["result"]["observed_value"]
        return ("Distinct count", observed_value)

    @classmethod
    def expect_column_proportion_of_unique_values_to_be_between(cls, ge_object):
        observed_value = ge_object["result"]["observed_value"]
        return ("Unique (%)", "%.1f%%" % (100*observed_value))

    @classmethod
    def expect_column_max_to_be_between(cls, ge_object):
        observed_value = ge_object["result"]["observed_value"]
        return ("Max", observed_value)

    @classmethod
    def expect_column_mean_to_be_between(cls, ge_object):
        observed_value = ge_object["result"]["observed_value"]
        return ("Mean", observed_value)

    @classmethod
    def expect_column_values_to_be_in_set(cls, ge_object):
        return ("test row", "garbage data")

    @classmethod
    def _add_common_table_entries(cls, ge_object, table_entries={}):            
        if "result" in ge_object:
            if "unexpected_percent" in ge_object["result"]:
                unexpected_percent = ge_object["result"]["unexpected_percent"]
                table_entries["Unexpected (%)"] = "%.1f%%" % unexpected_percent

            if "unexpected_count" in ge_object["result"]:
                unexpected_count = ge_object["result"]["unexpected_count"]
                table_entries["Unexpected (n)"] = str(unexpected_count)

            if "missing_percent" in ge_object["result"]:
                missing_percent = ge_object["result"]["missing_percent"]
                table_entries["Missing (%)"] = "%.1f%%" % missing_percent

            if "missing_count" in ge_object["result"]:
                missing_count = ge_object["result"]["missing_count"]
                table_entries["Missing (n)"] = str(missing_count)

        return table_entries