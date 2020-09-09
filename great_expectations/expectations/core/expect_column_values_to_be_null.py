from great_expectations.expectations.expectation import ColumnMapDatasetExpectation


class ExpectColumnValuesToBeNull(ColumnMapDatasetExpectation):
    def format_map_output(
        self,
        result_format,
        success,
        element_count,
        nonnull_count,
        unexpected_count,
        unexpected_list,
        unexpected_index_list,
    ):
        return_obj = super().format_map_output(
            result_format,
            success,
            element_count,
            nonnull_count,
            unexpected_count,
            unexpected_list,
            unexpected_index_list,
        )
        del return_obj["result"]["unexpected_percent_nonmissing"]
        del return_obj["result"]["missing_count"]
        del return_obj["result"]["missing_percent"]
        try:
            del return_obj["result"]["partial_unexpected_counts"]
        except KeyError:
            pass
        try:
            del return_obj["result"]["partial_unexpected_list"]
        except KeyError:
            pass

        return return_obj
