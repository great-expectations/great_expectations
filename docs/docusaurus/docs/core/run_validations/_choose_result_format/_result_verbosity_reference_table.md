The following table lists the fields that can be found in the `result` dictionary of a Validation Result and the Result Format verbosity levels that return that field.

| Fields within `result`                |BOOLEAN_ONLY    |BASIC           |SUMMARY         |COMPLETE        |
----------------------------------------|----------------|----------------|----------------|-----------------
|    element_count                      |no              |yes             |yes             |yes             |
|    missing_count                      |no              |yes             |yes             |yes             |
|    missing_percent                    |no              |yes             |yes             |yes             |
|    unexpected_count                   |no              |yes             |yes             |yes             |
|    unexpected_percent                 |no              |yes             |yes             |yes             |
|    unexpected_percent_nonmissing      |no              |yes             |yes             |yes             |
|    observed_value                     |no              |yes             |yes             |yes             |
|    partial_unexpected_list            |no              |yes             |yes             |yes             |
|    partial_unexpected_index_list      |no              |no              |yes             |yes             |
|    partial_unexpected_counts          |no              |no              |yes             |yes             |
|    unexpected_index_list              |no              |no              |no              |yes             |
|    unexpected_index_query             |no              |no              |no              |yes             |
|    unexpected_list                    |no              |no              |no              |yes             |
