"""Example usage stats events for "data_context.run_validation_operator" events.

This file contains example events primarily for usage stats schema validation testing.
By splitting up these events into a separate file, the hope is we can organize them better here and separate
them from tests so that test files are less verbose.
"""
from typing import List

data_context_run_validation_operator_events: List[dict] = [
    {
        "event": "data_context.run_validation_operator",
        "event_payload": {
            "anonymized_operator_name": "dbb859464809a03647feb14a514f12b8",
            "anonymized_batches": [
                {
                    "anonymized_batch_kwarg_keys": [
                        "path",
                        "datasource",
                        "data_asset_name",
                    ],
                    "anonymized_expectation_suite_name": "dbb859464809a03647feb14a514f12b8",
                    "anonymized_datasource_name": "a41caeac7edb993cfbe55746e6a328b5",
                }
            ],
        },
        "success": True,
        "version": "1.0.0",
        "event_time": "2020-08-03T23:36:26.422Z",
        "data_context_id": "00000000-0000-0000-0000-000000000002",
        "data_context_instance_id": "10000000-0000-0000-0000-000000000002",
        "ge_version": "0.10.0.manual_testing",
    }
]
