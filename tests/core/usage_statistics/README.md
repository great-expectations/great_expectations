# Usage Stats Testing

Usage stats testing is done in several different ways.

The main test locations are:
- tests/core/usage_statistics
- tests/integration/usage_statistics

We ensure:

- the right messages are generated
  - throughout the unit test suite there are checks that mock `UsageStatisticsHandler.emit()` and check that the correct messages were passed to it.
- messages are sent
  - tests/integration/usage_statistics/test_usage_stats_common_messages_are_sent_v3api.py tests a subset of common v3api events
  - tests/core/usage_statistics/test_usage_statistics_handler_methods.py e.g. those used in `UsageStatisticsHandler.emit()`: `build_init_payload()`, `build_envelope()` and `validate_message()`
- no warnings are generated when sending messages
  - TODO: since we don't raise errors in the usage stats machinery, these tests will check the logs for errors
- valid messages pass our validation schema
  - tests/core/usage_statistics/test_usage_stats_schema.py
  - tests/core/usage_statistics/test_schema_validation.py
- valid messages are accepted
  - tests/integration/usage_statistics/test_usage_statistics_messages.py
- invalid messages are blocked
  - tests/integration/usage_statistics/test_integration_usage_statistics.py
- opt-out is honored
  - tests/core/usage_statistics/test_usage_statistics.py
- Anonymizers work as expected
  - tests/core/usage_statistics/test_usage_statistics.py
  - tests/core/usage_statistics/test_anonymizer.py
  - tests/datasource/test_datasource_anonymizer.py
  - tests/execution_engine/test_execution_engine_anonymizer.py - TODO: implement
  - TODO: Other anonymizer tests
- Graceful failure with no internet
  - tests/integration/usage_statistics/test_integration_usage_statistics.py
