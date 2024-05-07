from __future__ import annotations

from great_expectations.analytics.base_event import Action

DATA_CONTEXT_INITIALIZED = Action("data_context.initialized")
CHECKPOINT_CREATED = Action("checkpoint.created")
CHECKPOINT_DELETED = Action("checkpoint.deleted")
EXPECTATION_SUITE_CREATED = Action("expectation_suite.created")
EXPECTATION_SUITE_DELETED = Action("expectation_suite.deleted")
EXPECTATION_SUITE_EXPECTATION_CREATED = Action("expectation_suite.expectation_created")
EXPECTATION_SUITE_EXPECTATION_DELETED = Action("expectation_suite.expectation_deleted")
EXPECTATION_SUITE_EXPECTATION_UPDATED = Action("expectation_suite.expectation_updated")
VALIDATION_DEFINITION_CREATED = Action("validation_definition.created")
VALIDATION_DEFINITION_DELETED = Action("validation_definition.deleted")
