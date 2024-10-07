from __future__ import annotations

from great_expectations.analytics.base_event import Action

DOMAIN_OBJECT_ALL_DESERIALIZE_ERROR: Action = Action("domain_object.all_deserialize_error")
DATA_CONTEXT_INITIALIZED: Action = Action("data_context.initialized")
CHECKPOINT_CREATED: Action = Action("checkpoint.created")
CHECKPOINT_DELETED: Action = Action("checkpoint.deleted")
CHECKPOINT_RAN: Action = Action("checkpoint.ran")
EXPECTATION_SUITE_CREATED: Action = Action("expectation_suite.created")
EXPECTATION_SUITE_DELETED: Action = Action("expectation_suite.deleted")
EXPECTATION_SUITE_EXPECTATION_CREATED: Action = Action("expectation_suite.expectation_created")
EXPECTATION_SUITE_EXPECTATION_DELETED: Action = Action("expectation_suite.expectation_deleted")
EXPECTATION_SUITE_EXPECTATION_UPDATED: Action = Action("expectation_suite.expectation_updated")
VALIDATION_DEFINITION_CREATED: Action = Action("validation_definition.created")
VALIDATION_DEFINITION_DELETED: Action = Action("validation_definition.deleted")
