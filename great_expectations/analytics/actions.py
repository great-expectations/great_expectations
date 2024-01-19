from __future__ import annotations

from great_expectations.analytics.base_event import Action

DATA_CONTEXT_INITIALIZED = Action("data_context.initialized")
EXPECTATION_SUITE_EXPECTATION_CREATED = Action("expectation_suite.expectation_created")
EXPECTATION_SUITE_EXPECTATION_UPDATED = Action("expectation.expectation_updated")
EXPECTATION_SUITE_EXPECTATION_DELETED = Action("expectation.expectation_deleted")
EXPECTATION_SUITE_CREATED = Action("expectation_suite.created")
EXPECTATION_SUITE_UPDATED = Action("expectation_suite.updated")
EXPECTATION_SUITE_DELETED = Action("expectation_suite.deleted")
