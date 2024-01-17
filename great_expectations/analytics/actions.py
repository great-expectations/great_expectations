from __future__ import annotations

from great_expectations.analytics.base_event import Action

DATA_CONTEXT_INITIALIZED = Action("data_context.initialized")
EXPECTATION_CREATED = Action("expectation.created")
EXPECTATION_UPDATED = Action("expectation.updated")
EXPECTATION_DELETED = Action("expectation.deleted")
EXPECTATION_SUITE_CREATED = Action("expectation_suite.created")
EXPECTATION_SUITE_UPDATED = Action("expectation_suite.updated")
EXPECTATION_SUITE_DELETED = Action("expectation_suite.deleted")
