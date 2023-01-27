"""Example usage stats events for the cloud_migrate event.

This file contains example events primarily for usage stats schema validation testing.
"""
from great_expectations.core.usage_statistics.events import UsageStatsEvents

cloud_migrate: dict = {
    "event": UsageStatsEvents.CLOUD_MIGRATE,
    "event_payload": {"organization_id": "some_organization_id_probably_uuid"},
    "success": True,
    "version": "1.0.0",
    "event_time": "2022-09-26T14:22:53.921Z",
    "data_context_id": "bda195d7-5d83-410d-905c-d6a824debd78",
    "data_context_instance_id": "03b419d8-f8d1-4c2d-8944-f60e58e12d4a",
    "ge_version": "0.15.25",
}
