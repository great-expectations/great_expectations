from great_expectations.agent.models import Event, RunOnboardingDataAssistantEvent
from great_expectations.data_context import CloudDataContext
from great_expectations.exceptions import StoreBackendError


class EventHandler:
    """
    Core business logic mapping events to actions.
    """

    def __init__(self, context: CloudDataContext) -> None:
        self._context = context

    def handle_event(self, event: Event, correlation_id: str) -> None:
        """Pass event to the correct handler."""
        if isinstance(event, RunOnboardingDataAssistantEvent):
            self._handle_run_data_assistant(event, correlation_id)
        else:
            pass

    def _handle_run_data_assistant(
        self, event: RunOnboardingDataAssistantEvent, correlation_id: str
    ) -> None:
        """Action that occurs when a RunOnboardingDataAssistantEvent is received."""

        # todo: this action should create a checkpoint as well as a suite, but
        #       that workflow is still in progress.
        print("Starting Onboarding Data Assistant")
        suite_name = f"{event.data_asset_name} onboarding assistant suite"
        # checkpoint_name = f"{event.data_asset_name} onboarding assistant checkpoint"

        # ensure resources we create don't already exist
        try:
            self._context.get_expectation_suite(expectation_suite_name=suite_name)
            raise ValueError(
                f"Onboarding Assistant Expectation Suite `{suite_name}` already exists. "
                + "Please rename or delete suite and try again"
            )
        except StoreBackendError:
            # resource is unique
            pass

        # try:
        #     self._context.get_checkpoint(name=checkpoint_name)
        #     raise ValueError(
        #         f"Onboarding Assistant Checkpoint `{checkpoint_name}` already exists. "
        #         + "Please rename or delete Checkpoint and try again"
        #     )
        # except StoreBackendError:
        #     # resource is unique
        #     pass

        datasource = self._context.get_datasource(datasource_name=event.datasource_name)
        asset = datasource.get_asset(asset_name=event.data_asset_name)
        batch_request = asset.build_batch_request()

        data_assistant_result = self._context.assistants.onboarding.run(
            batch_request=batch_request,
        )
        expectation_suite = data_assistant_result.get_expectation_suite(
            expectation_suite_name=suite_name
        )
        self._context.add_or_update_expectation_suite(
            expectation_suite=expectation_suite
        )
        print(f"Onboarding Data Assistant created the following resources:")
        print(f"    Expectation Suite: {suite_name}")
        # print(f"    Checkpoint: {checkpoint_name}")
