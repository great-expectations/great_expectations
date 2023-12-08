from __future__ import annotations

import logging
import warnings
from collections import OrderedDict
from typing import TYPE_CHECKING, Optional, Union

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.async_executor import AsyncExecutor
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.data_asset.util import parse_result_format
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.types.refs import GXCloudResourceRef
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    GXCloudIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.validation_operators.types.validation_operator_result import (
    ValidationOperatorResult,
)

if TYPE_CHECKING:
    from great_expectations.core.batch import Batch
    from great_expectations.data_asset import DataAsset

logger = logging.getLogger(__name__)


class ValidationOperator:
    """
    The base class of all validation operators.

    It defines the signature of the public run method. This method and the validation_operator_config property are the
    only contract re operators' API. Everything else is up to the implementors
    of validation operator classes that will be the descendants of this base class.
    """

    def __init__(self) -> None:
        self._validation_operator_config = None

    @property
    def validation_operator_config(self) -> None:
        """
        This method builds the config dict of a particular validation operator. The "kwargs" key is what really
        distinguishes different validation operators.

        e.g.:
        {
            "class_name": "ActionListValidationOperator",
            "module_name": "great_expectations.validation_operators",
            "name": self.name,
            "kwargs": {
                "action_list": self.action_list
            },
        }

        {
            "class_name": "WarningAndFailureExpectationSuitesValidationOperator",
            "module_name": "great_expectations.validation_operators",
            "name": self.name,
            "kwargs": {
                "action_list": self.action_list,
                "base_expectation_suite_name": self.base_expectation_suite_name,
                "expectation_suite_name_suffixes": self.expectation_suite_name_suffixes,
                "stop_on_first_error": self.stop_on_first_error,
                "slack_webhook": self.slack_webhook,
                "notify_on": self.notify_on,
            },
        }
        """

        raise NotImplementedError

    def run(  # noqa: PLR0913
        self,
        assets_to_validate,
        run_id=None,
        evaluation_parameters=None,
        run_name=None,
        run_time=None,
    ) -> None:
        raise NotImplementedError


class ActionListValidationOperator(ValidationOperator):
    """

    ActionListValidationOperator validates each batch in its ``run`` method's ``assets_to_validate`` argument against the Expectation Suite included within that batch.

    Then it invokes a list of configured actions on every validation result.

    Each action in the list must be an instance of :py:class:`ValidationAction<great_expectations.validation_operators.actions.ValidationAction>`
    class (or its descendants). See the actions included in Great Expectations and how to configure them :py:mod:`here<great_expectations.validation_operators.actions>`. You can also implement your own actions by extending the base class.

    The init command includes this operator in the default configuration file.


    **Configuration**

    An instance of ActionListValidationOperator is included in the default configuration file ``great_expectations.yml`` that ``great_expectations init`` command creates.

    .. code-block:: yaml

      perform_action_list_operator:  # this is the name you will use when you invoke the operator
        class_name: ActionListValidationOperator

        # the operator will call the following actions on each validation result
        # you can remove or add actions to this list. See the details in the actions
        # reference
        action_list:
          - name: store_validation_result
            action:
              class_name: StoreValidationResultAction
              target_store_name: validations_store
          - name: send_slack_notification_on_validation_result
            action:
              class_name: SlackNotificationAction
              # put the actual webhook URL in the uncommitted/config_variables.yml file
              slack_webhook: ${validation_notification_slack_webhook}
              notify_on: all # possible values: "all", "failure", "success"
              notify_with: optional list of DataDocs sites (ie local_site or gcs_site") to include in Slack notification. Will default to including all configured DataDocs sites.
              renderer:
                module_name: great_expectations.render.renderer.slack_renderer
                class_name: SlackRenderer
          - name: update_data_docs
            action:
              class_name: UpdateDataDocsAction


    **Invocation**

    This is an example of invoking an instance of a Validation Operator from Python:

    .. code-block:: python

        results = context.run_validation_operator(
            assets_to_validate=[batch0, batch1, ...],
            run_id=RunIdentifier(**{
              "run_name": "some_string_that_uniquely_identifies_this_run",
              "run_time": "2020-04-29T10:46:03.197008"  # optional run timestamp, defaults to current UTC datetime
            }),  # you may also pass in a dictionary with run_name and run_time keys
            validation_operator_name="operator_instance_name",
        )

    * ``assets_to_validate`` - an iterable that specifies the data assets that the operator will validate. The members of the list can be either batches or triples that will allow the operator to fetch the batch: (data_asset_name, expectation_suite_name, batch_kwargs) using this method: :py:meth:`~great_expectations.data_context.BaseDataContext.get_batch`
    * ``run_id`` - pipeline run id of type RunIdentifier, consisting of a ``run_time`` (always assumed to be UTC time) and ``run_name`` string that is meaningful to you and will help you refer to the result of this operation later
    * ``validation_operator_name`` you can instances of a class that implements a Validation Operator

    The ``run`` method returns a ValidationOperatorResult object:

    ::

        {
            "run_id": {"run_time": "20200527T041833.074212Z", "run_name": "my_run_name"},
            "success": True,
            "evaluation_parameters": None,
            "validation_operator_config": {
                "class_name": "ActionListValidationOperator",
                "module_name": "great_expectations.validation_operators",
                "name": "action_list_operator",
                "kwargs": {
                    "action_list": [
                        {
                            "name": "store_validation_result",
                            "action": {"class_name": "StoreValidationResultAction"},
                        },
                        {
                            "name": "store_evaluation_params",
                            "action": {"class_name": "StoreEvaluationParametersAction"},
                        },
                        {
                            "name": "update_data_docs",
                            "action": {"class_name": "UpdateDataDocsAction"},
                        },
                    ]
                },
            },
            "run_results": {
                ValidationResultIdentifier: {
                    "validation_result": ExpectationSuiteValidationResult object,
                    "actions_results": {
                        "store_validation_result": {},
                        "store_evaluation_params": {},
                        "update_data_docs": {},
                    },
                }
            },
        }
    """

    def __init__(
        self,
        data_context,
        action_list,
        name,
        result_format={"result_format": "SUMMARY"},  # noqa: B006 # mutable default
    ) -> None:
        super().__init__()
        self.data_context = data_context
        self.name = name

        result_format = parse_result_format(result_format)
        assert result_format["result_format"] in [
            "BOOLEAN_ONLY",
            "BASIC",
            "SUMMARY",
            "COMPLETE",
        ]
        self.result_format = result_format

        self.action_list = action_list
        self.actions = OrderedDict()
        # For a great expectations cloud context it's important that we store the validation result before we send
        # notifications. That's because we want to provide a link to the validation result and the validation result
        # page won't get created until we run the store action.
        store_action_detected = False
        notify_before_store: Optional[str] = None
        for action_config in action_list:
            assert isinstance(action_config, dict)
            # NOTE: Eugene: 2019-09-23: need a better way to validate an action config:
            if not set(action_config.keys()) == {"name", "action"}:
                raise KeyError(
                    'Action config keys must be ("name", "action"). Instead got {}'.format(
                        action_config.keys()
                    )
                )

            if "class_name" in action_config["action"]:
                if (
                    action_config["action"]["class_name"]
                    == "StoreValidationResultAction"
                ):
                    store_action_detected = True
                elif (
                    action_config["action"]["class_name"].endswith("NotificationAction")
                    and not store_action_detected
                ):
                    # We currently only support SlackNotifications but setting this for any notification.
                    notify_before_store = action_config["action"]["class_name"]

            config = action_config["action"]
            module_name = "great_expectations.validation_operators"
            new_action = instantiate_class_from_config(
                config=config,
                runtime_environment={"data_context": self.data_context},
                config_defaults={"module_name": module_name},
            )
            if not new_action:
                raise gx_exceptions.ClassInstantiationError(
                    module_name=module_name,
                    package_name=None,
                    class_name=config["class_name"],
                )
            self.actions[action_config["name"]] = new_action
        if notify_before_store and self._using_cloud_context:
            warnings.warn(
                f"The checkpoints action_list configuration has a notification, {notify_before_store}"
                "configured without a StoreValidationResultAction configured. This means the notification can't"
                "provide a link the validation result. Please move all notification actions after "
                "StoreValidationResultAction in your configuration."
            )

    @property
    def _using_cloud_context(self) -> bool:
        # Chetan - 20221216 - This is a temporary property to encapsulate any Cloud leakage
        # Upon refactoring this class to decouple Cloud-specific branches, this should be removed
        from great_expectations.data_context.data_context.cloud_data_context import (
            CloudDataContext,
        )

        return isinstance(self.data_context, CloudDataContext)

    @property
    def validation_operator_config(self) -> dict:
        if self._validation_operator_config is None:
            self._validation_operator_config = {
                "class_name": "ActionListValidationOperator",
                "module_name": "great_expectations.validation_operators",
                "name": self.name,
                "kwargs": {
                    "action_list": self.action_list,
                    "result_format": self.result_format,
                },
            }
        return self._validation_operator_config

    def _build_batch_from_item(self, item):
        """Internal helper method to take an asset to validate, which can be either:
          (1) a DataAsset; or
          (2) a tuple of data_asset_name, expectation_suite_name, and batch_kwargs (suitable for passing to get_batch)

        Args:
            item: The item to convert to a batch (see above)

        Returns:
            A batch of data

        """
        # if not isinstance(item, (DataAsset, Validator)):
        if isinstance(item, tuple):
            if not (
                len(item) == 2  # noqa: PLR2004
                and isinstance(item[0], dict)
                and isinstance(item[1], str)
            ):
                raise ValueError("Unable to build batch from item.")
            batch = self.data_context._get_batch_v2(
                batch_kwargs=item[0], expectation_suite_name=item[1]
            )
        else:
            batch = item

        return batch

    def run(  # noqa: PLR0913
        self,
        assets_to_validate,
        run_id=None,
        evaluation_parameters=None,
        run_name=None,
        run_time=None,
        catch_exceptions=None,
        result_format=None,
        checkpoint_identifier: Optional[GXCloudIdentifier] = None,
        checkpoint_name: Optional[str] = None,
        validation_id: Optional[str] = None,
    ) -> ValidationOperatorResult:
        assert not (run_id and run_name) and not (
            run_id and run_time
        ), "Please provide either a run_id or run_name and/or run_time."
        if isinstance(run_id, dict):
            run_id = RunIdentifier(**run_id)
        elif not isinstance(run_id, RunIdentifier):
            run_id = RunIdentifier(run_name=run_name, run_time=run_time)

        ###
        # NOTE: 20211010 - jdimatteo: This method is called by Checkpoint.run and below
        # usage of AsyncExecutor may speed up I/O bound validations by running them in parallel with multithreading
        # (if concurrency is enabled in the data context configuration).
        #
        # When this method is called by Checkpoint.run, len(assets_to_validate) may be 1 even if there are multiple
        # validations, because Checkpoint.run calls this method in a loop for each validation. AsyncExecutor is also
        # used in the Checkpoint.run loop to optionally run each validation in parallel with multithreading, so this
        # method's AsyncExecutor is nested within the Checkpoint.run AsyncExecutor. The AsyncExecutor logic to only use
        # multithreading when max_workers > 1 ensures that no nested multithreading is ever used when
        # len(assets_to_validate) is equal to 1. So no unnecessary multithreading is ever used here even though it may
        # be nested inside another AsyncExecutor (and this is a good thing because it avoids extra overhead associated
        # with each thread and minimizes the total number of threads to simplify debugging).
        with AsyncExecutor(
            self.data_context.concurrency, max_workers=len(assets_to_validate)
        ) as async_executor:
            batch_and_async_result_tuples = []
            for item in assets_to_validate:
                batch = self._build_batch_from_item(item)

                if hasattr(batch, "active_batch_id"):
                    batch_identifier = batch.active_batch_id
                else:
                    batch_identifier = batch.batch_id

                if result_format is None:
                    result_format = self.result_format

                batch_validate_arguments = {
                    "run_id": run_id,
                    "result_format": result_format,
                    "evaluation_parameters": evaluation_parameters,
                }

                if catch_exceptions is not None:
                    batch_validate_arguments["catch_exceptions"] = catch_exceptions

                if checkpoint_name is not None:
                    batch_validate_arguments["checkpoint_name"] = checkpoint_name

                batch_and_async_result_tuples.append(
                    (
                        batch,
                        async_executor.submit(
                            batch.validate,
                            **batch_validate_arguments,
                        ),
                    )
                )

            run_results = {}
            for batch, async_batch_validation_result in batch_and_async_result_tuples:
                if self._using_cloud_context:
                    expectation_suite_identifier = GXCloudIdentifier(
                        resource_type=GXCloudRESTResource.EXPECTATION_SUITE,
                        id=batch._expectation_suite.ge_cloud_id,
                    )
                    validation_result_id = GXCloudIdentifier(
                        resource_type=GXCloudRESTResource.VALIDATION_RESULT
                    )
                else:
                    expectation_suite_identifier = ExpectationSuiteIdentifier(
                        expectation_suite_name=batch._expectation_suite.expectation_suite_name
                    )
                    validation_result_id = ValidationResultIdentifier(
                        batch_identifier=batch_identifier,
                        expectation_suite_identifier=expectation_suite_identifier,
                        run_id=run_id,
                    )

                validation_result = async_batch_validation_result.result()
                validation_result.meta["validation_id"] = validation_id
                validation_result.meta["checkpoint_id"] = (
                    checkpoint_identifier.id if checkpoint_identifier else None
                )

                batch_actions_results = self._run_actions(
                    batch=batch,
                    expectation_suite_identifier=expectation_suite_identifier,
                    expectation_suite=batch._expectation_suite,
                    batch_validation_result=validation_result,
                    run_id=run_id,
                    validation_result_id=validation_result_id,
                    checkpoint_identifier=checkpoint_identifier,
                )

                run_result_obj = {
                    "validation_result": validation_result,
                    "actions_results": batch_actions_results,
                }
                run_results[validation_result_id] = run_result_obj

        return ValidationOperatorResult(
            run_id=run_id,
            run_results=run_results,
            validation_operator_config=self.validation_operator_config,
            evaluation_parameters=evaluation_parameters,
        )

    def _run_actions(  # noqa: PLR0913
        self,
        batch: Union[Batch, DataAsset],
        expectation_suite_identifier: ExpectationSuiteIdentifier,
        expectation_suite,
        batch_validation_result,
        run_id,
        validation_result_id=None,
        checkpoint_identifier=None,
    ):
        """
        Runs all actions configured for this operator on the result of validating one
        batch against one expectation suite.

        If an action fails with an exception, the method does not continue.

        :param batch:
        :param expectation_suite:
        :param batch_validation_result:
        :param run_id:
        :return: a dictionary: {action name -> result returned by the action}
        """
        batch_actions_results = {}
        for action in self.action_list:
            # NOTE: Eugene: 2019-09-23: log the info about the batch and the expectation suite
            name = action["name"]
            logger.debug(f"Processing validation action with name {name}")

            if hasattr(batch, "active_batch_id"):
                batch_identifier = batch.active_batch_id
            else:
                batch_identifier = batch.batch_id

            if validation_result_id is None:
                validation_result_id = ValidationResultIdentifier(
                    expectation_suite_identifier=expectation_suite_identifier,
                    run_id=run_id,
                    batch_identifier=batch_identifier,
                )
            try:
                action_result = self.actions[name].run(
                    validation_result_suite_identifier=validation_result_id,
                    validation_result_suite=batch_validation_result,
                    data_asset=batch,
                    payload=batch_actions_results,
                    expectation_suite_identifier=expectation_suite_identifier,
                    checkpoint_identifier=checkpoint_identifier,
                )

                # Transform action_result if it not a dictionary.
                if isinstance(action_result, GXCloudResourceRef):
                    transformed_result = {
                        "id": action_result.id,
                        "validation_result_url": action_result.response["data"][
                            "attributes"
                        ]["validation_result"]["display_url"],
                    }
                elif action_result is None:
                    transformed_result = {}
                else:
                    transformed_result = action_result

                # add action_result
                batch_actions_results[action["name"]] = transformed_result
                batch_actions_results[action["name"]]["class"] = action["action"][
                    "class_name"
                ]

            except Exception as e:
                logger.exception(f"Error running action with name {action['name']}")
                raise e

        return batch_actions_results
