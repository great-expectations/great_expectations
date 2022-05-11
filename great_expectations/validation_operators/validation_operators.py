import logging
import warnings
from collections import OrderedDict
from typing import Union

from dateutil.parser import parse

import great_expectations.exceptions as ge_exceptions
from great_expectations.checkpoint.util import send_slack_notification
from great_expectations.core.async_executor import AsyncExecutor
from great_expectations.core.batch import Batch
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.data_asset import DataAsset
from great_expectations.data_asset.util import parse_result_format
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    GeCloudIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.validation_operators.types.validation_operator_result import (
    ValidationOperatorResult,
)

logger = logging.getLogger(__name__)


class ValidationOperator:
    "\n    The base class of all validation operators.\n\n    It defines the signature of the public run method. This method and the validation_operator_config property are the\n    only contract re operators' API. Everything else is up to the implementors\n    of validation operator classes that will be the descendants of this base class.\n"

    def __init__(self) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        self._validation_operator_config = None

    @property
    def validation_operator_config(self) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        This method builds the config dict of a particular validation operator. The "kwargs" key is what really\n        distinguishes different validation operators.\n\n        e.g.:\n        {\n            "class_name": "ActionListValidationOperator",\n            "module_name": "great_expectations.validation_operators",\n            "name": self.name,\n            "kwargs": {\n                "action_list": self.action_list\n            },\n        }\n\n        {\n            "class_name": "WarningAndFailureExpectationSuitesValidationOperator",\n            "module_name": "great_expectations.validation_operators",\n            "name": self.name,\n            "kwargs": {\n                "action_list": self.action_list,\n                "base_expectation_suite_name": self.base_expectation_suite_name,\n                "expectation_suite_name_suffixes": self.expectation_suite_name_suffixes,\n                "stop_on_first_error": self.stop_on_first_error,\n                "slack_webhook": self.slack_webhook,\n                "notify_on": self.notify_on,\n            },\n        }\n        '
        raise NotImplementedError

    def run(
        self,
        assets_to_validate,
        run_id=None,
        evaluation_parameters=None,
        run_name=None,
        run_time=None,
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        raise NotImplementedError


class ActionListValidationOperator(ValidationOperator):
    '\n\n    ActionListValidationOperator validates each batch in its ``run`` method\'s ``assets_to_validate`` argument against the Expectation Suite included within that batch.\n\n    Then it invokes a list of configured actions on every validation result.\n\n    Each action in the list must be an instance of :py:class:`ValidationAction<great_expectations.validation_operators.actions.ValidationAction>`\n    class (or its descendants). See the actions included in Great Expectations and how to configure them :py:mod:`here<great_expectations.validation_operators.actions>`. You can also implement your own actions by extending the base class.\n\n    The init command includes this operator in the default configuration file.\n\n\n    **Configuration**\n\n    An instance of ActionListValidationOperator is included in the default configuration file ``great_expectations.yml`` that ``great_expectations init`` command creates.\n\n    .. code-block:: yaml\n\n      perform_action_list_operator:  # this is the name you will use when you invoke the operator\n        class_name: ActionListValidationOperator\n\n        # the operator will call the following actions on each validation result\n        # you can remove or add actions to this list. See the details in the actions\n        # reference\n        action_list:\n          - name: store_validation_result\n            action:\n              class_name: StoreValidationResultAction\n              target_store_name: validations_store\n          - name: send_slack_notification_on_validation_result\n            action:\n              class_name: SlackNotificationAction\n              # put the actual webhook URL in the uncommitted/config_variables.yml file\n              slack_webhook: ${validation_notification_slack_webhook}\n              notify_on: all # possible values: "all", "failure", "success"\n              notify_with: optional list of DataDocs sites (ie local_site or gcs_site") to include in Slack notification. Will default to including all configured DataDocs sites.\n              renderer:\n                module_name: great_expectations.render.renderer.slack_renderer\n                class_name: SlackRenderer\n          - name: update_data_docs\n            action:\n              class_name: UpdateDataDocsAction\n\n\n    **Invocation**\n\n    This is an example of invoking an instance of a Validation Operator from Python:\n\n    .. code-block:: python\n\n        results = context.run_validation_operator(\n            assets_to_validate=[batch0, batch1, ...],\n            run_id=RunIdentifier(**{\n              "run_name": "some_string_that_uniquely_identifies_this_run",\n              "run_time": "2020-04-29T10:46:03.197008"  # optional run timestamp, defaults to current UTC datetime\n            }),  # you may also pass in a dictionary with run_name and run_time keys\n            validation_operator_name="operator_instance_name",\n        )\n\n    * ``assets_to_validate`` - an iterable that specifies the data assets that the operator will validate. The members of the list can be either batches or triples that will allow the operator to fetch the batch: (data_asset_name, expectation_suite_name, batch_kwargs) using this method: :py:meth:`~great_expectations.data_context.BaseDataContext.get_batch`\n    * ``run_id`` - pipeline run id of type RunIdentifier, consisting of a ``run_time`` (always assumed to be UTC time) and ``run_name`` string that is meaningful to you and will help you refer to the result of this operation later\n    * ``validation_operator_name`` you can instances of a class that implements a Validation Operator\n\n    The ``run`` method returns a ValidationOperatorResult object:\n\n    ::\n\n        {\n            "run_id": {"run_time": "20200527T041833.074212Z", "run_name": "my_run_name"},\n            "success": True,\n            "evaluation_parameters": None,\n            "validation_operator_config": {\n                "class_name": "ActionListValidationOperator",\n                "module_name": "great_expectations.validation_operators",\n                "name": "action_list_operator",\n                "kwargs": {\n                    "action_list": [\n                        {\n                            "name": "store_validation_result",\n                            "action": {"class_name": "StoreValidationResultAction"},\n                        },\n                        {\n                            "name": "store_evaluation_params",\n                            "action": {"class_name": "StoreEvaluationParametersAction"},\n                        },\n                        {\n                            "name": "update_data_docs",\n                            "action": {"class_name": "UpdateDataDocsAction"},\n                        },\n                    ]\n                },\n            },\n            "run_results": {\n                ValidationResultIdentifier: {\n                    "validation_result": ExpectationSuiteValidationResult object,\n                    "actions_results": {\n                        "store_validation_result": {},\n                        "store_evaluation_params": {},\n                        "update_data_docs": {},\n                    },\n                }\n            },\n        }\n'

    def __init__(
        self,
        data_context,
        action_list,
        name,
        result_format={"result_format": "SUMMARY"},
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
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
        for action_config in action_list:
            assert isinstance(action_config, dict)
            if not (set(action_config.keys()) == {"name", "action"}):
                raise KeyError(
                    'Action config keys must be ("name", "action"). Instead got {}'.format(
                        action_config.keys()
                    )
                )
            config = action_config["action"]
            module_name = "great_expectations.validation_operators"
            new_action = instantiate_class_from_config(
                config=config,
                runtime_environment={"data_context": self.data_context},
                config_defaults={"module_name": module_name},
            )
            if not new_action:
                raise ge_exceptions.ClassInstantiationError(
                    module_name=module_name,
                    package_name=None,
                    class_name=config["class_name"],
                )
            self.actions[action_config["name"]] = new_action

    @property
    def validation_operator_config(self) -> dict:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
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
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Internal helper method to take an asset to validate, which can be either:\n          (1) a DataAsset; or\n          (2) a tuple of data_asset_name, expectation_suite_name, and batch_kwargs (suitable for passing to get_batch)\n\n        Args:\n            item: The item to convert to a batch (see above)\n\n        Returns:\n            A batch of data\n\n        "
        if isinstance(item, tuple):
            if not (
                (len(item) == 2)
                and isinstance(item[0], dict)
                and isinstance(item[1], str)
            ):
                raise ValueError("Unable to build batch from item.")
            batch = self.data_context.get_batch(
                batch_kwargs=item[0], expectation_suite_name=item[1]
            )
        else:
            batch = item
        return batch

    def run(
        self,
        assets_to_validate,
        run_id=None,
        evaluation_parameters=None,
        run_name=None,
        run_time=None,
        catch_exceptions=None,
        result_format=None,
        checkpoint_identifier=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        assert (not (run_id and run_name)) and (
            not (run_id and run_time)
        ), "Please provide either a run_id or run_name and/or run_time."
        if isinstance(run_id, str) and (not run_name):
            warnings.warn(
                "String run_ids are deprecated as of v0.11.0 and support will be removed in v0.16. Please provide a run_id of type RunIdentifier(run_name=None, run_time=None), or a dictionary containing run_name and run_time (both optional). Instead of providing a run_id, you may also providerun_name and run_time separately.",
                DeprecationWarning,
            )
            try:
                run_time = parse(run_id)
            except (ValueError, TypeError):
                pass
            run_id = RunIdentifier(run_name=run_id, run_time=run_time)
        elif isinstance(run_id, dict):
            run_id = RunIdentifier(**run_id)
        elif not isinstance(run_id, RunIdentifier):
            run_id = RunIdentifier(run_name=run_name, run_time=run_time)
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
                batch_and_async_result_tuples.append(
                    (
                        batch,
                        async_executor.submit(
                            batch.validate, **batch_validate_arguments
                        ),
                    )
                )
            run_results = {}
            for (batch, async_batch_validation_result) in batch_and_async_result_tuples:
                if self.data_context.ge_cloud_mode:
                    expectation_suite_identifier = GeCloudIdentifier(
                        resource_type="expectation_suite",
                        ge_cloud_id=batch._expectation_suite.ge_cloud_id,
                    )
                    validation_result_id = GeCloudIdentifier(
                        resource_type="suite_validation_result"
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
                batch_actions_results = self._run_actions(
                    batch=batch,
                    expectation_suite_identifier=expectation_suite_identifier,
                    expectation_suite=batch._expectation_suite,
                    batch_validation_result=async_batch_validation_result.result(),
                    run_id=run_id,
                    validation_result_id=validation_result_id,
                    checkpoint_identifier=checkpoint_identifier,
                )
                run_result_obj = {
                    "validation_result": async_batch_validation_result.result(),
                    "actions_results": batch_actions_results,
                }
                run_results[validation_result_id] = run_result_obj
        return ValidationOperatorResult(
            run_id=run_id,
            run_results=run_results,
            validation_operator_config=self.validation_operator_config,
            evaluation_parameters=evaluation_parameters,
        )

    def _run_actions(
        self,
        batch: Union[(Batch, DataAsset)],
        expectation_suite_identifier: ExpectationSuiteIdentifier,
        expectation_suite,
        batch_validation_result,
        run_id,
        validation_result_id=None,
        checkpoint_identifier=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Runs all actions configured for this operator on the result of validating one\n        batch against one expectation suite.\n\n        If an action fails with an exception, the method does not continue.\n\n        :param batch:\n        :param expectation_suite:\n        :param batch_validation_result:\n        :param run_id:\n        :return: a dictionary: {action name -> result returned by the action}\n        "
        batch_actions_results = {}
        for action in self.action_list:
            logger.debug(f"Processing validation action with name {action['name']}")
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
                action_result = self.actions[action["name"]].run(
                    validation_result_suite_identifier=validation_result_id,
                    validation_result_suite=batch_validation_result,
                    data_asset=batch,
                    payload=batch_actions_results,
                    expectation_suite_identifier=expectation_suite_identifier,
                    checkpoint_identifier=checkpoint_identifier,
                )
                batch_actions_results[action["name"]] = (
                    {} if (action_result is None) else action_result
                )
                batch_actions_results[action["name"]]["class"] = action["action"][
                    "class_name"
                ]
            except Exception as e:
                logger.exception(f"Error running action with name {action['name']}")
                raise e
        return batch_actions_results


class WarningAndFailureExpectationSuitesValidationOperator(
    ActionListValidationOperator
):
    '\n    WarningAndFailureExpectationSuitesValidationOperator is a validation operator\n    that accepts a list batches of data assets (or the information necessary to fetch these batches).\n    The operator retrieves 2 expectation suites for each data asset/batch - one containing\n    the critical expectations ("failure") and the other containing non-critical expectations\n    ("warning"). By default, the operator assumes that the first is called "failure" and the\n    second is called "warning", but "base_expectation_suite_name" attribute can be specified\n    in the operator\'s configuration to make sure it searched for "{base_expectation_suite_name}.failure"\n    and {base_expectation_suite_name}.warning" expectation suites for each data asset.\n\n    The operator validates each batch against its "failure" and "warning" expectation suites and\n    invokes a list of actions on every validation result.\n\n    The list of these actions is specified in the operator\'s configuration\n\n    Each action in the list must be an instance of ValidationAction\n    class (or its descendants).\n\n    The operator sends a Slack notification (if "slack_webhook" is present in its\n    config). The "notify_on" config property controls whether the notification\n    should be sent only in the case of failure ("failure"), only in the case\n    of success ("success"), or always ("all").\n\n\n    **Configuration**\n\n    Below is an example of this operator\'s configuration:\n\n    .. code-block:: yaml\n\n        run_warning_and_failure_expectation_suites:\n            class_name: WarningAndFailureExpectationSuitesValidationOperator\n\n            # the following two properties are optional - by default the operator looks for\n            # expectation suites named "failure" and "warning".\n            # You can use these two properties to override these names.\n            # e.g., with expectation_suite_name_prefix=boo_ and\n            # expectation_suite_name_suffixes = ["red", "green"], the operator\n            # will look for expectation suites named "boo_red" and "boo_green"\n            expectation_suite_name_prefix="",\n            expectation_suite_name_suffixes=["failure", "warning"],\n\n            # optional - if true, the operator will stop and exit after first failed validation. false by default.\n            stop_on_first_error=False,\n\n            # put the actual webhook URL in the uncommitted/config_variables.yml file\n            slack_webhook: ${validation_notification_slack_webhook}\n            # optional - if "all" - notify always, "success" - notify only on success, "failure" - notify only on failure\n            notify_on="all"\n\n            # the operator will call the following actions on each validation result\n            # you can remove or add actions to this list. See the details in the actions\n            # reference\n            action_list:\n              - name: store_validation_result\n                action:\n                  class_name: StoreValidationResultAction\n                  target_store_name: validations_store\n              - name: store_evaluation_params\n                action:\n                  class_name: StoreEvaluationParametersAction\n                  target_store_name: evaluation_parameter_store\n\n\n    **Invocation**\n\n    This is an example of invoking an instance of a Validation Operator from Python:\n\n    .. code-block:: python\n\n        results = context.run_validation_operator(\n            assets_to_validate=[batch0, batch1, ...],\n            run_id=RunIdentifier(**{\n              "run_name": "some_string_that_uniquely_identifies_this_run",\n              "run_time": "2020-04-29T10:46:03.197008"  # optional run timestamp, defaults to current UTC datetime\n            }),  # you may also pass in a dictionary with run_name and run_time keys\n            validation_operator_name="operator_instance_name",\n        )\n\n    * `assets_to_validate` - an iterable that specifies the data assets that the operator will validate. The members of the list can be either batches or triples that will allow the operator to fetch the batch: (data_asset_name, expectation_suite_name, batch_kwargs) using this method: :py:meth:`~great_expectations.data_context.BaseDataContext.get_batch`\n    * run_id - pipeline run id of type RunIdentifier, consisting of a run_time (always assumed to be UTC time) and run_name string that is meaningful to you and will help you refer to the result of this operation later\n    * validation_operator_name you can instances of a class that implements a Validation Operator\n\n    The `run` method returns a ValidationOperatorResult object.\n\n    The value of "success" is True if no critical expectation suites ("failure") failed to validate (non-critical warning") expectation suites are allowed to fail without affecting the success status of the run.\n\n    .. code-block:: json\n\n        {\n            "run_id": {"run_time": "20200527T041833.074212Z", "run_name": "my_run_name"},\n            "success": True,\n            "evaluation_parameters": None,\n            "validation_operator_config": {\n                "class_name": "WarningAndFailureExpectationSuitesValidationOperator",\n                "module_name": "great_expectations.validation_operators",\n                "name": "warning_and_failure_operator",\n                "kwargs": {\n                    "action_list": [\n                        {\n                            "name": "store_validation_result",\n                            "action": {"class_name": "StoreValidationResultAction"},\n                        },\n                        {\n                            "name": "store_evaluation_params",\n                            "action": {"class_name": "StoreEvaluationParametersAction"},\n                        },\n                        {\n                            "name": "update_data_docs",\n                            "action": {"class_name": "UpdateDataDocsAction"},\n                        },\n                    ],\n                    "base_expectation_suite_name": ...,\n                    "expectation_suite_name_suffixes": ...,\n                    "stop_on_first_error": ...,\n                    "slack_webhook": ...,\n                    "notify_on": ...,\n                    "notify_with":...,\n                },\n            },\n            "run_results": {\n                ValidationResultIdentifier: {\n                    "validation_result": ExpectationSuiteValidationResult object,\n                    "expectation_suite_severity_level": "warning",\n                    "actions_results": {\n                        "store_validation_result": {},\n                        "store_evaluation_params": {},\n                        "update_data_docs": {},\n                    },\n                }\n            }\n        }\n\n'

    def __init__(
        self,
        data_context,
        action_list,
        name,
        base_expectation_suite_name=None,
        expectation_suite_name_suffixes=None,
        stop_on_first_error=False,
        slack_webhook=None,
        notify_on="all",
        notify_with=None,
        result_format={"result_format": "SUMMARY"},
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        super().__init__(data_context, action_list, name)
        if expectation_suite_name_suffixes is None:
            expectation_suite_name_suffixes = [".failure", ".warning"]
        self.stop_on_first_error = stop_on_first_error
        self.base_expectation_suite_name = base_expectation_suite_name
        assert len(expectation_suite_name_suffixes) == 2
        for suffix in expectation_suite_name_suffixes:
            assert isinstance(suffix, str)
        self.expectation_suite_name_suffixes = expectation_suite_name_suffixes
        self.slack_webhook = slack_webhook
        self.notify_on = notify_on
        self.notify_with = notify_with
        result_format = parse_result_format(result_format)
        assert result_format["result_format"] in [
            "BOOLEAN_ONLY",
            "BASIC",
            "SUMMARY",
            "COMPLETE",
        ]
        self.result_format = result_format

    @property
    def validation_operator_config(self) -> dict:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if self._validation_operator_config is None:
            self._validation_operator_config = {
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
                    "notify_with": self.notify_with,
                    "result_format": self.result_format,
                },
            }
        return self._validation_operator_config

    def _build_slack_query(self, validation_operator_result: ValidationOperatorResult):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        success = validation_operator_result.success
        status_text = "Success :white_check_mark:" if success else "Failed :x:"
        run_id = validation_operator_result.run_id
        run_name = run_id.run_name
        run_time = run_id.run_time.strftime("%x %X")
        batch_identifiers = sorted(validation_operator_result.list_batch_identifiers())
        failed_data_assets_msg_strings = []
        run_results = validation_operator_result.run_results
        failure_level_run_results = {
            validation_result_identifier: run_result
            for (validation_result_identifier, run_result) in run_results.items()
            if (run_result["expectation_suite_severity_level"] == "failure")
        }
        if failure_level_run_results:
            failed_data_assets_msg_strings = [
                (
                    (
                        validation_result_identifier.expectation_suite_identifier.expectation_suite_name
                        + "-"
                    )
                    + validation_result_identifier.batch_identifier
                )
                for (
                    validation_result_identifier,
                    run_result,
                ) in failure_level_run_results.items()
                if (not run_result["validation_result"].success)
            ]
        title_block = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*FailureVsWarning Validation Operator Completed.*",
            },
        }
        divider_block = {"type": "divider"}
        query = {"blocks": [divider_block, title_block, divider_block]}
        status_element = {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Status*: {status_text}"},
        }
        query["blocks"].append(status_element)
        batch_identifiers_element = {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Batch Id List:* {batch_identifiers}"},
        }
        query["blocks"].append(batch_identifiers_element)
        if not success:
            failed_data_assets_element = {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Failed Batches:* {failed_data_assets_msg_strings}",
                },
            }
            query["blocks"].append(failed_data_assets_element)
        run_name_element = {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Run Name:* {run_name}"},
        }
        query["blocks"].append(run_name_element)
        run_time_element = {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Run Time:* {run_time}"},
        }
        query["blocks"].append(run_time_element)
        query["blocks"].append(divider_block)
        documentation_url = "https://docs.greatexpectations.io/en/latest/reference/validation_operators/warning_and_failure_expectation_suites_validation_operator.html"
        footer_section = {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "Learn about FailureVsWarning Validation Operators at {}".format(
                        documentation_url
                    ),
                }
            ],
        }
        query["blocks"].append(footer_section)
        return query

    def run(
        self,
        assets_to_validate,
        run_id=None,
        base_expectation_suite_name=None,
        evaluation_parameters=None,
        run_name=None,
        run_time=None,
        result_format=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        assert (not (run_id and run_name)) and (
            not (run_id and run_time)
        ), "Please provide either a run_id or run_name and/or run_time."
        if isinstance(run_id, str) and (not run_name):
            warnings.warn(
                "String run_ids are deprecated as of v0.11.0 and support will be removed in v0.16. Please provide a run_id of type RunIdentifier(run_name=None, run_time=None), or a dictionary containing run_name and run_time (both optional). Instead of providing a run_id, you may also providerun_name and run_time separately.",
                DeprecationWarning,
            )
            try:
                run_time = parse(run_id)
            except (ValueError, TypeError):
                pass
            run_id = RunIdentifier(run_name=run_id, run_time=run_time)
        elif isinstance(run_id, dict):
            run_id = RunIdentifier(**run_id)
        elif not isinstance(run_id, RunIdentifier):
            run_id = RunIdentifier(run_name=run_name, run_time=run_time)
        if base_expectation_suite_name is None:
            if self.base_expectation_suite_name is None:
                raise ValueError(
                    "base_expectation_suite_name must be configured in the validation operator or passed at runtime"
                )
            base_expectation_suite_name = self.base_expectation_suite_name
        run_results = {}
        for item in assets_to_validate:
            batch = self._build_batch_from_item(item)
            batch_id = batch.batch_id
            run_id = run_id
            assert not (batch_id is None)
            assert not (run_id is None)
            failure_expectation_suite_identifier = ExpectationSuiteIdentifier(
                expectation_suite_name=(
                    base_expectation_suite_name
                    + self.expectation_suite_name_suffixes[0]
                )
            )
            failure_validation_result_id = ValidationResultIdentifier(
                expectation_suite_identifier=failure_expectation_suite_identifier,
                run_id=run_id,
                batch_identifier=batch_id,
            )
            failure_expectation_suite = None
            try:
                failure_expectation_suite = self.data_context.stores[
                    self.data_context.expectations_store_name
                ].get(failure_expectation_suite_identifier)
            except Exception:
                logger.debug(
                    "Failure expectation suite not found: {}".format(
                        failure_expectation_suite_identifier
                    )
                )
            if failure_expectation_suite:
                failure_run_result_obj = {"expectation_suite_severity_level": "failure"}
                failure_validation_result = batch.validate(
                    failure_expectation_suite,
                    run_id,
                    result_format=(
                        result_format if result_format else self.result_format
                    ),
                    evaluation_parameters=evaluation_parameters,
                )
                failure_run_result_obj["validation_result"] = failure_validation_result
                failure_actions_results = self._run_actions(
                    batch,
                    failure_expectation_suite_identifier,
                    failure_expectation_suite,
                    failure_validation_result,
                    run_id,
                )
                failure_run_result_obj["actions_results"] = failure_actions_results
                run_results[failure_validation_result_id] = failure_run_result_obj
                if (not failure_validation_result.success) and self.stop_on_first_error:
                    break
            warning_expectation_suite_identifier = ExpectationSuiteIdentifier(
                expectation_suite_name=(
                    base_expectation_suite_name
                    + self.expectation_suite_name_suffixes[1]
                )
            )
            warning_validation_result_id = ValidationResultIdentifier(
                expectation_suite_identifier=warning_expectation_suite_identifier,
                run_id=run_id,
                batch_identifier=batch.batch_id,
            )
            warning_expectation_suite = None
            try:
                warning_expectation_suite = self.data_context.stores[
                    self.data_context.expectations_store_name
                ].get(warning_expectation_suite_identifier)
            except Exception:
                logger.debug(
                    "Warning expectation suite not found: {}".format(
                        warning_expectation_suite_identifier
                    )
                )
            if warning_expectation_suite:
                warning_run_result_obj = {"expectation_suite_severity_level": "warning"}
                warning_validation_result = batch.validate(
                    warning_expectation_suite,
                    run_id,
                    result_format=(
                        result_format if result_format else self.result_format
                    ),
                    evaluation_parameters=evaluation_parameters,
                )
                warning_run_result_obj["validation_result"] = warning_validation_result
                warning_actions_results = self._run_actions(
                    batch,
                    warning_expectation_suite_identifier,
                    warning_expectation_suite,
                    warning_validation_result,
                    run_id,
                )
                warning_run_result_obj["actions_results"] = warning_actions_results
                run_results[warning_validation_result_id] = warning_run_result_obj
        validation_operator_result = ValidationOperatorResult(
            run_id=run_id,
            run_results=run_results,
            validation_operator_config=self.validation_operator_config,
            evaluation_parameters=evaluation_parameters,
            success=all(
                [
                    run_result_obj["validation_result"].success
                    for run_result_obj in run_results.values()
                    if (run_result_obj["expectation_suite_severity_level"] == "failure")
                ]
            ),
        )
        if self.slack_webhook:
            if (
                (self.notify_on == "all")
                or (
                    (self.notify_on == "success") and validation_operator_result.success
                )
                or (
                    (self.notify_on == "failure")
                    and (not validation_operator_result.success)
                )
            ):
                slack_query = self._build_slack_query(
                    validation_operator_result=validation_operator_result
                )
                send_slack_notification(
                    query=slack_query, slack_webhook=self.slack_webhook
                )
        return validation_operator_result
