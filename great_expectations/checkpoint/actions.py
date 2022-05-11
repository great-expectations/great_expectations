
'\nAn action is a way to take an arbitrary method and make it configurable and runnable within a Data Context.\n\nThe only requirement from an action is for it to have a take_action method.\n'
import logging
import warnings
from typing import Dict, Optional, Union
from urllib.parse import urljoin
from great_expectations.core import ExpectationSuiteValidationResult
from great_expectations.data_context.types.refs import GeCloudResourceRef
try:
    import pypd
except ImportError:
    pypd = None
from great_expectations.checkpoint.util import send_cloud_notification, send_email, send_microsoft_teams_notifications, send_opsgenie_alert, send_slack_notification, send_sns_notification
from great_expectations.data_context.store.metric_store import MetricStore
from great_expectations.data_context.types.resource_identifiers import ExpectationSuiteIdentifier, GeCloudIdentifier, ValidationResultIdentifier
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.exceptions import ClassInstantiationError, DataContextError
logger = logging.getLogger(__name__)

class ValidationAction():
    '\n    This is the base class for all actions that act on validation results\n    and are aware of a Data Context namespace structure.\n\n    The Data Context is passed to this class in its constructor.\n    '

    def __init__(self, data_context) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.data_context = data_context

    def run(self, validation_result_suite: ExpectationSuiteValidationResult, validation_result_suite_identifier: Union[(ValidationResultIdentifier, GeCloudIdentifier)], data_asset, expectation_suite_identifier: ExpectationSuiteIdentifier=None, checkpoint_identifier=None, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n\n        :param validation_result_suite:\n        :param validation_result_suite_identifier:\n        :param data_asset:\n        :param expectation_suite_identifier:  The ExpectationSuiteIdentifier to use\n        :param checkpoint_identifier:  The Checkpoint to use\n        :param: kwargs - any additional arguments the child might use\n        :return:\n        '
        return self._run(validation_result_suite=validation_result_suite, validation_result_suite_identifier=validation_result_suite_identifier, data_asset=data_asset, expectation_suite_identifier=expectation_suite_identifier, checkpoint_identifier=checkpoint_identifier, **kwargs)

    def _run(self, validation_result_suite: ExpectationSuiteValidationResult, validation_result_suite_identifier: Union[(ValidationResultIdentifier, GeCloudIdentifier)], data_asset, expectation_suite_identifier=None, checkpoint_identifier=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return NotImplementedError

class NoOpAction(ValidationAction):

    def __init__(self, data_context) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__(data_context)

    def _run(self, validation_result_suite: ExpectationSuiteValidationResult, validation_result_suite_identifier: Union[(ValidationResultIdentifier, GeCloudIdentifier)], data_asset, expectation_suite_identifier=None, checkpoint_identifier=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        print('Happily doing nothing')

class SlackNotificationAction(ValidationAction):
    '\n    SlackNotificationAction sends a Slack notification to a given webhook.\n\n    **Configuration**\n\n    .. code-block:: yaml\n\n        - name: send_slack_notification_on_validation_result\n        action:\n          class_name: SlackNotificationAction\n          # put the actual webhook URL in the uncommitted/config_variables.yml file\n          # or pass in as environment variable\n          # use slack_webhook when not using slack bot token\n          slack_webhook: ${validation_notification_slack_webhook}\n          # pass slack token and slack channel when not using slack_webhook\n          slack_token: # token from slack app\n          slack_channel: # slack channel that messages should go to\n          notify_on: all # possible values: "all", "failure", "success"\n          notify_with: # optional list of DataDocs site names to display in Slack message. Defaults to showing all\n          renderer:\n            # the class that implements the message to be sent\n            # this is the default implementation, but you can\n            # implement a custom one\n            module_name: great_expectations.render.renderer.slack_renderer\n            class_name: SlackRenderer\n\n    '

    def __init__(self, data_context, renderer, slack_webhook=None, slack_token=None, slack_channel=None, notify_on='all', notify_with=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Construct a SlackNotificationAction\n\n        Args:\n            data_context:\n            renderer: dictionary specifying the renderer used to generate a query consumable by Slack API, for example:\n                {\n                   "module_name": "great_expectations.render.renderer.slack_renderer",\n                   "class_name": "SlackRenderer",\n               }\n            slack_webhook: incoming Slack webhook to which to send notification\n            notify_on: "all", "failure", "success" - specifies validation status that will trigger notification\n            payload: *Optional* payload from other ValidationActions\n        '
        super().__init__(data_context)
        self.renderer = instantiate_class_from_config(config=renderer, runtime_environment={}, config_defaults={})
        module_name = renderer['module_name']
        if (not self.renderer):
            raise ClassInstantiationError(module_name=module_name, package_name=None, class_name=renderer['class_name'])
        if ((not slack_token) and slack_channel):
            assert slack_webhook
        if (not slack_webhook):
            assert (slack_token and slack_channel)
        assert (not (slack_webhook and slack_channel and slack_token))
        self.slack_webhook = slack_webhook
        self.slack_token = slack_token
        self.slack_channel = slack_channel
        self.notify_on = notify_on
        self.notify_with = notify_with

    def _run(self, validation_result_suite: ExpectationSuiteValidationResult, validation_result_suite_identifier: Union[(ValidationResultIdentifier, GeCloudIdentifier)], data_asset=None, payload=None, expectation_suite_identifier=None, checkpoint_identifier=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        logger.debug('SlackNotificationAction.run')
        if (validation_result_suite is None):
            logger.warning(f'No validation_result_suite was passed to {type(self).__name__} action. Skipping action.')
            return
        if (not isinstance(validation_result_suite_identifier, (ValidationResultIdentifier, GeCloudIdentifier))):
            raise TypeError('validation_result_suite_id must be of type ValidationResultIdentifier or GeCloudIdentifier, not {}'.format(type(validation_result_suite_identifier)))
        validation_success = validation_result_suite.success
        data_docs_pages = None
        if payload:
            for action_names in payload.keys():
                if (payload[action_names]['class'] == 'UpdateDataDocsAction'):
                    data_docs_pages = payload[action_names]
        if ((self.notify_on == 'all') or ((self.notify_on == 'success') and validation_success) or ((self.notify_on == 'failure') and (not validation_success))):
            query: Dict = self.renderer.render(validation_result_suite, data_docs_pages, self.notify_with)
            slack_notif_result = send_slack_notification(query, slack_webhook=self.slack_webhook, slack_token=self.slack_token, slack_channel=self.slack_channel)
            return {'slack_notification_result': slack_notif_result}
        else:
            return {'slack_notification_result': 'none required'}

class PagerdutyAlertAction(ValidationAction):
    '\n    PagerdutyAlertAction sends a pagerduty event\n\n    **Configuration**\n\n    .. code-block:: yaml\n\n        - name: send_pagerduty_alert_on_validation_result\n        action:\n          class_name: PagerdutyAlertAction\n          api_key: ${pagerduty_api_key} # Events API v2 key\n          routing_key: # The 32 character Integration Key for an integration on a service or on a global ruleset.\n          notify_on: failure # possible values: "all", "failure", "success"\n\n    '

    def __init__(self, data_context, api_key, routing_key, notify_on='failure') -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Construct a PagerdutyAlertAction\n\n        Args:\n            data_context:\n            api_key: Events API v2 key for pagerduty.\n            routing_key: The 32 character Integration Key for an integration on a service or on a global ruleset.\n            notify_on: "all", "failure", "success" - specifies validation status that will trigger notification\n        '
        super().__init__(data_context)
        if (not pypd):
            raise DataContextError("ModuleNotFoundError: No module named 'pypd'")
        self.api_key = api_key
        assert api_key, 'No Pagerduty api_key found in action config.'
        self.routing_key = routing_key
        assert routing_key, 'No Pagerduty routing_key found in action config.'
        self.notify_on = notify_on

    def _run(self, validation_result_suite: ExpectationSuiteValidationResult, validation_result_suite_identifier: Union[(ValidationResultIdentifier, GeCloudIdentifier)], data_asset=None, payload=None, expectation_suite_identifier=None, checkpoint_identifier=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        logger.debug('PagerdutyAlertAction.run')
        if (validation_result_suite is None):
            logger.warning(f'No validation_result_suite was passed to {type(self).__name__} action. Skipping action.')
            return
        if (not isinstance(validation_result_suite_identifier, (ValidationResultIdentifier, GeCloudIdentifier))):
            raise TypeError('validation_result_suite_id must be of type ValidationResultIdentifier or GeCloudIdentifier, not {}'.format(type(validation_result_suite_identifier)))
        validation_success = validation_result_suite.success
        if ((self.notify_on == 'all') or ((self.notify_on == 'success') and validation_success) or ((self.notify_on == 'failure') and (not validation_success))):
            expectation_suite_name = validation_result_suite.meta.get('expectation_suite_name', '__no_expectation_suite_name__')
            pypd.api_key = self.api_key
            pypd.EventV2.create(data={'routing_key': self.routing_key, 'dedup_key': expectation_suite_name, 'event_action': 'trigger', 'payload': {'summary': f'Great Expectations suite check {expectation_suite_name} has failed', 'severity': 'critical', 'source': 'Great Expectations'}})
            return {'pagerduty_alert_result': 'success'}
        return {'pagerduty_alert_result': 'none sent'}

class MicrosoftTeamsNotificationAction(ValidationAction):
    '\n    MicrosoftTeamsNotificationAction sends a Microsoft Teams notification to a given webhook.\n\n    **Configuration**\n\n    .. code-block:: yaml\n\n        - name: send_microsoft_teams_notification_on_validation_result\n        action:\n          class_name: MicrosoftTeamsNotificationAction\n          # put the actual webhook URL in the uncommitted/config_variables.yml file\n          # or pass in as environment variable\n          microsoft_teams_webhook: ${validation_notification_microsoft_teams_webhook}\n          notify_on: all # possible values: "all", "failure", "success"\n          renderer:\n            # the class that implements the message to be sent\n            # this is the default implementation, but you can\n            # implement a custom one\n            module_name: great_expectations.render.renderer.microsoft_teams_renderer\n            class_name: MicrosoftTeamsRenderer\n\n    '

    def __init__(self, data_context, renderer, microsoft_teams_webhook, notify_on='all') -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Construct a MicrosoftTeamsNotificationAction\n\n        Args:\n            data_context:\n            renderer: dictionary specifying the renderer used to generate a query consumable by teams API, for example:\n                {\n                   "module_name": "great_expectations.render.renderer.microsoft_teams_renderer",\n                   "class_name": "MicrosoftTeamsRenderer",\n               }\n            microsoft_teams_webhook: incoming Microsoft Teams webhook to which to send notifications\n            notify_on: "all", "failure", "success" - specifies validation status that will trigger notification\n            payload: *Optional* payload from other ValidationActions\n        '
        super().__init__(data_context)
        self.renderer = instantiate_class_from_config(config=renderer, runtime_environment={}, config_defaults={})
        module_name = renderer['module_name']
        if (not self.renderer):
            raise ClassInstantiationError(module_name=module_name, package_name=None, class_name=renderer['class_name'])
        self.teams_webhook = microsoft_teams_webhook
        assert microsoft_teams_webhook, 'No Microsoft teams webhook found in action config.'
        self.notify_on = notify_on

    def _run(self, validation_result_suite: ExpectationSuiteValidationResult, validation_result_suite_identifier: Union[(ValidationResultIdentifier, GeCloudIdentifier)], data_asset=None, payload=None, expectation_suite_identifier=None, checkpoint_identifier=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        logger.debug('MicrosoftTeamsNotificationAction.run')
        if (validation_result_suite is None):
            logger.warning(f'No validation_result_suite was passed to {type(self).__name__} action. Skipping action.')
            return
        if (not isinstance(validation_result_suite_identifier, (ValidationResultIdentifier, GeCloudIdentifier))):
            raise TypeError('validation_result_suite_id must be of type ValidationResultIdentifier or GeCloudIdentifier, not {}'.format(type(validation_result_suite_identifier)))
        validation_success = validation_result_suite.success
        data_docs_pages = None
        if payload:
            for action_names in payload.keys():
                if (payload[action_names]['class'] == 'UpdateDataDocsAction'):
                    data_docs_pages = payload[action_names]
        if ((self.notify_on == 'all') or ((self.notify_on == 'success') and validation_success) or ((self.notify_on == 'failure') and (not validation_success))):
            query = self.renderer.render(validation_result_suite, validation_result_suite_identifier, data_docs_pages)
            teams_notif_result = send_microsoft_teams_notifications(query, microsoft_teams_webhook=self.teams_webhook)
            return {'microsoft_teams_notification_result': teams_notif_result}
        else:
            return {'microsoft_teams_notification_result': None}

class OpsgenieAlertAction(ValidationAction):
    '\n    OpsgenieAlertAction creates and sends an Opsgenie alert\n\n    **Configuration**\n\n    .. code-block:: yaml\n\n        - name: send_opsgenie_alert_on_validation_result\n        action:\n          class_name: OpsgenieAlertAction\n          # put the actual webhook URL in the uncommitted/config_variables.yml file\n          # or pass in as environment variable\n          api_key: ${opsgenie_api_key} # Opsgenie API key\n          region: specifies the Opsgenie region. Populate \'EU\' for Europe otherwise leave empty\n          priority: specify the priority of the alert (P1 - P5) defaults to P3\n          notify_on: failure # possible values: "all", "failure", "success"\n\n    '

    def __init__(self, data_context, renderer, api_key, region=None, priority='P3', notify_on='failure') -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Construct a OpsgenieAlertAction\n\n        Args:\n            data_context:\n            api_key: Opsgenie API key\n            region: specifies the Opsgenie region. Populate \'EU\' for Europe otherwise do not set\n            priority: specify the priority of the alert (P1 - P5) defaults to P3\n            notify_on: "all", "failure", "success" - specifies validation status that will trigger notification\n        '
        super().__init__(data_context)
        self.renderer = instantiate_class_from_config(config=renderer, runtime_environment={}, config_defaults={})
        module_name = renderer['module_name']
        if (not self.renderer):
            raise ClassInstantiationError(module_name=module_name, package_name=None, class_name=renderer['class_name'])
        self.api_key = api_key
        assert api_key, 'opsgenie_api_key missing in config_variables.yml'
        self.region = region
        self.priority = priority
        self.notify_on = notify_on

    def _run(self, validation_result_suite: ExpectationSuiteValidationResult, validation_result_suite_identifier: Union[(ValidationResultIdentifier, GeCloudIdentifier)], data_asset=None, payload=None, expectation_suite_identifier=None, checkpoint_identifier=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        logger.debug('OpsgenieAlertAction.run')
        if (validation_result_suite is None):
            logger.warning(f'No validation_result_suite was passed to {type(self).__name__} action. Skipping action.')
            return
        if (not isinstance(validation_result_suite_identifier, (ValidationResultIdentifier, GeCloudIdentifier))):
            raise TypeError('validation_result_suite_id must be of type ValidationResultIdentifier or GeCloudIdentifier, not {}'.format(type(validation_result_suite_identifier)))
        validation_success = validation_result_suite.success
        if ((self.notify_on == 'all') or ((self.notify_on == 'success') and validation_success) or ((self.notify_on == 'failure') and (not validation_success))):
            expectation_suite_name = validation_result_suite.meta.get('expectation_suite_name', '__no_expectation_suite_name__')
            settings = {'api_key': self.api_key, 'region': self.region, 'priority': self.priority}
            description = self.renderer.render(validation_result_suite, None, None)
            alert_result = send_opsgenie_alert(description, expectation_suite_name, settings)
            return {'opsgenie_alert_result': alert_result}
        else:
            return {'opsgenie_alert_result': ''}

class EmailAction(ValidationAction):
    '\n    EmailAction sends an email to a given list of email addresses.\n    **Configuration**\n    .. code-block:: yaml\n        - name: send_email_on_validation_result\n        action:\n          class_name: EmailAction\n          notify_on: all # possible values: "all", "failure", "success"\n          notify_with: # optional list of DataDocs site names to display in the email message. Defaults to showing all\n          renderer:\n            # the class that implements the message to be sent\n            # this is the default implementation, but you can\n            # implement a custom one\n            module_name: great_expectations.render.renderer.email_renderer\n            class_name: EmailRenderer\n          # put the actual following information in the uncommitted/config_variables.yml file\n          # or pass in as environment variable\n          smtp_address: ${smtp_address}\n          smtp_port: ${smtp_port}\n          sender_login: ${email_address}\n          sender_password: ${sender_password}\n          sender_alias: ${sender_alias} # useful to send an email as an alias (default = sender_login)\n          receiver_emails: ${receiver_emails} # string containing email addresses separated by commas\n          use_tls: False\n          use_ssl: True\n    '

    def __init__(self, data_context, renderer, smtp_address, smtp_port, sender_login, sender_password, receiver_emails, sender_alias=None, use_tls=None, use_ssl=None, notify_on='all', notify_with=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Construct an EmailAction\n        Args:\n            data_context:\n            renderer: dictionary specifying the renderer used to generate an email, for example:\n                {\n                   "module_name": "great_expectations.render.renderer.email_renderer",\n                   "class_name": "EmailRenderer",\n               }\n            smtp_address: address of the SMTP server used to send the email\n            smtp_address: port of the SMTP server used to send the email\n            sender_login: login used send the email\n            sender_password: password used to send the email\n            sender_alias: optional alias used to send the email (default = sender_login)\n            receiver_emails: email addresses that will be receive the email (separated by commas)\n            use_tls: optional use of TLS to send the email (using either TLS or SSL is highly recommended)\n            use_ssl: optional use of SSL to send the email (using either TLS or SSL is highly recommended)\n            notify_on: "all", "failure", "success" - specifies validation status that will trigger notification\n            notify_with: optional list of DataDocs site names to display in the email message\n        '
        super().__init__(data_context)
        self.renderer = instantiate_class_from_config(config=renderer, runtime_environment={}, config_defaults={})
        module_name = renderer['module_name']
        if (not self.renderer):
            raise ClassInstantiationError(module_name=module_name, package_name=None, class_name=renderer['class_name'])
        self.smtp_address = smtp_address
        self.smtp_port = smtp_port
        self.sender_login = sender_login
        self.sender_password = sender_password
        if (not sender_alias):
            self.sender_alias = sender_login
        else:
            self.sender_alias = sender_alias
        self.receiver_emails_list = list(map((lambda x: x.strip()), receiver_emails.split(',')))
        self.use_tls = use_tls
        self.use_ssl = use_ssl
        assert smtp_address, 'No SMTP server address found in action config.'
        assert smtp_port, 'No SMTP server port found in action config.'
        assert sender_login, 'No login found for sending the email in action config.'
        assert sender_password, 'No password found for sending the email in action config.'
        assert receiver_emails, 'No email addresses to send the email to in action config.'
        self.notify_on = notify_on
        self.notify_with = notify_with

    def _run(self, validation_result_suite: ExpectationSuiteValidationResult, validation_result_suite_identifier: Union[(ValidationResultIdentifier, GeCloudIdentifier)], data_asset=None, payload=None, expectation_suite_identifier=None, checkpoint_identifier=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        logger.debug('EmailAction.run')
        if (validation_result_suite is None):
            logger.warning(f'No validation_result_suite was passed to {type(self).__name__} action. Skipping action.')
            return
        if (not isinstance(validation_result_suite_identifier, (ValidationResultIdentifier, GeCloudIdentifier))):
            raise TypeError('validation_result_suite_id must be of type ValidationResultIdentifier or GeCloudIdentifier, not {}'.format(type(validation_result_suite_identifier)))
        validation_success = validation_result_suite.success
        data_docs_pages = None
        if payload:
            for action_names in payload.keys():
                if (payload[action_names]['class'] == 'UpdateDataDocsAction'):
                    data_docs_pages = payload[action_names]
        if ((self.notify_on == 'all') or ((self.notify_on == 'success') and validation_success) or ((self.notify_on == 'failure') and (not validation_success))):
            (title, html) = self.renderer.render(validation_result_suite, data_docs_pages, self.notify_with)
            email_result = send_email(title, html, self.smtp_address, self.smtp_port, self.sender_login, self.sender_password, self.sender_alias, self.receiver_emails_list, self.use_tls, self.use_ssl)
            return {'email_result': email_result}
        else:
            return {'email_result': ''}

class StoreValidationResultAction(ValidationAction):
    '\n        StoreValidationResultAction stores a validation result in the ValidationsStore.\n\n    **Configuration**\n\n    .. code-block:: yaml\n\n        - name: store_validation_result\n        action:\n          class_name: StoreValidationResultAction\n          # name of the store where the actions will store validation results\n          # the name must refer to a store that is configured in the great_expectations.yml file\n          target_store_name: validations_store\n\n    '

    def __init__(self, data_context, target_store_name=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n\n        :param data_context: Data Context\n        :param target_store_name: the name of the param_store in the Data Context which\n                should be used to param_store the validation result\n        '
        super().__init__(data_context)
        if (target_store_name is None):
            self.target_store = data_context.stores[data_context.validations_store_name]
        else:
            self.target_store = data_context.stores[target_store_name]

    def _run(self, validation_result_suite: ExpectationSuiteValidationResult, validation_result_suite_identifier: Union[(ValidationResultIdentifier, GeCloudIdentifier)], data_asset, payload=None, expectation_suite_identifier=None, checkpoint_identifier=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        logger.debug('StoreValidationResultAction.run')
        if (validation_result_suite is None):
            logger.warning(f'No validation_result_suite was passed to {type(self).__name__} action. Skipping action.')
            return
        if (not isinstance(validation_result_suite_identifier, (ValidationResultIdentifier, GeCloudIdentifier))):
            raise TypeError('validation_result_id must be of type ValidationResultIdentifier or GeCloudIdentifier, not {}'.format(type(validation_result_suite_identifier)))
        contract_ge_cloud_id = None
        if (self.data_context.ge_cloud_mode and checkpoint_identifier):
            contract_ge_cloud_id = checkpoint_identifier.ge_cloud_id
        expectation_suite_ge_cloud_id = None
        if (self.data_context.ge_cloud_mode and expectation_suite_identifier):
            expectation_suite_ge_cloud_id = str(expectation_suite_identifier.ge_cloud_id)
        return_val = self.target_store.set(validation_result_suite_identifier, validation_result_suite, contract_id=contract_ge_cloud_id, expectation_suite_id=expectation_suite_ge_cloud_id)
        if self.data_context.ge_cloud_mode:
            return_val: GeCloudResourceRef
            new_ge_cloud_id = return_val.ge_cloud_id
            validation_result_suite_identifier.ge_cloud_id = new_ge_cloud_id

class StoreEvaluationParametersAction(ValidationAction):
    '\n    StoreEvaluationParametersAction extracts evaluation parameters from a validation result and stores them in the store\n    configured for this action.\n\n    Evaluation parameters allow expectations to refer to statistics/metrics computed\n    in the process of validating other prior expectations.\n\n    **Configuration**\n\n    .. code-block:: yaml\n\n        - name: store_evaluation_params\n        action:\n          class_name: StoreEvaluationParametersAction\n          # name of the store where the action will store the parameters\n          # the name must refer to a store that is configured in the great_expectations.yml file\n          target_store_name: evaluation_parameter_store\n\n    '

    def __init__(self, data_context, target_store_name=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n\n        Args:\n            data_context: Data Context\n            target_store_name: the name of the store in the Data Context which\n                should be used to store the evaluation parameters\n        '
        super().__init__(data_context)
        if (target_store_name is None):
            self.target_store = data_context.evaluation_parameter_store
        else:
            self.target_store = data_context.stores[target_store_name]

    def _run(self, validation_result_suite: ExpectationSuiteValidationResult, validation_result_suite_identifier: Union[(ValidationResultIdentifier, GeCloudIdentifier)], data_asset, payload=None, expectation_suite_identifier=None, checkpoint_identifier=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        logger.debug('StoreEvaluationParametersAction.run')
        if (validation_result_suite is None):
            logger.warning(f'No validation_result_suite was passed to {type(self).__name__} action. Skipping action.')
            return
        if (not isinstance(validation_result_suite_identifier, (ValidationResultIdentifier, GeCloudIdentifier))):
            raise TypeError('validation_result_id must be of type ValidationResultIdentifier or GeCloudIdentifier, not {}'.format(type(validation_result_suite_identifier)))
        self.data_context.store_evaluation_parameters(validation_result_suite)

class StoreMetricsAction(ValidationAction):
    '\n    StoreMetricsAction extracts metrics from a Validation Result and stores them\n    in a metrics store.\n\n    **Configuration**\n\n    .. code-block:: yaml\n\n        - name: store_evaluation_params\n        action:\n          class_name: StoreMetricsAction\n          # name of the store where the action will store the metrics\n          # the name must refer to a store that is configured in the great_expectations.yml file\n          target_store_name: my_metrics_store\n\n    '

    def __init__(self, data_context, requested_metrics, target_store_name='metrics_store') -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n\n        Args:\n            data_context: Data Context\n            requested_metrics: dictionary of metrics to store. Dictionary should have the following structure:\n\n                expectation_suite_name:\n                    metric_name:\n                        - metric_kwargs_id\n\n                You may use "*" to denote that any expectation suite should match.\n            target_store_name: the name of the store in the Data Context which\n                should be used to store the metrics\n        '
        super().__init__(data_context)
        self._requested_metrics = requested_metrics
        self._target_store_name = target_store_name
        try:
            store = data_context.stores[target_store_name]
        except KeyError:
            raise DataContextError('Unable to find store {} in your DataContext configuration.'.format(target_store_name))
        if (not isinstance(store, MetricStore)):
            raise DataContextError('StoreMetricsAction must have a valid MetricsStore for its target store.')

    def _run(self, validation_result_suite: ExpectationSuiteValidationResult, validation_result_suite_identifier: Union[(ValidationResultIdentifier, GeCloudIdentifier)], data_asset, payload=None, expectation_suite_identifier=None, checkpoint_identifier=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        logger.debug('StoreMetricsAction.run')
        if (validation_result_suite is None):
            logger.warning(f'No validation_result_suite was passed to {type(self).__name__} action. Skipping action.')
            return
        if (not isinstance(validation_result_suite_identifier, (ValidationResultIdentifier, GeCloudIdentifier))):
            raise TypeError('validation_result_id must be of type ValidationResultIdentifier or GeCloudIdentifier, not {}'.format(type(validation_result_suite_identifier)))
        self.data_context.store_validation_result_metrics(self._requested_metrics, validation_result_suite, self._target_store_name)

class UpdateDataDocsAction(ValidationAction):
    '\n    UpdateDataDocsAction is a validation action that\n    notifies the site builders of all the data docs sites of the Data Context\n    that a validation result should be added to the data docs.\n\n    **Configuration**\n\n    .. code-block:: yaml\n\n        - name: update_data_docs\n        action:\n          class_name: UpdateDataDocsAction\n\n    You can also instruct ``UpdateDataDocsAction`` to build only certain sites by providing a ``site_names`` key with a\n    list of sites to update:\n\n        - name: update_data_docs\n        action:\n          class_name: UpdateDataDocsAction\n          site_names:\n            - production_site\n\n    '

    def __init__(self, data_context, site_names=None, target_site_names=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        :param data_context: Data Context\n        :param site_names: *optional* List of site names for building data docs\n        '
        super().__init__(data_context)
        if target_site_names:
            warnings.warn('target_site_names is deprecated as of v0.10.10 and will be removed in v0.16. Please use site_names instead.', DeprecationWarning)
            if site_names:
                raise DataContextError('Invalid configuration: legacy key target_site_names and site_names key are both present in UpdateDataDocsAction configuration')
            site_names = target_site_names
        self._site_names = site_names

    def _run(self, validation_result_suite: ExpectationSuiteValidationResult, validation_result_suite_identifier: Union[(ValidationResultIdentifier, GeCloudIdentifier)], data_asset, payload=None, expectation_suite_identifier=None, checkpoint_identifier=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        logger.debug('UpdateDataDocsAction.run')
        if (validation_result_suite is None):
            logger.warning(f'No validation_result_suite was passed to {type(self).__name__} action. Skipping action.')
            return
        if (not isinstance(validation_result_suite_identifier, (ValidationResultIdentifier, GeCloudIdentifier))):
            raise TypeError('validation_result_id must be of type ValidationResultIdentifier or GeCloudIdentifier, not {}'.format(type(validation_result_suite_identifier)))
        self.data_context.build_data_docs(site_names=self._site_names, resource_identifiers=[validation_result_suite_identifier, expectation_suite_identifier])
        data_docs_validation_results = {}
        if self.data_context.ge_cloud_mode:
            return data_docs_validation_results
        docs_site_urls_list = self.data_context.get_docs_sites_urls(resource_identifier=validation_result_suite_identifier, site_names=self._site_names)
        for sites in docs_site_urls_list:
            data_docs_validation_results[sites['site_name']] = sites['site_url']
        return data_docs_validation_results

class CloudNotificationAction(ValidationAction):
    '\n    CloudNotificationAction is an action which utilizes the Cloud store backend\n    to deliver user-specified Notification Actions.\n    '

    def __init__(self, data_context: 'DataContext', checkpoint_ge_cloud_id: str) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__(data_context)
        self.checkpoint_ge_cloud_id = checkpoint_ge_cloud_id

    def _run(self, validation_result_suite: ExpectationSuiteValidationResult, validation_result_suite_identifier: GeCloudIdentifier, data_asset=None, payload: Optional[Dict]=None, expectation_suite_identifier=None, checkpoint_identifier=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        logger.debug('CloudNotificationAction.run')
        if (validation_result_suite is None):
            logger.warning(f'No validation_result_suite was passed to {type(self).__name__} action. Skipping action. ')
        if (not self.data_context.ge_cloud_mode):
            return Exception('CloudNotificationActions can only be used in GE Cloud Mode.')
        if (not isinstance(validation_result_suite_identifier, GeCloudIdentifier)):
            raise TypeError('validation_result_id must be of type GeCloudIdentifier, not {}'.format(type(validation_result_suite_identifier)))
        ge_cloud_url = urljoin(self.data_context.ge_cloud_config.base_url, f'/organizations/{self.data_context.ge_cloud_config.organization_id}/contracts/{self.checkpoint_ge_cloud_id}/suite-validation-results/{validation_result_suite_identifier.ge_cloud_id}/notification-actions')
        auth_headers = {'Content-Type': 'application/vnd.api+json', 'Authorization': f'Bearer {self.data_context.ge_cloud_config.access_token}'}
        return send_cloud_notification(url=ge_cloud_url, headers=auth_headers)

class SNSNotificationAction(ValidationAction):
    '\n    Action that pushes validations results to an SNS topic with a subject of passed or failed.\n    '

    def __init__(self, data_context: 'DataContext', sns_topic_arn: str, sns_message_subject) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__(data_context)
        self.sns_topic_arn = sns_topic_arn
        self.sns_message_subject = sns_message_subject

    def _run(self, validation_result_suite: ExpectationSuiteValidationResult, validation_result_suite_identifier: ValidationResultIdentifier, expectation_suite_identifier=None, checkpoint_identifier=None, data_asset=None, **kwargs) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        logger.debug('SNSNotificationAction.run')
        if (validation_result_suite is None):
            logger.warning(f'No validation_result_suite was passed to {type(self).__name__} action. Skipping action. ')
        if (self.sns_message_subject is None):
            logger.warning(f'No message subject was passed checking for expectation_suite_name')
            if (expectation_suite_identifier is None):
                subject = validation_result_suite_identifier.run_id
                logger.warning(f'No expectation_suite_identifier was passed. Defaulting to validation run_id: {subject}.')
            else:
                subject = expectation_suite_identifier.expectation_suite_name
                logger.info(f'Using expectation_suite_name: {subject}')
        else:
            subject = self.sns_message_subject
        return send_sns_notification(self.sns_topic_arn, subject, validation_result_suite.__str__(), **kwargs)
