.. _how_to_guides__validation__how_to_trigger_slack_notifications_as_a_validation_action:

How to trigger Slack notifications as a Validation Action
=========================================================

This guide will help you trigger Slack notifications as a :ref:`Validation Action <validation_actions>`
.  It will allow you to send a Slack message including information about a Validation Result, including whether or not the Validation succeeded.

.. admonition:: Prerequisites: This how-to guide assumes that you have already:

    - Configured a Slack app with the Webhook function enabled (See Additional Resources below for more information on setting up a new Slack app).
    - Obtained the Webhook address for your Slack app.
    - Identified the Slack channel that messages will be sent to.

Steps
-----

1. Open ``uncommitted/config_variables.yml`` file and add ``validation_notification_slack_webhook`` variable by adding the following line:

.. code-block:: yaml

    validation_notification_slack_webhook: [address to web hook]


2. Open ``great_expectations.yml`` and add ``send_slack_notification_on_validation_result`` action to ``validation_operators``. Make sure the following section exists in the ``great_expectations.yml`` file.

.. code-block:: yaml

    validation_operators:
        action_list_operator:
            # To learn how to configure sending Slack notifications during evaluation
            # (and other customizations), read: https://docs.greatexpectations.io/en/latest/reference/validation_operators/action_list_validation_operator.html
            class_name: ActionListValidationOperator
            action_list:
            #--------------------------------
            # here is what you will be adding
            #--------------------------------
            - name: send_slack_notification_on_validation_result # name can be set to any value
              action:
                class_name: SlackNotificationAction
                # put the actual webhook URL in the uncommitted/config_variables.yml file
                slack_webhook: ${validation_notification_slack_webhook}
                notify_on: all # possible values: "all", "failure", "success"
                notify_with: # optional list containing the DataDocs sites to include in the notification. Defaults to including links to all configured sites.
                renderer:
                  module_name: great_expectations.render.renderer.slack_renderer
                  class_name: SlackRenderer

3. Run your ``action_list_operator``, to validate a batch of data and receive Slack notification on the success or failure of validation suite.  

  .. code-block:: python
  
      context.run_validation_operator('action_list_operator', assets_to_validate=batch, run_name="slack_test")

  If successful, you should receive a Slack message that looks like this:

    .. image:: /images/slack_notification_example.png


Additional notes
--------------------

- If your ``great_expectations.yml`` contains multiple configurations for Data Docs sites, all of them will be included in the Slack notification by default. If you would like to be more specific, you can configure the ``notify_with`` variable in your ``great_expectations.yml``.
- The following example will configure the Slack message to include links Data Docs at ``local_site`` and ``s3_site``.

.. code-block:: yaml

    # Example data_docs_sites configuration
    data_docs_sites:
      local_site:
        class_name: SiteBuilder
        show_how_to_buttons: true
        store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: uncommitted/data_docs/local_site/
        site_index_builder:
          class_name: DefaultSiteIndexBuilder
      s3_site:  # this is a user-selected name - you may select your own
        class_name: SiteBuilder
        store_backend:
          class_name: TupleS3StoreBackend
          bucket: data-docs.my_org  # UPDATE the bucket name here to match the bucket you configured above.
        site_index_builder:
          class_name: DefaultSiteIndexBuilder
          show_cta_footer: true

    validation_operators:
        action_list_operator:
        ...
        - name: send_slack_notification_on_validation_result # name can be set to any value
              action:
                class_name: SlackNotificationAction
                # put the actual webhook URL in the uncommitted/config_variables.yml file
                slack_webhook: ${validation_notification_slack_webhook}
                notify_on: all # possible values: "all", "failure", "success"
                #--------------------------------
                # This is what was configured
                #--------------------------------
                notify_with:
                  - local_site
                  - s3_site
                renderer:
                  module_name: great_expectations.render.renderer.slack_renderer
                  class_name: SlackRenderer


Additional resources
--------------------

- Instructions on how to set up a Slack app with webhook can be found in the documentation for the `Slack API <https://api.slack.com/messaging/webhooks#>`_

.. discourse::
    :topic_identifier: 228
