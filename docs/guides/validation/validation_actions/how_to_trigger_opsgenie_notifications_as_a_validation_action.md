---
title: How to trigger Opsgenie notifications as a Validation Action
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'

This guide will help you setup Opsgenie alert notifications when running Great Expectations. This is useful as it can provide alerting when Great Expectations is run, or certain expectations begin failing (or passing!).

<Prerequisites>

  - [Set up a working deployment of Great Expectations](../../../tutorials/getting_started/intro.md)
  - You already have an Opsgenie account

</Prerequisites>

Steps
-----

1. First, setup a new API integration within Opsgenie.

    - Navigate to Settings > Integration list within Opsgenie using the sidebar menu.

    ![/images/opsgenie_integration_list.png](../../../images/opsgenie_integration_list.png)

    - Select add on the 'API' integration, this will generally be the first available option.
    - Name the integration something meaningful such as 'Great Expectations'
    - Assign the alerts to any relevant team.
    - Click the copy icon next to the API Key - you'll need this for the next step.
    - Add any required responders.
    - Ensure 'Create and Update Access' is checked along with the 'Enabled' checkbox.
    - Click 'Save Integration' to save the newly created integration.

2. Using the API Key you copied from Step 1, update your Great Expectations configuration variables in your config_variables.yml file

    ```yaml
    opsgenie_api_key: YOUR-API-KEY
    ```

3. Next, update your Great Expectations configuration file to add a new operator to the actions list in great_expectations.yml

    ```yaml
    validation_operators:
      action_list_operator:
        class_name: ActionListValidationOperator
        action_list:
        - name: send_opsgenie_alert_on_validation_result
          action:
            class_name: OpsgenieAlertAction
            notify_on: all
            api_key: ${opsgenie_api_key}
            priority: P3
            renderer:
              module_name: great_expectations.render.renderer.opsgenie_renderer
              class_name: OpsgenieRenderer
    ```

    - Set notify_on to one of, "all", "failure", or "success"
    - Optionally set a priority (from P1 - P5, defaults to P3)
    - Set region: eu if you are using the European Opsgenie endpoint

4. That's it. Start running your expectations!

