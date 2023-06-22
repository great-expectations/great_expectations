---
title: How to trigger Slack notifications as an Action
---
import Prerequisites from '../../../guides/connecting_to_your_data/components/prerequisites.jsx';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you trigger Slack notifications as an <TechnicalTag tag="action" text="Action" />.
It will allow you to send a Slack message including information about a <TechnicalTag tag="validation_result" text="Validation Result" />, including whether or not the <TechnicalTag tag="validation" text="Validation" /> succeeded.

Great Expectations is able to use a Slack webhook or Slack app to send notifications.

## Prerequisites 

<Tabs
  groupId="webhook-or-app"
  defaultValue='webhook'
  values={[
  {label: 'For Webhook', value:'webhook'},
  {label: 'For App', value:'app'},
  ]}>

<TabItem value="webhook">

<Prerequisites>

- A Slack app with the Webhook function enabled. See [Sending messages using Incoming Webhooks](https://api.slack.com/messaging/webhooks#).
- A Webhook address for your Slack app.
- A Slack channel to send messages to.
- A Checkpoint configured to send the notification.

</Prerequisites>

</TabItem>

<TabItem value="app">

<Prerequisites>

- A Slack app with a Bot Token. See [Sending messages using Incoming Webhooks](https://api.slack.com/messaging/webhooks#).
- A Bot Token for your Slack app.
- A Slack channel to send messages to.
- A Checkpoint configured to send the notification.

</Prerequisites>

</TabItem>

</Tabs>

## Steps

### 1. Edit your configuration variables to include the Slack webhook

Open `uncommitted/config_variables.yml` file and add `validation_notification_slack_webhook` variable by adding the following line:

```yaml
validation_notification_slack_webhook: [address to web hook]
```

### 2. Include the `send_slack_notification_on_validation_result` Action in your Checkpoint configuration

Open the `.yml` configuration file in `great_expectations/checkpoints` that corresponds to the <TechnicalTag tag="checkpoint" text="Checkpoint" /> you want to add Slack notifications to.  

Add the `send_slack_notification_on_validation_result` Action to the `action_list` section of the configuration. Make sure the following section exists in the Checkpoint configuration.

<Tabs
  groupId="webhook-or-app2"
  defaultValue='webhook'
  values={[
  {label: 'For Webhook', value:'webhook'},
  {label: 'For App', value:'app'},
  ]}>

<TabItem value="webhook">

#### Webhook config

```yaml
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
```

</TabItem>

<TabItem value="app">

#### Slack bot config

```yaml
action_list:
    #--------------------------------
    # here is what you will be adding
    #--------------------------------
    - name: send_slack_notification_on_validation_result # name can be set to any value
      action:
        class_name: SlackNotificationAction
        # put the actual bot token in the uncommitted/config_variables.yml file
        slack_token: {bot_token}
        slack_channel: <channel-name>
        notify_on: all # possible values: "all", "failure", "success"
        notify_with: # optional list containing the DataDocs sites to include in the notification. Defaults to including links to all configured sites.
        renderer:
          module_name: great_expectations.render.renderer.slack_renderer
          class_name: SlackRenderer
```

</TabItem>

</Tabs>

### 3. Test your Slack notifications

Run your Checkpoint to Validate a <TechnicalTag tag="batch" text="Batch" /> of data and receive Slack notification on the success or failure of the <TechnicalTag tag="expectation_suite" text="Expectation Suite's" /> Validation.  

:::note Reminder
Our [guide on how to Validate data by running a Checkpoint](../how_to_validate_data_by_running_a_checkpoint.md) has instructions for this step.
:::

  If successful, you should receive a Slack message that looks like this:

![slack_notification_example](../../../images/slack_notification_example.png)


## Additional notes

- If your `great_expectations.yml` contains multiple configurations for <TechnicalTag tag="data_docs" text="Data Docs" /> sites, all of them will be included in the Slack notification by default. If you would like to be more specific, you can configure the `notify_with` variable in your Checkpoint configuration.
- The following example will configure the Slack message to include links Data Docs at `local_site` and `s3_site`.

```yaml
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
```

```yaml
    # Example action_list in Checkpoint configuration   
    action_list:
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
```


## Additional resources

- To set up a Slack app with a webhook, see [Sending messages using Incoming Webhooks](https://api.slack.com/messaging/webhooks#).
