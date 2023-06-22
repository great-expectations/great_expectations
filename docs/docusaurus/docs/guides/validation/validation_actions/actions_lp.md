---
sidebar_label: 'Configure Actions'
title: 'Configure Actions'
id: actions_lp
description: Configure Actions to send Validation Result notifications, update Data Docs, and store Validation Results.
---

import LinkCardGrid from '/docs/components/LinkCardGrid';
import LinkCard from '/docs/components/LinkCard';

<p class="DocItem__header-description">This is where you'll find information about using Actions to send Validation Result notifications, update Data Docs, and store Validation Results.</p>

<LinkCardGrid>
  <LinkCard topIcon label="Trigger Email as an Action" description="Create an Action that sends an email with Validation Result information, including Validation success or failure" href="/docs/guides/validation/validation_actions/how_to_trigger_email_as_a_validation_action" icon="/img/email_action_icon.svg" />
  <LinkCard topIcon label="Collect OpenLineage metadata" description="Use an Action to emit results to an OpenLineage backend" href="/docs/guides/validation/validation_actions/how_to_collect_openlineage_metadata_using_a_validation_action" icon="/img/metadata_icon.svg" />
  <LinkCard topIcon label="Trigger Opsgenie notifications" description="Use an Action to create Opsgenie alert notifications" href="/docs/guides/validation/validation_actions/how_to_trigger_opsgenie_notifications_as_a_validation_action" icon="/img/opsgenie_icon.svg" />
  <LinkCard topIcon label="Trigger Slack notifications" description="Use an Action to create Slack notifications for Validation Results" href="/docs/guides/validation/validation_actions/how_to_trigger_slack_notifications_as_a_validation_action" icon="/img/slack_icon.svg" />
  <LinkCard topIcon label="Update Data Docs" description="Use an Action to update Data Docs sites with new Validation Results" href="/docs/guides/validation/validation_actions/how_to_update_data_docs_as_a_validation_action" icon="/img/update_data_docs_icon.svg" />
</LinkCardGrid>