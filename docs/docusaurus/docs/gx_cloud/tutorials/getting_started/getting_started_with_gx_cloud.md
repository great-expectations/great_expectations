---
title: Getting started with Great Expectations Cloud
tag: [tutorial, getting started, cloud, onboarding]
description: This tutorial will help you onboard with GX Cloud and get ready to connect to your data.
keywords: [tutorial, getting started, cloud, onboarding]
---

import Prerequisites from '/docs/guides/connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/\_tag.mdx';

Welcome to Great Expectations Cloud! This tutorial will help you onboard with GX Cloud and get ready to connect to your data.

## Prerequisites

<Prerequisites>

- This tutorial assumes you have Great Expectations OSS installed on your machine. If that's not the case please complete [Step 1: OSS Setup](/docs/tutorials/getting_started/tutorial_setup.md) first.

</Prerequisites>

## Steps

### Step 1: Login

- First of all, you'll need to go to [https://app.greatexpectations.io](https://app.greatexpectations.io)
- You should have received GX Cloud invitation email. Follow the link in the email to set the password.
- Login into the app with your email and password.

### Step 2: Generate User Token

- Go to [“Settings” > “Users”](https://app.greatexpectations.io/users) in the navigation panel. This is the screen where you can invite your mates into the app.
- Go to [“Settings” > “Tokens”](https://app.greatexpectations.io/tokens) in the navigation panel. In this tutorial, we’ll create a User Token, but GX Cloud also supports Organization tokens, e.g. for use in shared execution environments. These tokens are see-once and stored as a hash in Great Expectation Cloud's backend database. Once a user copies their API key, the Cloud UI will never show the token value again.

### Step 3: Set tokens & <TechnicalTag tag="data_context" text="Data Context"/>

- Open Jupyter Notebook

:::tip Any Python Interpreter or script file will work for the remaining steps in the guide, we recommend using a Jupyter Notebook, since they are included in the OSS GX installation and give the best experience of both composing a script file and running code in a live interpreter.
:::

- Set environment variables in the notebook (alternatively, add these as <TechnicalTag tag="data_context" text="Data Context"/> [config variables](/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials))

```console
import os

os.environ["GX_CLOUD_ORGANIZATION_ID"] = "<organization_id_from_the_app>"
os.environ["GX_CLOUD_ACCESS_TOKEN"] = "<user_token_you_just_generated_in_the_app>"
```

- Set Cloud data context in the notebook

```console
import great_expectations as gx

context = gx.get_context()
```

## Next Steps

Follow in-app snippets to create a <TechnicalTag tag="datasource" text="Datasource"/>, define an <TechnicalTag tag="expectation_suite" text="Expectation Suite"/>, configure and run a <TechnicalTag tag="checkpoint" text="Checkpoint"/> and view <TechnicalTag tag="validation_result" text="Validation Results"/>.
