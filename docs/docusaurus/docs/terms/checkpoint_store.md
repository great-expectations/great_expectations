---
title: "Checkpoint Store"
---

import TechnicalTag from '../term_tags/_tag.mdx';

A Checkpoint Store is a connector to store and retrieve information about means for validating data in a production deployment of Great Expectations.

The Checkpoint Store manages storage and retrieval of <TechnicalTag relative="../" tag="checkpoint" text="Checkpoint" /> configurations for the <TechnicalTag relative="../" tag="data_context" text="Data Context" />.  Checkpoint configurations can be added through the Data Context's `add_checkpoint()` method and retrieved with its `get_checkpoint` method. A configured Checkpoint Store is not required in order to work with Great Expectations, however a local configuration for a Checkpoint Store will be added automatically to `great_expectations.yml` when you store a Checkpoint configuration for the first time.

When working with Great Expectations to <TechnicalTag relative="../" tag="validation" text="Validate" /> data you will not need to interact with a Checkpoint Store directly outside configuring the <TechnicalTag relative="../" tag="store" text="Store" />.  Instead, your Data Context will use the Checkpoint Store to store and retrieve Checkpoints behind the scenes, and the objects you will directly work with will be those Checkpoints.

## Relationship to other objects

The Data Context will use your Checkpoint Store to store Checkpoint configurations and retrieve those configurations when initializing Checkpoints.  Typically, the Checkpoint Store will not need to interact with anything other than the Data Context and the configuration information being stored.

## Use cases

When you save your first Checkpoint, a Checkpoint Store configuration will automatically be added to `great_expectations.yml`.  If you wish, you can adjust this configuration but in most cases the default will suffice.  Whenever you request an existing Checkpoint from your Data Context it will use the Checkpoint Store behind the scenes to retrieve that Checkpoint's configuration and initialize it.  Likewise, when you instruct your Data Context to store a newly created or edited Checkpoint it will use the Checkpoint Store behind the scenes to store that Checkpoint's configuration.

## Access

Your Data Context will handle accessing your Checkpoint Store behind the scenes when you use it to store or retrieve a Checkpoint's configuration.  Rather than interacting with the Checkpoint Store itself, you will generally be interacting with your Data Context and a Checkpoint.
