---
title: Data Contexts
id: gx-overview-data-context
displayed_sidebar: docs
pagination_prev: conceptual_guides/gx_overview/gx-overview-lp
pagination_next: conceptual_guides/gx_overview/gx-overview-datasources
---

A GX project is defined by the configurations in a Data Context.  It contains the configurations for Stores, which contain the metadata information used by GX, as well as the settings for where those configurations reside.  

In Python, the Data Context object also serves as your entry point for the GX API, providing convenience methods to further configure and interact with GX.

The following are the available Data Context types:
- Ephemeral Data Context: Exists in memory, and does not persist beyond the current Python session.
- Filesystem Data Context: Exists as a folder and configuration files.  Its contents persist between Python sessions.
- Cloud Data Context: Supports persistence between Python sessions, but additionally serves as the entry point for Great Expectations Cloud.

[//]: # (TODO: Additional Reading: Data Contexts LP)

## Stores and metadata

Stores are connectors to the metadata GX uses, such as:
- The Datasource and Data Asset configurations that tell GX how to connect to your Source Data System.
- The Expectations you have specified about your Source Data.
- The Metrics that are recorded when GX validates certain Expectations against your Source Data.
- The Checkpoints you have configured for validating your Expectations.
- The Validation Results that GX returns when you run a Checkpoint.
- Credentials, which can be stored in a special file that GX references for string substitutions in other configurations.  Credentials can also be stored as environment variables outside of GX.

By default, Stores exist in the same folder hierarchy as your Data Context (or in memory, if you are using an Ephemeral Data Context).  However, you can update these settings if you want to change where your Store configurations exist, such as hosting them in a shared cloud environment like AWS.

[//]: # (TODO: Additional reading: Configuring metadata Stores LP)

## The GX Python API

An instantiated Data Context object in Python gives you access to the GX API.  The Data Context object manages classes, limits the number of objects you need to directly manage, and provides a number of convenience methods to help streamline GX workflows.

[//]: # (TODO: Reference material: The GX Python API LP)