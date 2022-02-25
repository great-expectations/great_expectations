---
id: metric_store
title: Metric Store
hoverText: A connector to store and retrieve information about computed attributes of data, such as the mean of a column.
---

import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import TechnicalTag from '../term_tags/_tag.mdx';
import SetupHeader from '/docs/images/universal_map/_um_setup_header.mdx'
import CreateHeader from '/docs/images/universal_map/_um_create_header.mdx';
import ValidateHeader from '/docs/images/universal_map/_um_validate_header.mdx';


<UniversalMap setup='active' connect='active' create='active' validate='active'/> 

## Overview

### Definition

A Metric Store is a connector to store and retrieve information about computed attributes of data, such as the mean of a column.

### Features and promises

The Metric Store differs from an Evaluation Parameter Store in how it formats its data.  Information stored in a Metric Store is kept in a format that more easily converted into tables which can be used for reports or to analyze trends.  It can also be referenced as values for Evaluation Parameters.  

### Relationship to other objects

A Metric Store can be referenced by an Expectation Suite to populate values for Evaluation Parameters used by Expectations within that suite.  Metric Stores are also used in Checkpoints to store Metrics that are included in the Validation Results which are passed to the Checkpoints' `action_list`.

## Use cases

<SetupHeader/>

If you intend to use a Metric Store, you can configure it in your `great_expectations.yml` file when you configure other Stores.  A Metric Store is an optional addition to `great_expectations.yml`, and one will not be included by default when you first initialize your Data Context.

For more information, please see [our guide on how to configure a Metric Store](../guides/setup/configuring_metadata_stores/how_to_configure_a_metricsstore.md).


<CreateHeader/>

When creating Expectations, you can configure them to use your Metric Store to retrieve values for Evaluation Parameters.

<ValidateHeader/>

When a Checkpoint is run, it will use a Metric Store to populate values for Evaluation Parameters if the Expectation Suite being run is configured to do so.  The `StoreMetricsAction` Action can also be included in a Checkpoint's `action_list`.  This will cause the Checkpoint to use the Metric Store and store any Metrics that were included in the Validation Results passed to the `action_list`.

## Features

The Metric Store is tailored for storing and retrieving Metrics.  Although can be used within Great Expectations as a reference to populate Evaluation Parameters when an Expectation Suite is run, the fact that it doesn't include other information found in Validation Results means you can also use it to more easily examine trends in your Metrics over time.  To help with this, a Metric Store will track the `run_id` of a Validation and the Expectation Suite name in addition to the metric name and metric kwargs.

## API basics

For information on how to create and use a Metric Store, please see our guide on [how to configure a Metric Store](../guides/setup/configuring_metadata_stores/how_to_configure_a_metricsstore.md).


