---
id: evaluation_parameter_store
title: Evaluation Parameter Store
hoverText: A connector to store and retrieve information about parameters used during Validation of an Expectation which reference simple expressions or previously generated metrics.
---
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';

<UniversalMap setup='active' connect='active' create='active' validate='active'/>

An Evaluation Parameter Store is a connector to store and retrieve information about parameters used during Validation of an Expectation which reference simple expressions or previously generated metrics.


NOTES: Everything under here is temporary, for use in writing the detailed page.
----------

- **Evaluation Parameter Stores**, also known as **Metric Stores**, are an alternative way to store Metrics without the accompanying Validation Results.  The process of storing information in an Evaluation Parameter Store is also typically executed bby an Action in a Checkpoint's `action_list`.  Metrics stored in Evaluation Parameter Stores are also available as Evaluation Parameters when defining Expectations.