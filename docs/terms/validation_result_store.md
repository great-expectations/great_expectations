---
id: validation_result_store
title: Validation Result Store
hoverText: A connector to store and retrieve information about objects generated when data is Validated against an Expectation Suite.
---
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';

<UniversalMap setup='active' connect='active' create='active' validate='active'/>

A Validation Result Store is a connector to store and retrieve information about objects generated when data is Validated against an Expectation Suite.


NOTES: Everything under here is temporary, for use in writing the detailed page.
----------


- **Validation Result Stores** can be used to store and retrieve Validation Results and associated metadata.  The process of storing this information is typically executed by an Action in a Checkpoint's `action_list`.  Metrics found in the Validation Results stored in Validation Result Stores can also be accessed as Evaluation Parameters when defining Expectations.