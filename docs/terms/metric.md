---
id: metric
title: Metric
hoverText: A computed attribute of data such as the mean of a column.
---
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';

<UniversalMap setup='active' connect='active' create='active' validate='active'/>

A Metric is a computed attribute of data such as the mean of a column.


NOTES: Everything under here is temporary, for use in writing the detailed page.
----------
Some attribute of data, such as the mean of a column. Metrics are generated and stored as part of running Expectations against a Batch (and can be referenced as such). For example, if you have an Expectation that the mean of a column fall within a certain range, the mean of the column must first be computed to see if its value is expected.