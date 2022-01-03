---
id: data_asset
title: Data Asset
hoverText: A collection of records within a Datasource which is usually named based on the underlying data system and sliced to correspond to a desired specification.
---
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';

<UniversalMap setup='active' connect='active' create='active' validate='active'/>

A Data Asset is a collection of records within a Datasource which is usually named based on the underlying data system and sliced to correspond to a desired specification.


## NOTES: Temporary

### https://docs.google.com/document/d/1oyKFMqo6I6yL3OAy_y-1zn26fsiIzfpXe0uMxKIJmlc/edit
Data assets are usually tied to existing data that already has a name (e.g. “the UserEvents table”). In many cases, Data Assets slice the data one step further (e.g. “new records for each day within the UserEvents table.”)
A collection of records that we care about. For example, in a SQL database, rows from a table grouped by the week they were delivered may be a data asset; in an S3 bucket or filesystem, files matching a particular regex pattern may be a data asset. Data Assets Batches are created from Data Assets, and Data Assets . In the first example of SQL records grouped by week, a Batch could be the records for a given week (created by adding an additional WHERE clause to the query); in the second example, it could be a further refinement of the regex (or a subsequent pattern that is applied) to filter down to a particular data delivery.
