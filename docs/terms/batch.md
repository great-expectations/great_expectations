---
id: batch
title: Batch
hoverText: A selection of records from a Data Asset.
---
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import CreateHeader from '/docs/images/universal_map/_um_create_header.mdx';
import ValidateHeader from '/docs/images/universal_map/_um_validate_header.mdx';

<UniversalMap setup='inactive' connect='inactive' create='active' validate='active'/>

Batches are selections of records from a Data Asset.  For example, it could include records in a SQL query that were created in the last 24 hours, or files added to a directory over the last month.

## Use Cases

<CreateHeader/>

When Profilers create Expectations automatically they require a Batch to analyze.  You can also analyze multiple Batches with a Multi-Batch Profiler.

<ValidateHeader/>

The data that you will Validate is defined by a Batch which you will then pass to a Checkpoint or Validator.

## How to define a Batch


## Example Batch configurations
