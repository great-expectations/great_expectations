---
title: Use Great Expectations in Flyte
description: "Use Great Expectations with Flyte"
sidebar_label: "Flyte"
sidebar_custom_props: { icon: 'img/integrations/flyte_icon.png' }
---
[Flyte](https://flyte.org/) is a structured programming and distributed processing platform that enables highly concurrent, scalable, and maintainable workflows for Machine Learning and Data Processing. It is a fabric that connects disparate computation backends using a type-safe data dependency graph. It records all changes to a pipeline, making it possible to rewind time. It also stores a history of all executions and provides an intuitive UI, CLI, and REST/gRPC API to interact with the computation.

The power of data validation in Great Expectations can be integrated with Flyte to validate the data moving in and out of the pipeline entities you may have defined in Flyte. This helps establish stricter boundaries around your data to ensure that everything works as expected and data does not crash your pipelines unexpectedly!

:::info 

The most recent version of GX supported by this guide is GX 0.15.50.

To use Great Expectations (GX) in Flyte, see [How to use GX in Flyte](/docs/0.15.50/deployment_patterns/how_to_use_great_expectations_in_flyte) in version 0.15.50 of the documentation.

For more information about implementing and using Flyte, see the [Flyte site](https://flyte.org/).

:::