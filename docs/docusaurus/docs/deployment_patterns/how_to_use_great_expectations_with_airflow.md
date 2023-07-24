---
title: How to Use Great Expectations with Airflow
sidebar_label: "Airflow"
description: "Run a Great Expectations checkpoint in Apache Airflow"
id: how_to_use_great_expectations_with_airflow
sidebar_custom_props: { icon: 'img/integrations/airflow_icon.png' }
---

Airflow is a data orchestration tool for creating and maintaining data pipelines through DAGs written in Python. DAGs complete work through operators, which are templates that encapsulate a specific type of work. This document explains how to use the `GreatExpectationsOperator` to perform data quality work in an Airflow DAG.

:::info 

Consult Astronomer's [Orchestrate Great Expectations with Airflow](https://docs.astronomer.io/learn/airflow-great-expectations) guide for more information on how to set up and configure the `GreatExpectationsOperator` in an Airflow DAG.

For more information about implementing and using Airflow, see [Astronomer's documentation](https://docs.astronomer.io/).

:::