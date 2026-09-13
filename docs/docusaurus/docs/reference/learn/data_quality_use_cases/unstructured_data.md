---
sidebar_label: 'Unstructured data'
title: 'Validate unstructured data'
description: 'Learn how to validate metadata from scanned PDFs using GX Core.'
---

Enterprise data often consists of large amounts of unstructured data such as PDFs, images, emails, and sensor logs, but many often find it difficult to validate the quality of it. Data quality issues related to unstructured data can often go unnoticed, leading to downstream problems. For example, an AI model may be compromised if duplicate documents and failed OCR (Optical Character Recognition) do not get immediately flagged, leading to poor outputs.

This tutorial provides a working, hands-on example of how to validate unstructured data using sample PDF data and GX Core. An OCR process on a PDF doesn't just extract text; it produces metadata like confidence scores, and word counts. GX Core allows you to set up data quality checks on this metadata to maximize the confidence in your unstructured data.

## Prerequisite knowledge

This article assumes basic familiarity with GX components and workflows. If you're new to GX, start with the [GX Core overview](/core/introduction/gx_overview.md) to familiarize yourself with key concepts and setup procedures.

## Prerequisites

- [Python version 3.10 to 3.13](https://www.python.org/downloads/)

## Install dependencies

1. Open a terminal window and navigate to the folder you want to use for this tutorial.

2. Install [poppler](https://poppler.freedesktop.org/) and [tesseract](https://github.com/tesseract-ocr/tesseract). Poppler is a PDF rendering library that this tutorial uses to read the PDFs. Tesseract is an open source OCR engine that this tutorial uses to perform OCR on the PDFs.

   ```bash title="Terminal input"
   brew install poppler
   brew install tesseract
   ```

3. Optional. Create a Python virtual environment and start it.

   ```bash title="Terminal input"
   python -m venv my_venv
   source my_venv/bin/activate
   ```

4. Install the Python libraries that you will use in this tutorial, including the Great Expectations library.

   ```bash title="Terminal input"
   pip install pandas
   pip install datasets
   pip install pdf2image
   pip install pytesseract
   pip install great_expectations
   ```

## Import the required Python libraries

1. Create the Python file for this project.

   ```bash title="Terminal input"
   touch gx_unstructured_data.py
   ```

2. Open the Python file in your code editor of choice. 

3. Import the libraries you will be using for data validation in this tutorial.

   ```python title="Python" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/unstructured_data/unstructured_data.py - import the libraries"
   ```

## Load the dataset and convert it into a dataframe

This tutorial uses an [open source dataset of PDFs from Hugging Face](https://huggingface.co/datasets/broadfield-dev/pdf-ocr-dataset). You will convert the first page of the first 5 PDFs into an image, run OCR on that page, and finally extract the metrics from it.

1. Load the dataset.

   ```python title="Python" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/unstructured_data/unstructured_data_process_files.py - load the dataset"
   ```

2. Iterate through the PDFs, converting the first page into an image before running OCR and storing the metrics.

   ```python title="Python" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/unstructured_data/unstructured_data_process_files.py - iterate through the data"
   ```

3. Convert the metrics into a dataframe for validation.

   ```python title="Python" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/unstructured_data/unstructured_data.py - convert the data into a dataframe"
   ```

## Set up GX and define Expectations
In this tutorial, you will create a GX Data Context and either get or create a pandas Data Source and a dataframe Data Asset. Batch Definitions both organize a Data Asset's records into Batches and provide a method for retrieving those records. The Batch Definition in this tutorial will use the whole dataframe that you created in the previous step.

1. Instantiate the GX Data Context and get or create the Data Source, Data Asset, and Batch Definition.
   ```python title="Python" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/unstructured_data/unstructured_data.py - create the GX entities"
   ```

2. Get or create an Expectation Suite and create Expectations to validate the metrics generated from the PDFs. This tutorial utilizes the `ExpectColumnValuesToBeBetween` Expectation in order to validate that the metrics we stored in the dataframe meet our parameters. You can also try using different Expectations or value ranges.

   ```python title="Python" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/unstructured_data/unstructured_data.py - create the expectation suite"
   ```

## Validate your Expectations
GX uses a Validation Definition to link a Batch Definition and Expectation Suite. A Checkpoint will be used to execute Validations.

1. Create the Validation Definition.

   ```python title="Python" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/unstructured_data/unstructured_data.py - create the vd"
   ```

2. Create and run the Checkpoint.

   ```python title="Python" name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/unstructured_data/unstructured_data.py - create and run the checkpoint"
   ```

## Review the results
Now that you have set up the Data Source, Data Asset, Expectations, and have run the Checkpoint, you can review the outcome. Running the Checkpoint returns a result object that reports, for each Expectation, whether the OCR metrics met your defined thresholds, so you can confirm which PDFs passed validation and which require investigation.

## The path forward
Using this tutorial as a framework, you can try plugging in your own unstructured data, as well as add other Expectations from the [Expectation Gallery](https://greatexpectations.io/expectations) to the Expectation Suite. You can also explore validating your unstructured data [within a data pipeline](/reference/learn/integrations/data_pipeline_tutorial.md) by using this code with an orchestrator.

Businesses that rely on unstructured data should take the steps necessary to ensure the quality of it, but this is only one of many data quality scenarios that is relevant to an organization. Explore our other [data quality use cases](/reference/learn/data_quality_use_cases/dq_use_cases_lp.md) for more insights and best practices to expand your data validation to encompass key quality dimensions.