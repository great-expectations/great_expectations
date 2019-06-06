# Great Expectations Render

The render API provides support for converting a variety of Great Expectations objects into documentation and other views.

Render supports the following types of objects:

1. Expectations
2. Validations (Expectation Validation Results or EVRs)
3. Data Profiles
4. Structured expectations, validations, or data profiles (see below)

To generate *views* of those object types, the render API first creates *models*.

## Models

- Models are structured objects. All raw GE objects including expectations, EVRs and Data Profiles are valid models for the Render API, but the render API also introduces several new model types such as *pages*, *sections*, and *content_blocks*.
- The render.model module supports ordering, nesting, and augmenting great_expectations objects.

## Views

- Views are human-consumable renderings of models.
- The render.view module supports converting structured or primitive GE objects into view-ready documents such as HTML, PDF, and raw text.
