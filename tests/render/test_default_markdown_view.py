import json
import os
from collections import OrderedDict

import pytest

import great_expectations as ge
import great_expectations.render as render
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResultSchema,
)
from great_expectations.render.renderer import ValidationResultsPageRenderer
from great_expectations.render.types import (
    RenderedDocumentContent,
    RenderedHeaderContent,
    RenderedSectionContent,
    RenderedTableContent,
)
from great_expectations.render.view import DefaultMarkdownPageView
from great_expectations.validation_operators.types.validation_operator_result import (
    ValidationOperatorResult,
)


@pytest.fixture()
def validation_operator_result():
    fixture_filename = os.path.join(
        os.path.dirname(__file__),
        "fixtures/ValidationOperatorResult_with_multiple_validation_results.json",
    )
    with open(fixture_filename) as infile:
        validation_operator_result = json.load(infile, object_pairs_hook=OrderedDict)
        run_results = validation_operator_result["run_results"]
        for k, validation_result in run_results.items():
            validation_result[
                "validation_result"
            ] = ExpectationSuiteValidationResultSchema().load(
                validation_result["validation_result"]
            )
        return validation_operator_result


def test_render_section_page():
    section = RenderedSectionContent(
        **{
            "section_name": None,
            "content_blocks": [
                RenderedHeaderContent(
                    **{
                        "content_block_type": "header",
                        "header": "Overview",
                    }
                ),
                RenderedTableContent(
                    **{
                        "content_block_type": "table",
                        "header": "Dataset info",
                        "table": [
                            ["Number of variables", "12"],
                            ["Number of observations", "891"],
                        ],
                        "styling": {
                            "classes": ["col-6", "table-responsive"],
                            "styles": {"margin-top": "20px"},
                            "body": {"classes": ["table", "table-sm"]},
                        },
                    }
                ),
            ],
        }
    )

    rendered_doc = ge.render.view.view.DefaultMarkdownPageView().render(
        RenderedDocumentContent(sections=[section])
    )

    rendered_doc = rendered_doc.replace(" ", "").replace("\t", "").replace("\n", "")

    assert (
        rendered_doc
        == """
        #ValidationResults
        ##Overview
        ###Datasetinfo
        ||||------------|------------|Numberofvariables|12Numberofobservations|891
        -----------------------------------------------------------
        Poweredby[GreatExpectations](https://greatexpectations.io/)
        """.replace(
            " ", ""
        )
        .replace("\t", "")
        .replace("\n", "")
    )


def test_snapshot_render_section_page_with_fixture_data(validation_operator_result):
    """
    Make sure the appropriate markdown rendering is done for the applied fixture.
    Args:
        validation_operator_result: test fixture

    Returns: None

    """
    validation_operator_result = ValidationOperatorResult(**validation_operator_result)

    validation_results_page_renderer = ValidationResultsPageRenderer(
        run_info_at_end=True
    )

    rendered_document_content_list = (
        validation_results_page_renderer.render_validation_operator_result(
            validation_operator_result=validation_operator_result
        )
    )

    md_str_list = DefaultMarkdownPageView().render(rendered_document_content_list)

    md_str = " ".join(md_str_list)

    md_str = md_str.replace(" ", "").replace("\t", "").replace("\n", "")

    assert (
        md_str
        == """
# Validation Results




## Overview
### **Expectation Suite:** **basic.warning**
**Data asset:** **None**
**Status:**  **Failed**





### Statistics






 |  |  |
 | ------------  | ------------ |
Evaluated Expectations  | 11
Successful Expectations  | 9
Unsuccessful Expectations  | 2
Success Percent  | ≈81.82%





## Table-Level Expectations








 | Status | Expectation | Observed Value |
 | ------------  | ------------  | ------------ |
❌  | Must have greater than or equal to **27000** and less than or equal to **33000** rows.  | 30
✅  | Must have exactly **3** columns.  | 3
✅  | Must have these columns in this order: **Team**, ** "Payroll (millions)"**, ** "Wins"**  | ['Team', ' "Payroll (millions)"', ' "Wins"']





##  "Payroll (millions)"








 | Status | Expectation | Observed Value |
 | ------------  | ------------  | ------------ |
✅  | values must never be null.  | 100% not null
✅  | minimum value must be greater than or equal to **54.24** and less than or equal to **56.24**.  | 55.24
✅  | maximum value must be greater than or equal to **196.96** and less than or equal to **198.96**.  | 197.96
✅  | mean must be greater than or equal to **97.01899999999998** and less than or equal to **99.01899999999998**.  | ≈98.019
❌  | median must be greater than or equal to **84000.75** and less than or equal to **86000.75**.  | 85.75
✅  | quantiles must be within the following value ranges.




 | Quantile | Min Value | Max Value |
 | ------------  | ------------  | ------------ |
0.05  | 54.37  | 56.37
Q1  | 74.48  | 76.48
Median  | 82.31  | 84.31
Q3  | 116.62  | 118.62
0.95  | 173.54  | 175.54
  |




 | Quantile | Value |
 | ------------  | ------------ |
0.05  | 55.37
Q1  | 75.48
Median  | 83.31
Q3  | 117.62
0.95  | 174.54






## Team








 | Status | Expectation | Observed Value |
 | ------------  | ------------  | ------------ |
✅  | values must never be null.  | 100% not null
✅  | values must always be greater than or equal to **1** characters long.  | 0% unexpected







### Info






 |  |  |
 | ------------  | ------------ |
Great Expectations Version  | 0.11.8+4.g4ab34df3.dirty
Run Name  | getest run
Run Time  | 2020-07-27T17:19:32.959193+00:00





### Batch Markers






 |  |  |
 | ------------  | ------------ |
**ge_load_time**  | **20200727T171932.954810Z**
**pandas_data_fingerprint**  | **8c46fdaf0bd356fd58b7bcd9b2e6012d**





### Batch Kwargs






 |  |  |
 | ------------  | ------------ |
**PandasInMemoryDF**  | **True**
**datasource**  | **getest**
**ge_batch_id**  | **56615f40-d02d-11ea-b6ea-acde48001122**




-----------------------------------------------------------
Powered by [Great Expectations](https://greatexpectations.io/)
# Validation Results




## Overview
### **Expectation Suite:** **basic.warning**
**Data asset:** **None**
**Status:**  **Failed**





### Statistics






 |  |  |
 | ------------  | ------------ |
Evaluated Expectations  | 11
Successful Expectations  | 9
Unsuccessful Expectations  | 2
Success Percent  | ≈81.82%





## Table-Level Expectations








 | Status | Expectation | Observed Value |
 | ------------  | ------------  | ------------ |
❌  | Must have greater than or equal to **27000** and less than or equal to **33000** rows.  | 30
✅  | Must have exactly **3** columns.  | 3
✅  | Must have these columns in this order: **Team**, ** "Payroll (millions)"**, ** "Wins"**  | ['Team', ' "Payroll (millions)"', ' "Wins"']





##  "Payroll (millions)"








 | Status | Expectation | Observed Value |
 | ------------  | ------------  | ------------ |
✅  | values must never be null.  | 100% not null
✅  | minimum value must be greater than or equal to **54.24** and less than or equal to **56.24**.  | 55.24
✅  | maximum value must be greater than or equal to **196.96** and less than or equal to **198.96**.  | 197.96
✅  | mean must be greater than or equal to **97.01899999999998** and less than or equal to **99.01899999999998**.  | ≈98.019
❌  | median must be greater than or equal to **84000.75** and less than or equal to **86000.75**.  | 85.75
✅  | quantiles must be within the following value ranges.




 | Quantile | Min Value | Max Value |
 | ------------  | ------------  | ------------ |
0.05  | 54.37  | 56.37
Q1  | 74.48  | 76.48
Median  | 82.31  | 84.31
Q3  | 116.62  | 118.62
0.95  | 173.54  | 175.54
  |




 | Quantile | Value |
 | ------------  | ------------ |
0.05  | 55.37
Q1  | 75.48
Median  | 83.31
Q3  | 117.62
0.95  | 174.54






## Team








 | Status | Expectation | Observed Value |
 | ------------  | ------------  | ------------ |
✅  | values must never be null.  | 100% not null
✅  | values must always be greater than or equal to **1** characters long.  | 0% unexpected







### Info






 |  |  |
 | ------------  | ------------ |
Great Expectations Version  | 0.11.8+4.g4ab34df3.dirty
Run Name  | getest run
Run Time  | 2020-07-27T17:19:32.959193+00:00





### Batch Markers






 |  |  |
 | ------------  | ------------ |
**ge_load_time**  | **20200727T171932.954810Z**
**pandas_data_fingerprint**  | **8c46fdaf0bd356fd58b7bcd9b2e6012d**





### Batch Kwargs






 |  |  |
 | ------------  | ------------ |
**PandasInMemoryDF**  | **True**
**datasource**  | **getest**
**ge_batch_id**  | **56615f40-d02d-11ea-b6ea-acde48001122**




-----------------------------------------------------------
Powered by [Great Expectations](https://greatexpectations.io/)
""".replace(
            " ", ""
        )
        .replace("\t", "")
        .replace("\n", "")
    )


def test_render_section_page_with_fixture_data_multiple_validations(
    validation_operator_result,
):
    """
    Make sure the appropriate markdown rendering is done for the applied fixture.
    :param validation_operator_result: test fixture
    :return: None
    """

    validation_operator_result = ValidationOperatorResult(**validation_operator_result)

    validation_results_page_renderer = ValidationResultsPageRenderer(
        run_info_at_end=True
    )

    rendered_document_content_list = (
        validation_results_page_renderer.render_validation_operator_result(
            validation_operator_result=validation_operator_result
        )
    )

    md_str_list = DefaultMarkdownPageView().render(rendered_document_content_list)

    md_str = " ".join(md_str_list)

    md_str = md_str.replace(" ", "").replace("\t", "").replace("\n", "")

    print(md_str)

    assert (
        md_str
        == """
# Validation Results




## Overview
### **Expectation Suite:** **basic.warning**
**Dataasset:** **None**
**Status:**  **Failed**





### Statistics






 |  |  |
 | ------------  | ------------ |
Evaluated Expectations  | 11
Successful Expectations  | 9
Unsuccessful Expectations  | 2
Success Percent  | ≈81.82%





## Table-Level Expectations








 | Status | Expectation | Observed Value |
 | ------------  | ------------  | ------------ |
❌  | Must have greater than or equal to **27000** and less than or equal to **33000** rows.  | 30
✅  | Must have exactly **3** columns.  | 3
✅  | Must have these columns in this order: **Team**, ** "Payroll (millions)"**, ** "Wins"**  | ['Team', ' "Payroll (millions)"', ' "Wins"']





##  "Payroll (millions)"








 | Status | Expectation | Observed Value |
 | ------------  | ------------  | ------------ |
✅  | values must never be null.  | 100% not null
✅  | minimum value must be greater than or equal to **54.24** and less than or equal to **56.24**.  | 55.24
✅  | maximum value must be greater than or equal to **196.96** and less than or equal to **198.96**.  | 197.96
✅  | mean must be greater than or equal to **97.01899999999998** and less than or equal to **99.01899999999998**.  | ≈98.019
❌  | median must be greater than or equal to **84000.75** and less than or equal to **86000.75**.  | 85.75
✅  | quantiles must be within the following value ranges.




 | Quantile | Min Value | Max Value |
 | ------------  | ------------  | ------------ |
0.05  | 54.37  | 56.37
Q1  | 74.48  | 76.48
Median  | 82.31  | 84.31
Q3  | 116.62  | 118.62
0.95  | 173.54  | 175.54
  |




 | Quantile | Value |
 | ------------  | ------------ |
0.05  | 55.37
Q1  | 75.48
Median  | 83.31
Q3  | 117.62
0.95  | 174.54






## Team








 | Status | Expectation | Observed Value |
 | ------------  | ------------  | ------------ |
✅  | values must never be null.  | 100% not null
✅  | values must always be greater than or equal to **1** characters long.  | 0% unexpected







### Info






 |  |  |
 | ------------  | ------------ |
Great Expectations Version  | 0.11.8+4.g4ab34df3.dirty
Run Name  | getest run
Run Time  | 2020-07-27T17:19:32.959193+00:00





### Batch Markers






 |  |  |
 | ------------  | ------------ |
**ge_load_time**  | **20200727T171932.954810Z**
**pandas_data_fingerprint**  | **8c46fdaf0bd356fd58b7bcd9b2e6012d**





### Batch Kwargs






 |  |  |
 | ------------  | ------------ |
**PandasInMemoryDF**  | **True**
**datasource**  | **getest**
**ge_batch_id**  | **56615f40-d02d-11ea-b6ea-acde48001122**




-----------------------------------------------------------
Powered by [Great Expectations](https://greatexpectations.io/)
# Validation Results




## Overview
### **Expectation Suite:** **basic.warning**
**Dataasset:** **None**
**Status:**  **Failed**





### Statistics






 |  |  |
 | ------------  | ------------ |
Evaluated Expectations  | 11
Successful Expectations  | 9
Unsuccessful Expectations  | 2
Success Percent  | ≈81.82%





## Table-Level Expectations








 | Status | Expectation | Observed Value |
 | ------------  | ------------  | ------------ |
❌  | Must have greater than or equal to **27000** and less than or equal to **33000** rows.  | 30
✅  | Must have exactly **3** columns.  | 3
✅  | Must have these columns in this order: **Team**, ** "Payroll (millions)"**, ** "Wins"**  | ['Team', ' "Payroll (millions)"', ' "Wins"']





##  "Payroll (millions)"








 | Status | Expectation | Observed Value |
 | ------------  | ------------  | ------------ |
✅  | values must never be null.  | 100% not null
✅  | minimum value must be greater than or equal to **54.24** and less than or equal to **56.24**.  | 55.24
✅  | maximum value must be greater than or equal to **196.96** and less than or equal to **198.96**.  | 197.96
✅  | mean must be greater than or equal to **97.01899999999998** and less than or equal to **99.01899999999998**.  | ≈98.019
❌  | median must be greater than or equal to **84000.75** and less than or equal to **86000.75**.  | 85.75
✅  | quantiles must be within the following value ranges.




 | Quantile | Min Value | Max Value |
 | ------------  | ------------  | ------------ |
0.05  | 54.37  | 56.37
Q1  | 74.48  | 76.48
Median  | 82.31  | 84.31
Q3  | 116.62  | 118.62
0.95  | 173.54  | 175.54
  |




 | Quantile | Value |
 | ------------  | ------------ |
0.05  | 55.37
Q1  | 75.48
Median  | 83.31
Q3  | 117.62
0.95  | 174.54






## Team








 | Status | Expectation | Observed Value |
 | ------------  | ------------  | ------------ |
✅  | values must never be null.  | 100% not null
✅  | values must always be greater than or equal to **1** characters long.  | 0% unexpected







### Info






 |  |  |
 | ------------  | ------------ |
Great Expectations Version  | 0.11.8+4.g4ab34df3.dirty
Run Name  | getest run
Run Time  | 2020-07-27T17:19:32.959193+00:00





### Batch Markers






 |  |  |
 | ------------  | ------------ |
**ge_load_time**  | **20200727T171932.954810Z**
**pandas_data_fingerprint**  | **8c46fdaf0bd356fd58b7bcd9b2e6012d**





### Batch Kwargs






 |  |  |
 | ------------  | ------------ |
**PandasInMemoryDF**  | **True**
**datasource**  | **getest**
**ge_batch_id**  | **56615f40-d02d-11ea-b6ea-acde48001122**




-----------------------------------------------------------
Powered by [Great Expectations](https://greatexpectations.io/)
""".replace(
            " ", ""
        )
        .replace("\t", "")
        .replace("\n", "")
    )
