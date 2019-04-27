import unittest
import json

from great_expectations import render

test_expectations_config = {
    "dataset_name": None,
    "meta": {
        "great_expectations.__version__": "0.4.5"
    },
    "expectations": [
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "x_var"}
        },
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "y_var"}
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "x_var"}
        },
        {
            "expectation_type": "expect_column_values_to_be_of_type",
            "kwargs": {
                "column": "x_var",
                "type_": "int",
                "target_datasource": "python"
            }
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "y_var"
            }
        }
    ]
}

test_validation_results = {
  "results": [
    {
      "success": True,
      "exception_info": {
        "raised_exception": False,
        "exception_message": None,
        "exception_traceback": None
      },
      "expectation_config": {
        "expectation_type": "expect_column_to_exist",
        "kwargs": {
          "column": "x_var"
        }
      }
    },
    {
      "success": True,
      "exception_info": {
        "raised_exception": False,
        "exception_message": None,
        "exception_traceback": None
      },
      "expectation_config": {
        "expectation_type": "expect_column_to_exist",
        "kwargs": {
          "column": "y_var"
        }
      }
    },
    {
      "success": True,
      "result": {
        "element_count": 5,
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_count": 0,
        "unexpected_percent": 0.0,
        "unexpected_percent_nonmissing": 0.0,
        "partial_unexpected_list": []
      },
      "exception_info": {
        "raised_exception": False,
        "exception_message": None,
        "exception_traceback": None
      },
      "expectation_config": {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {
          "column": "x_var"
        }
      }
    },
    {
      "success": True,
      "result": {
        "element_count": 5,
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_count": 0,
        "unexpected_percent": 0.0,
        "unexpected_percent_nonmissing": 0.0,
        "partial_unexpected_list": []
      },
      "exception_info": {
        "raised_exception": False,
        "exception_message": None,
        "exception_traceback": None
      },
      "expectation_config": {
        "expectation_type": "expect_column_values_to_be_of_type",
        "kwargs": {
          "column": "x_var",
          "type_": "int",
          "target_datasource": "python"
        }
      }
    },
    {
      "success": True,
      "result": {
        "element_count": 5,
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_count": 0,
        "unexpected_percent": 0.0,
        "unexpected_percent_nonmissing": 0.0,
        "partial_unexpected_list": []
      },
      "exception_info": {
        "raised_exception": False,
        "exception_message": None,
        "exception_traceback": None
      },
      "expectation_config": {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {
          "column": "y_var"
        }
      }
    }
  ]
}

class TestFullPageRender(unittest.TestCase):

    def test_import(self):
        from great_expectations import render

    def test_prescriptive_expectation_renderer(self):
        results = render.render(
            renderer_class=render.PrescriptiveExpectationPageRenderer,
            # expectations=test_expectations_config["expectations"],
            # expectations=json.load(open('tests/test_fixtures/test_expectations.json')),
            expectations=json.load(open('tests/test_fixtures/more_test_expectations.json'))["expectations"],
        )
        assert results != None

    def test_descriptive_evr_renderer(self):
        R = render.DescriptiveEvrPageRenderer(
          json.load(open('tests/test_fixtures/more_test_expectations_results.json'))["results"],
        )
        rendered_page = R.render()
        assert rendered_page != None

        with open('./test.html', 'w') as f:
            f.write(rendered_page)
