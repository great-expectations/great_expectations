import unittest
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

class TestRender(unittest.TestCase):

    def test_import(self):
        from great_expectations import render

    def test_does_something(self):
        results = render.render(
            renderer_class=render.FullPageHtmlRenderer,
            expectations=test_expectations_config["expectations"],
        )
        print(results)
        assert results != None
        # assert False


