import unittest
from great_expectations import render

class TestRender(unittest.TestCase):

    def test_import(self):
        from great_expectations import render

    def test_does_something(self):
        render.generate_single_expectations()


