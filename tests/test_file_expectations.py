import unittest

import great_expectations as ge


class TestUtilMethods(unittest.TestCase):
    def test_expect_file_hash_to_equal(self):
        # Test for non-existent file
        try:
            ge.expect_file_hash_to_equal('abc', value='abc')
        except IOError:
            pass
        # Test for non-existent hash algorithm
        test_file = './tests/test_sets/Titanic.csv'
        try:
            ge.expect_file_hash_to_equal(test_file,
                                         hash_alg='md51',
                                         value='abc')
        except ValueError:
            pass
        # Test non-matching hash value
        self.assertFalse(ge.expect_file_hash_to_equal(test_file,
                                                      value='abc'))
        # Test matching hash value with default algorithm
        self.assertTrue(ge.expect_file_hash_to_equal(test_file,
                                                     value='63188432302f3a6e8c9e9c500ff27c8a'))
        # Test matching hash value with specified algorithm
        self.assertTrue(ge.expect_file_hash_to_equal(test_file,
                                                     value='f89f46423b017a1fc6a4059d81bddb3ff64891e3c81250fafad6f3b3113ecc9b',
                                                     hash_alg='sha256'))

    def test_expect_file_size_to_be_between(self):
        # Test for non-existent file
        try:
            ge.expect_file_size_to_be_between('abc', 0, 10000)
        except OSError:
            pass
        test_file = './tests/test_sets/Titanic.csv'
        # Test minsize not an integer
        try:
            ge.expect_file_size_to_be_between(test_file, '0', 10000)
        except TypeError:
            pass
        # Test maxsize not an integer
        try:
            ge.expect_file_size_to_be_between(test_file, 0, '10000')
        except TypeError:
            pass
        # Test minsize less than 0
        try:
            ge.expect_file_size_to_be_between(test_file, -1, 10000)
        except ValueError:
            pass
        # Test maxsize less than 0
        try:
            ge.expect_file_size_to_be_between(test_file, 0, -1)
        except ValueError:
            pass
        # Test minsize > maxsize
        try:
            ge.expect_file_size_to_be_between(test_file, 10000, 0)
        except ValueError:
            pass
        # Test file size not in range
        self.assertFalse(
            ge.expect_file_size_to_be_between(test_file, 0, 10000))
        # Test file size in range
        self.assertTrue(ge.expect_file_size_to_be_between(
            test_file, 70000, 71000))

    def test_expect_file_to_exist(self):
        # Test for non-existent file
        self.assertFalse(ge.expect_file_to_exist('abc'))
        # Test for existing file
        self.assertTrue(ge.expect_file_to_exist(
            './tests/test_sets/Titanic.csv'))

    def test_expect_file_unique_column_names_csv(self):
        # Test for non-existent file
        try:
            ge.expect_file_unique_column_names_csv('abc')
        except IOError:
            pass
        # Test for non-unique column names
        test_file = './tests/test_sets/same_column_names.csv'
        self.assertFalse(ge.expect_file_unique_column_names_csv(test_file,
                                                                sep='|',
                                                                skipLines=2))
        # Test for unique column names
        test_file = './tests/test_sets/Titanic.csv'
        self.assertTrue(ge.expect_file_unique_column_names_csv(test_file))

    def test_expect_file_valid_json(self):
        # Test for non-existent file
        try:
            ge.expect_file_valid_json('abc')
        except IOError:
            pass

        # Test invalid JSON file
        test_file = './tests/test_sets/invalid_json_file.json'
        self.assertFalse(ge.expect_file_valid_json(test_file))

        # Test valid JSON file
        test_file = './tests/test_sets/titanic_expectations.json'
        self.assertTrue(ge.expect_file_valid_json(test_file))

        # Test valid JSON file with non-matching schema
        test_file = './tests/test_sets/json_test1_against_schema.json'
        schema_file = './tests/test_sets/sample_schema.json'
        self.assertFalse(ge.expect_file_valid_json(
            test_file, schema=schema_file))

        # Test valid JSON file with valid schema
        test_file = './tests/test_sets/json_test2_against_schema.json'
        schema_file = './tests/test_sets/sample_schema.json'
        self.assertTrue(ge.expect_file_valid_json(
            test_file, schema=schema_file))
