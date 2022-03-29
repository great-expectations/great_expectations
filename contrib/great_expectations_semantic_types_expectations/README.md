## Great Expectations Semantic Types Expectations
A collection of Expectations to validate Semantically Typed Data with Great Expectations.

### What Are Semantic Types?

Semantic Types categorize data by the type of information it represents. For example, consider a column with STRING data types. How do you know what the data means? You could be looking at a list of streets, cities, counties, etc. Without background information, it can be hard to know what you're looking at. That's where Semantic Types come in. The entity’s Semantic Type defines how it should be interpreted.

### Why should we test on Semantic Types?

Testing on these Semantic Types allows us to create more explicit, fit-for-purpose tests, unlocking questions like `Does this column contain US State abbreviations?` instead of asking `Does this column contain strings in the set [AL, AL, AR, AZ...]`

### How do you know when data entities are good candidates for Semantic Types testing?

Any data that you can tie to a real-world category or reference is ideal for this kind of test, e.g., phone numbers, ZIP codes, countries, coordinates, URLs, email addresses, etc.

This package contains a number of Expectations to support validation of Semantically Typed Data.

Author: [Great Expectations](https://github.com/great_expectations/great_expectations)

[PyPi Link](https://pypi/python.org/pypi/great_expectations_semantic_types_expectations)
