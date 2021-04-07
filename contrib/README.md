# Great Expectations Contrib Packages.


Our goal is support a vibrant data community that is fully empowered to express their understanding of data in the language of Great Expectations. To do that, we need to ensure that the language is sufficiently expressive. That means we need to expand the vocabulary of Expectations and improve the inventory of Metrics. And our community is how we make that possible.

So, welcome to Great Expectations contrib--the place for experimental and domain-specific contributions to the Great Expectations repository. Here, you can expect to find a place to make contributions of all kinds; large or small, complete or in ideation, robust or experimental. By contributing to the great-expectations-contrib package, your custom Expectation can be quickly made available to any user. It's as easy as a PR into the /contrib/expectations directory of the main Great Expectations repository at https://github.com/great-expectations/great_expectations.  We'll run automated checks--including adding the expectation to our Gallery based on metadata and docstrings that you can add--and immediately include it in the contrib package.

Note that we do not guarantee that Expectations in the `experimental` package are semantically correct and we do not plan to maintain them independently. However, Expectations that the community finds valuable can move into domain-specific or core packages in the future.

## Using Contrib

Using a new Expectation contributed by a user is easy. Simply `pip install --upgrade great-expectations-experimental` and import the Expectation you'd like. Because each Expectation is modular and not imported until you declare you'd like to use it, you're in complete control of dependencies for contrib expectations.

For example:

```
from great_expectations_contrib.expectations import ExpectNelsonsColumnToExist

# ... obtain Validator

validator.expect_nelsons_column_to_exist()
```


## Contributing to Contrib

1. Start with a Docstring. Even if you're proposing just a "Request for Expectation" or description of desired behavior, it's important to include a docstring that covers that information.

2. Tests for Expectations. The Gallery relies on tests to describe Expectations (and of course test that they work). Follow the how-to guide on authoring custom Expectations to learn more about the test format.

3. Tag your work with Metadata! Adding `library_metadata` helps others understand the Expectation better in the documentation (and helps give you credit too!). Follow the how-to guide on authoring custom Expectations to learn more about the metadata format.

4. Don't forget renderers. Renderers are a powerful part of the new Expectation. Make sure to add renderers that help users understand what the Expectation is validating. See [the how-to guide](https://docs.greatexpectations.io/en/latest/guides/how_to_guides/configuring_data_docs/how_to_create_renderers_for_custom_expectations.html) for more details.
