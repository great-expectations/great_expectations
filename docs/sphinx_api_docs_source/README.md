# API Docs

First `pip install -r requirements-dev-api-docs.txt` into your virtual 
environment. 
To build API docs, run `invoke api-docs`. 
To view the docusaurus docs which contain the API docs, run `yarn start`.

The API docs are built by:
1. Generating markdown stubs and then static html from sphinx
2. Pulling out the content from the html files
3. Making modifications to the content and writing into mdx files
4. Serving the mdx files from within the docusaurus site

To add a new class or module level function to the docs, add the @public_api 
decorator e.g.:

```python
    from great_expectations.core._docs_decorators import public_api
    
    @public_api
    class ExamplePublicAPIClass:
        """My public API class."""

        @public_api
        def example_public_api_method(self):
            """My public API method."""
            pass

    @public_api
    def example_public_api_module_level_function():
        """My public API module level function."""
        pass
```

Once a class or method is marked as part of the public API, its docstring will
be subject to linting in our CI pipeline. You can run the linter via running
`invoke docstrings` at the repo root.

You can also add additional decorators to show deprecation or new items (classes,
parameters, methods). See great_expectations.core._docs_decorators for more
info.

Our API docs styling is based on `pydata-sphinx-theme` with some modifications. 
You can find the stylesheets linked from the docusaurus custom CSS `custom.scss`
Typically you will not need to modify the styling.

Code blocks in docstrings are supported, use markdown triple backticks to add a code snippet:

````
```yaml
my_yaml:
  - code_snippet
```
````

Note:
1. There must be an opening and closing set of triple backticks.
2. The opening backticks can have an optional language (see docusaurus for supported languages).

Tables are also supported, using [Sphinx table style](https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html#tables).
