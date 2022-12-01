# API Docs

First `pip install -r requirements-dev-api-docs.txt` into your virtual environment. 
To build API docs, run `invoke docs`. 
To view the docusaurus docs which contain the API docs, run `yarn start`.

The API docs are built by:
1. Generating static html from sphinx
2. Pulling out the content 
3. Making modifications to the content and writing into mdx files
4. Serving the mdx files from within the docusaurus site

To add a new class to the docs, create a `my_class_name.md` file in this directory
using the snake case corresponding to your class name.
Add the following content, making sure to change MyClassName to your class in 
both places.

```markdown
    # MyClassName
    
    ```{eval-rst}
    .. autoclass:: great_expectations.data_context.data_context.MyClassName
       :members:
       :inherited-members:
    
    ```
```

To add a method on an existing class, use the @public_api decorator e.g.

```python
    from great_expectations.core._docs_decorators import public_api
    
    class MyClassName:

        @public_api
        def some_public_method(self):
            """My public method."""
            pass
```

Our styling is based on `pydata-sphinx-theme` with some modifications. You can
find the stylesheets linked from the docusaurus custom CSS `custom.scss`
Typically you will not need to modify the styling.
