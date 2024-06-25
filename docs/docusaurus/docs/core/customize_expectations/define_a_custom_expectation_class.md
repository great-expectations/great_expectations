## Customize an Expectation Class

1. Choose and import a base Expectation class:

  ```python title="Python"
  from great_expectations.expectations import ExpectColumnValueToBeBetween
  ```

  You can customize any of the core Expectation classes in GX. You can view the available Expectations and their functionality in the [Expectation Gallery](https://greatexpectations.io/expectations).

2. Create a new Expectation class that inherits the base Expectation class.
  
  The core Expectations in GX have names descriptive of their functionality.  When you create a customized Expectation class you can provide a class name that is more indicative of your specific use case:

  ```python title="Python"
  class ExpectValidPassengerCount(ExpectColumnValueToBeBetween):
  ```

3. Override the Expectation's attributes with new default values:

  ```python title="Python"
  class ExpectValidPassengerCount(ExpectColumnValueToBeBetween):
      column: str = "passenger_count"
      min_value: int = 0
      max_value: int = 6
  ```

  The attributes that can be overriden correspond to the parameters required by the base Expectation.  These can be referenced from the [Expectation Gallery](https://greatexpectations.io/expectations).

4. Customize the rendering of the new Expectation when displayed in Data Docs:

  ```python title="Python"
  class ExpectValidPassengerCount(ExpectColumnValueToBeBetween):
      column: str = "passenger_count"
      min_value: int = 0
      max_value: int = 6
      render_text: str = "There should be between **0** and **6** passengers."
  ```

  The `render_text` attribute contains the text describing the customized Expectation when your results are rendered into Data Docs.  You can format the text with Markdown syntax.
