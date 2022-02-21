---
title: How to contribute a Custom Expectation to Great Expectations
---
import Prerequisites from '../creating_custom_expectations/components/prerequisites.jsx'
import Tabs from '@theme/Tabs'
import TabItem from '@theme/TabItem'

This guide will help you contribute your Custom Expectations to Great Expectations’ shared library. Your Custom Expectation will be featured in the Expectations Gallery, 
along with many others developed by data practitioners from around the world as part of this collaborative community effort.

<Prerequisites>

  * [Created a Custom Expectation](../creating_custom_expectations/overview.md)

</Prerequisites>

## Steps

### 1. Verify that your Custom Expectation is ready for contribution

We accept contributions into the Great Expectations codebase at several levels: Experimental, Beta, and Production. The requirements to meet these benchmarks are available in our 
document on [levels of maturity for Expectations](../../../contributing/contributing_maturity.md).

If you call the `print_diagnostic_checklist()` method on your Custom Expectation, you should see a checklist similar to this one:

```
✔ Has a library_metadata object
✔ Has a docstring, including a one-line short description
  ...
✔ Has at least one positive and negative example case, and all test cases pass
✔ Has core logic and passes tests on at least one Execution Engine
  ...
✔ Has basic input validation and type checking
✔ Has all three statement Renderers: descriptive, prescriptive, diagnostic
✔ Has core logic that passes tests for all applicable Execution Engines and SQL dialects
  ...
✔ Passes all linting checks
  Has a full suite of tests, as determined by project code standards
  Has passed a manual review by a code owner for code standards and style guides
```

If you've satisified at least the first four checks, you're ready to make a contribution!

:::info
Not quite there yet? See our guides on [creating Custom Expectations](../creating_custom_expectations/overview.md) for help!

For more information on our code standards and contribution, see our guide on [Levels of Maturity](../../../contributing/contributing_maturity.md#contributing-expectations) for Expectations.
:::

### 2. Double-check your Library Metadata

We want to verify that your Custom Expectation is properly credited and accurately described. 

Ensure that your Custom Expectation's `library_metadata` has correct information for the following:

- `contributors`: You and anyone else who helped you create this Custom Expectation.
- `tags`: These are simple descriptors of your Custom Expectation's functionality and domain (`statistics`, `flexible comparisons`, `geography`, etc.).
- `requirements`: If your Custom Expectation relies on any third-party packages, verify that those dependencies are listed here.
- `package` (optional): If you're contributing to a specific package, be sure to list it here!

<details>
<summary>Packages?</summary>
If you're interested in learning more about Custom Expectation Packages, see our <a href='/docs/contributing/contributing_package'>guide on packaging your Custom Expectations</a>.
<br/><br/>
Not contributing to a specifc package? Your Custom Expectation will be automatically published in the <a href="https://pypi.org/project/great-expectations-experimental/">PyPI package <inlineCode>great-expectations-experimental</inlineCode></a>. 
This package contains all of our Experimental community-contributed Custom Expectations, and is separate from the core <inlineCode>great-expectations</inlineCode> package.
</details>

### 3. Open a Pull Request

You're ready to open a [Pull Request](https://github.com/great-expectations/great_expectations/pulls)! 

As a part of this process, we ask you to:

- Sign our [Contributor License Agreement (CLA)](../../../contributing/contributing_misc.md#contributor-license-agreement-cla)
- Provide some information for our reviewers to expedite your contribution process, including:
  - A `[CONTRIB]` tag in your title
  - Titleing your Pull Request with the name of your Custom Expectation
  - A brief summary of the functionality and use-cases for your Custom Expectation
  - A description of any previous discussion or coordination related to this Pull Request
- Update your branch with the most recent code from the Great Expectations main repository
- Resolve any failing tests and merge conflicts


<div style={{"text-align":"center"}}>  
<p style={{"color":"#8784FF","font-size":"1.4em"}}><b>  
Congratulations!<br/>&#127881; You've submitted a Custom Expectation for contribution to the Great Expectations codebase! &#127881;  
</b></p>  
</div>

:::info
Contributing as a part of a Great Expectations Hackathon?

Submit your PR with a `[HACKATHON]` tag in your title instead of `[CONTRIB]`, and be sure to call out your 
participation in the Hackathon in the text of your PR as well!
:::

### 4. Stay involved!

Once your Custom Expectation has been reviewed by a Great Expectations code owner, it may require some 
additional work before it is approved. For example, if you are missing required checks in your diagnostic checklist, have failing tests, 
or have an error in the functionality of your Custom Expectation, we will ask you to resolve these before moving forward. 

If you are submitting a Custom Expectation for acceptance at a Production level, we will additionally require that you work with us to bring your Custom Expectation 
up to our standards for testing and code style on a case-by-case basis.

Once your Custom Expectation has passing tests and an approving review from a code owner, your contribution will be complete, and your Custom Expectation 
will be included in the next release of Great Expectations. 

Keep an eye out for an acknowledgement in our release notes, and welcome to the community!

:::note
If you’ve included your (physical) mailing address in the [Contributor License Agreement](../../../contributing/contributing_misc.md#contributor-license-agreement-cla), 
we’ll send you a personalized Great Expectations mug once your first PR is merged!
:::
