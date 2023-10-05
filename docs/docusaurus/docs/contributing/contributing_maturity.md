---
title: Feature and code readiness
---

The following are the readiness levels for Great Expectations features and code:

<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css" crossorigin="anonymous" referrerpolicy="no-referrer" />
<div>
    <ul style={{
        "list-style-type": "none"
    }}>
        <li><i class="fas fa-circle" style={{color: "#dc3545"}}></i> &nbsp; Experimental: Try, but do not rely</li>
        <li><i class="fas fa-circle" style={{color: "#ffc107"}}></i> &nbsp; Beta: Ready for early adopters</li>
        <li><i class="fas fa-check-circle" style={{color: "#28a745"}}></i> &nbsp; Production: Ready for general use</li>
    </ul>
</div>

These readiness levels allow experimentation, without the need for unnecessary changes as features and APIs evolve. These readiness levels also help streamline development by providing contributors with a clear, incremental path for creating and improving the Great Expectations code base.

The following table details Great Expectations readiness levels. Great Expectations uses a cautious approach when determining if a feature or code change should be moved to the next readiness level. If you need a specific feature or code change advanced, open a GitHub issue.


| Criteria                                 | <i class="fas fa-circle" style={{color: "#dc3545"}}></i> Experimental <br/>Try, but do not rely | <i class="fas fa-circle" style={{color: "#ffc107"}}></i> Beta <br/>Ready for early adopters | <i class="fas fa-check-circle" style={{color: "#28a745"}}></i> Production <br/>Ready for general use |
|------------------------------------------|--------------------------------------|----------------------------------|-------------------------------------|
| API stability                            | Unstable ¹                            | Mostly Stable                    | Stable                              |
| Implementation completeness              | Minimal                              | Partial                          | Complete                            |
| Unit test coverage                       | Minimal                              | Partial                          | Complete                            |
| Integration/Infrastructure test coverage | Minimal                              | Partial                          | Complete                            |
| Documentation completeness               | Minimal                              | Partial                          | Complete                            |
| Bug risk                                 | High                                 | Moderate                         | Low                                 |


¹ When an experimental class is initialized, the following warning message appears in the log: 

`Warning: great_expectations.some_module.SomeClass is experimental. Methods, APIs, and core behavior may change in the future.`

## Expectation contributions

[Create and manage Custom Expectations](../guides/expectations/custom_expectations_lp.md) explains how you can create an Expectation with an experimental status. The `print_diagnostic_checklist()` method provides you with a list of requirements that you must meet to move your Expectation from experimental to beta, and then to production. The first five requirements are required for experimental status, the following three are required for beta status, and the final two are required for production status.

| Criteria                                 | <i class="fas fa-circle" style={{color: "#dc3545"}}></i> Experimental <br/>Try, but do not rely | <i class="fas fa-circle" style={{color: "#ffc107"}}></i> Beta <br/>Ready for early adopters | <i class="fas fa-check-circle" style={{color: "#28a745"}}></i> Production <br/>Ready for general use |
|------------------------------------------|:------------------------------------:|:--------------------------------:|:-----------------------------------:|
| Has a valid library_metadata object            | &#10004; | &#10004; | &#10004; |
| Has a docstring, including a one-line short description that begins with "Expect" and ends with a period | &#10004; | &#10004; | &#10004; |
| Has at least one positive and negative example case, and all test cases pass | &#10004; | &#10004; | &#10004; |
| Has core logic and passes tests on at least one Execution Engine | &#10004; | &#10004; | &#10004; |
| Passes all linting checks | &#10004; | &#10004; | &#10004; |
| Has basic input validation and type checking | &#8213; | &#10004; | &#10004; |
| Has both Statement Renderers: prescriptive and diagnostic | &#8213; | &#10004; | &#10004; |
| Has core logic that passes tests for all applicable Execution Engines and SQL dialects | &#8213; | &#10004; | &#10004; |
| Has a robust suite of tests, as determined by a code owner | &#8213; | &#8213; | &#10004; |
| Has passed a manual review by a code owner for code standards and style guides | &#8213; | &#8213; | &#10004; |
