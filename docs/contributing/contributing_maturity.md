---
title: Levels  of Maturity
---

Features and code within Great Expectations are separated into three levels of maturity: Experimental, Beta, and Production.

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

Being explicit about these levels allows us to enable experimentation, without creating unnecessary thrash when features and APIs evolve. It also helps streamline development, by giving contributors a clear, incremental path to create and improve the Great Expectations code base.

For users of Great Expectations, our goal is to enable informed decisions about when to adopt which features.

For contributors to Great Expectations, our goal is to channel creativity by always making the next step as clear as possible.

This grid provides guidelines for how the maintainers of Great Expectations evaluate levels of maturity. Maintainers will always exercise some discretion in determining when any given feature is ready to graduate to the next level. If you have ideas or suggestions for leveling up a specific feature, please raise them in Github issues, and we’ll work with you to get there.


| Criteria                                 | <i class="fas fa-circle" style={{color: "#dc3545"}}></i> Experimental <br/>Try, but do not rely | <i class="fas fa-circle" style={{color: "#ffc107"}}></i> Beta <br/>Ready for early adopters | <i class="fas fa-circle" style={{color: "#28a745"}}></i> Production <br/>Ready for general use |
|------------------------------------------|--------------------------------------|----------------------------------|-------------------------------------|
| API stability                            | Unstable*                            | Mostly Stable                    | Stable                              |
| Implementation completeness              | Minimal                              | Partial**                        | Complete                            |
| Unit test coverage                       | Minimal                              | Partial                          | Complete                            |
| Integration/Infrastructure test coverage | Minimal                              | Partial                          | Complete                            |
| Documentation completeness               | Minimal                              | Partial                          | Complete                            |
| Bug risk                                 | High                                 | Moderate                         | Low                                 |


* Experimental classes log warning-level messages when initialized: “Warning: great_expectations.some_module.SomeClass is experimental. Methods, APIs, and core behavior may change in the future.”

** In the special case of Expectations, some gaps in implementation are allowed in beta (e.g. works in pandas and Spark, but not yet in SQLAlchemy; validation and rendering work, but not profiling yet)
