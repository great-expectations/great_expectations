---
title: Levels  of Maturity
---

Features and code within Great Expectations are separated into three levels of maturity: Experimental, Beta, and Production.

* Experimental: Try, but do not rely
* Beta: Ready for early adopters
* Production: Ready for general use

Being explicit about these levels allows us to enable experimentation, without creating unnecessary thrash when features and APIs evolve. It also helps streamline development, by giving contributors a clear, incremental path to create and improve the Great Expectations code base.

For users of Great Expectations, our goal is to enable informed decisions about when to adopt which features.

For contributors to Great Expectations, our goal is to channel creativity by always making the next step as clear as possible.

This grid provides guidelines for how the maintainers of Great Expectations evaluate levels of maturity. Maintainers will always exercise some discretion in determining when any given feature is ready to graduate to the next level. If you have ideas or suggestions for leveling up a specific feature, please raise them in Github issues, and we’ll work with you to get there.


| Criteria                                 |    Experimental <br/>Try, but do not rely |    Beta <br/>Ready for early adopters |    Production <br/>Ready for general use |
|------------------------------------------|--------------------------------------|----------------------------------|-------------------------------------|
| API stability                            | Unstable*                            | Mostly Stable                    | Stable                              |
| Implementation completeness              | Minimal                              | Partial**                        | Complete                            |
| Unit test coverage                       | Minimal                              | Partial                          | Complete                            |
| Integration/Infrastructure test coverage | Minimal                              | Partial                          | Complete                            |
| Documentation completeness               | Minimal                              | Partial                          | Complete                            |
| Bug risk                                 | High                                 | Moderate                         | Low                                 |


* Experimental classes log warning-level messages when initialized: “Warning: great_expectations.some_module.SomeClass is experimental. Methods, APIs, and core behavior may change in the future.”

** In the special case of Expectations, some gaps in implementation are allowed in beta (e.g. works in pandas and Spark, but not yet in SQLAlchemy; validation and rendering work, but not profiling yet)
