# 3. Automated testing

Date: 2023-03-28

## Status

Proposed

## Context

We have a large number of automated tests. Some are fast, isolated unit tests, others are integration tests that exercise our interactions with external resources such as local and cloud storage, datasources, etc. and typically take longer to run.

Our goal over time is to achieve a high level of code coverage with unit tests alone. To support this analysis, we tag our tests using pytest as either `@pytest.mark.unit` or `@pytest.mark.integration`.

### Test Definitons
Michael Feathers<sup>1</sup> defines a unit test as tests that:
1. Run fast (execute in 1/100th of a second or less)
2. Help us localize problems

In differentiating unit tests from other kinds of tests, he writes:

> Unit tests run fast. If they don't run fast, they aren't unit tests.
>
> Other kinds of tests often masquarade as unit tets. A test is not a unit test if:
> 1. It talks to a database.
> 2. It communicates across a network.
> 3. It touches the file system.
> 4. You have to do special things to your environment (such as editing configuration files) to run it.
>
> Tests that do these things aren't bad. Often they are worth writing, and you generally will write them in unit test harnesses. However, it is important to be able to separate them from true unit tests so that you can keep a set of tests that you can run _fast_ whenever you make changes.

## Decision

All unit tests must be marked with `@pytest.mark.unit`, other tests must be marked with `@pytest.mark.integration`. To be considered unit tests, they must meet the above defintion.

## Consequences

Doing this effectively requires both that we instrument our build pipeline to detect improperly marked tests and an ongoing effort to tag our existing test suites. As we work to increase the isolation necessary to localize problems and keep tests fast, this effort will drive us towards new coding patterns (such as dependency injection).

<sup>1</sup> [_Working Effectively with Legacy Code_](https://www.amazon.com/Working-Effectively-Legacy-Michael-Feathers/dp/0131177052/) by Michael C. Feathers