# 2. Support latest point release

Date: 2023-03-24

## Status

Accepted

## Context

Dependency management is an ongoing challenge when publishing a library. While our constraints need to be reasonably loose, we must narrow the scope of what we support (both with our test suite and in community support channels) to avoid overwhelming our automated testing resources and support teams.

In discussing versions and version number components, please refer to: https://semver.org/

## Decision

Within a minor version, we only support the latest patch version.

For example, while we constrain our minor versions of SQLAlchmey to 1.4.0&gt;=,&lt;=2.0, we test and support only the latest (1.4.47 and 2.0.7 at the time of this writing).

## Consequences

Most users installing the latest release version of Great Expectations will also be installing the latest version of our supported libraries within constraints (considering how `pip install` and `pip --upgrade` works). We therefore expect the versions installed in our test environment (using the same mechanism as our users do for installing great_expectations) to match the environment of most users.

This also implies that in cases where we support a range of minor versions that we test Great Expectations on each of those minor versions. For instance, if we support both SQLAlchmey 1.4 and 2.0, we must run our SQLAlchemy-specific tests twice, once with 1.4 installed, and again with 2.0 installed.
