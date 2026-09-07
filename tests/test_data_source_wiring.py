"""Cross-checks that every registered data source's declared wiring coordinates actually exist
in the surfaces that select, install, and run its tests.

A record (see `tests/integration/test_utils/data_source_config/data_source_spec.py`, and its SQL
sub-record in `backend_spec.py`) may name its own marker, its dev-requirements file, its
task-runner key, its CI workflow job and lane token, and its compose service. Declaring one of
those coordinates and never actually wiring it up produces a data source that looks configured but
silently never runs anywhere - this module is the mechanical check that catches that gap the moment
a new record registers, rather than leaving it to be noticed the next time someone goes looking for
why a marker never seems to execute.

**The check is scaled to what each record claims.** It enumerates every *record*, not only the ones
that carry a harness config, and it demands of a record exactly what that record declares: a
coordinate a record does not state is not required, and a coordinate it does state is verified.
That distinction is load-bearing rather than lenient. A record claiming no tier is saying "this
data source ships, but no tier's suite proves it", which is an honest declaration that owes no
marker and no lane - Azure Blob Storage declares neither, and the postgres-compatible flavors
declare nothing at all. An unconditional demand would make that honest declaration unexpressible
and would fail records that are correct. What keeps the scaling from collapsing into "declares
nothing, so asserts nothing is wrong" is the last assertion below: a record that claims a *tier*
claims that a suite runs somewhere, and at that moment a marker and a lane become required again.

Every assertion here is presence-of-a-named-entry: it never pins a count, an order, or a shape,
so it survives an unrelated edit to any of the files it reads. Direction is one-way - it verifies
that every *registered* record is wired, not that every wiring entry has a registered record -
so it stays correct as other markers and jobs come and go for reasons that have nothing to do with
the data sources this module cares about.
"""

from __future__ import annotations

import pathlib
import re
from functools import cache
from typing import Any, Callable, Dict, Final, Iterator, Optional, Tuple, TypeVar

import pytest
import tomli
from tasks import MARKER_DEPENDENCY_MAP

from great_expectations.core.yaml_handler import YAMLHandler
from tests.conftest import REQUIRED_MARKERS
from tests.integration.test_utils.data_source_config.data_source_spec import (
    CiLaneRef,
    DataSourceProvisioning,
    DataSourceSpec,
    SupportTier,
)
from tests.integration.test_utils.data_source_config.registry import (
    isolated_registry,
    iter_data_sources,
    register_data_source,
)

pytestmark = pytest.mark.project

PROJECT_ROOT: Final = pathlib.Path(__file__).parent.parent
PYPROJECT_TOML: Final = PROJECT_ROOT / "pyproject.toml"
CONFTEST_PY: Final = PROJECT_ROOT / "tests" / "conftest.py"
TASKS_PY: Final = PROJECT_ROOT / "tasks.py"
CI_WORKFLOW: Final = PROJECT_ROOT / ".github" / "workflows" / "ci.yml"
COMPOSE_ROOT: Final = PROJECT_ROOT / "assets" / "docker"

_T = TypeVar("_T")


def _read_config(path: pathlib.Path, parse: Callable[[str], _T]) -> _T:
    """The one place a wiring-coordinate configuration file is read.

    Fails naming the file's absolute path - never a relative one, and never a silent pass -
    whichever way the file is unusable: absent, or present but not parseable by `parse`. A file
    read directly at each call site, instead of through here, is a second way to get this wrong
    that this module intentionally has no room for.
    """
    absolute = path.resolve()
    if not path.is_file():
        raise AssertionError(
            f"expected a wiring configuration file at {absolute}, but it does not exist"
        )
    try:
        return parse(path.read_text())
    except Exception as error:
        raise AssertionError(
            f"wiring configuration file at {absolute} could not be parsed: {error}"
        ) from error


@cache
def _pyproject_markers() -> Tuple[str, ...]:
    data = _read_config(PYPROJECT_TOML, tomli.loads)
    entries = data["tool"]["pytest"]["ini_options"]["markers"]
    return tuple(entry.split(":")[0].strip() for entry in entries)


@cache
def _ci_workflow_jobs() -> Dict[str, Any]:
    yaml_handler = YAMLHandler()
    data = _read_config(CI_WORKFLOW, yaml_handler.load)
    jobs = data.get("jobs", {})
    assert isinstance(jobs, dict), f"{CI_WORKFLOW.resolve()} has a non-mapping 'jobs' section"
    return jobs


# Splitting the whole serialized subtree once and comparing by exact token membership is what
# makes this a whole-token check rather than a substring search. The distinction matters because a
# marker name can be a prefixed form of another (`docs-spark` contains `spark`): under substring
# matching, a record declaring the shorter name would look wired to a job that only ever mentions
# the longer one, and its lane would appear configured while nothing ran it.
#
# Non-word characters split a token, but `-` does not, which is what keeps a hyphenated marker one
# token instead of two. A plain `\b` word boundary would not do - `\b` does not treat `-` as part
# of a word, so it would match the shorter name inside the longer one just as substring search
# does.
_TOKEN_SPLIT_PATTERN: Final = re.compile(r"[^\w-]+")


def _tokens(text: str) -> Tuple[str, ...]:
    return tuple(sorted({token for token in _TOKEN_SPLIT_PATTERN.split(text) if token}))


# The registry is read once, here, and every view below is derived from this one snapshot rather
# than from a fresh accessor call. A view rebuilt inside a test body would be a live read, and a
# live read taken inside the isolation seam sees the *emptied* registry - an assertion written
# that way passes because it iterates nothing, which is indistinguishable from passing because
# every record is wired.
_REGISTERED_ENTRIES: Final = iter_data_sources()
_REGISTERED_RECORDS: Final = tuple(entry.spec for entry in _REGISTERED_ENTRIES)


def _name_for_record(spec: DataSourceSpec, config_class: Optional[type]) -> str:
    """Name a record in a failure message.

    A record carried by a config class is named by that class, which is the name a maintainer sees
    at the decoration site. A record with no config has no such name, so its label and public name
    identify it instead - mirroring the registry's own convention, so a maintainer reads the same
    identifier in a registration rejection and in a drift failure.
    """
    if config_class is not None:
        return config_class.__name__
    return f"the record labelled {spec.label!r} ({spec.public_name!r})"


_NAMES_BY_LABEL: Final = {
    entry.spec.label: _name_for_record(entry.spec, entry.config_class)
    for entry in _REGISTERED_ENTRIES
}


def _record_label(spec: DataSourceSpec) -> str:
    return spec.label


def _registered_name(spec: DataSourceSpec) -> str:
    return _NAMES_BY_LABEL[spec.label]


@pytest.fixture(scope="module", autouse=True)
def _real_registry_is_unchanged_by_this_module() -> Iterator[None]:
    """The failure-path tests below register throwaway records. Each does so inside the isolation
    seam and checks the restore itself, but a per-test check only proves the seam held for the test
    that looked. Comparing the whole registry against the import-time snapshot once at module
    teardown is what makes "this module leaves the real registry exactly as it found it" a property
    of the module rather than of the tests that remembered to assert it.
    """
    yield
    assert iter_data_sources() == _REGISTERED_ENTRIES, (
        "this module's tests changed the real data source registry; the throwaway records they "
        "register must stay inside isolated_registry()"
    )


def test_registry_is_non_empty_at_collection() -> None:
    """Guards every parametrized test below against passing vacuously. A registry emptied by an
    import-order accident would make each `@pytest.mark.parametrize(..., _REGISTERED_RECORDS)`
    test collect zero cases and report as passed, which is indistinguishable from a real pass
    unless something asserts the collection was non-empty in the first place.
    """
    assert _REGISTERED_RECORDS, "no data sources were registered to check"


def test_a_lane_token_inside_a_compound_expression_is_found() -> None:
    """Pins the one thing `_tokens` has to do for a real declaration.

    A lane token has to be found inside a job that selects several markers in one expression,
    which rules out comparing the job's entry for equality. Relaxing the matching in the other
    direction, to whole-entry equality, fails immediately against the real records below.
    """
    assert "sqlite" in _tokens("openpyxl or pyarrow or project or sqlite or aws_creds")


# --------------------------------------------------------------------------------------------
# The wiring assertions. Each is a standalone function so it can be exercised two ways: once
# per registered record below, and once more per synthetic, deliberately-broken declaration in
# the failure-path tests further down - the same assertion code both proves the happy path and
# is proven, by those failure-path tests, to actually fire when wiring is missing.
#
# Each takes the record's name rather than deriving it, because a synthetic record is not in the
# registry and so has no name to look up.
# --------------------------------------------------------------------------------------------


def _assert_marker_is_registered_in_pyproject(name: str, spec: DataSourceSpec) -> None:
    if spec.marker is None:
        return
    assert spec.marker in _pyproject_markers(), (
        f"{name} declares marker {spec.marker!r} but it is not in the "
        f"markers list in {PYPROJECT_TOML.resolve()}"
    )


def _assert_marker_is_a_required_marker(name: str, spec: DataSourceSpec) -> None:
    if spec.marker is None:
        return
    assert spec.marker in REQUIRED_MARKERS, (
        f"{name} declares marker {spec.marker!r} but it is not in "
        f"REQUIRED_MARKERS in {CONFTEST_PY.resolve()}"
    )


def _assert_dev_requirements_file_exists(name: str, spec: DataSourceSpec) -> None:
    if spec.dev_requirements_file is None:
        return
    path = PROJECT_ROOT / spec.dev_requirements_file
    assert path.is_file(), (
        f"{name} declares dev_requirements_file "
        f"{spec.dev_requirements_file!r} but {path.resolve()} does not exist"
    )


def _assert_task_runner_entry_exists(name: str, spec: DataSourceSpec) -> None:
    """The first half of the task-runner check: the declared key resolves to an entry.

    This is deliberately separate from the second half below, and the split is not cosmetic. A
    record may declare a task-runner marker and no requirements file - Amazon S3 does, because the
    `aws_deps` entry names the shared lite requirements file rather than anything specific to S3,
    and claiming that file would read as "this file installs Amazon S3's dependencies", which is
    false. Checked as one assertion, such a record would assert that `None` is among that entry's
    requirement files and fail for declaring *less* than it could have. Two assertions, each firing
    on its own condition, is what makes "demand exactly what the record claims" true rather than
    approximately true.
    """
    if spec.task_runner_marker is None:
        return
    entry = MARKER_DEPENDENCY_MAP.get(spec.task_runner_marker)
    assert entry is not None, (
        f"{name} declares task-runner marker {spec.task_runner_marker!r} but "
        f"MARKER_DEPENDENCY_MAP in {TASKS_PY.resolve()} has no such key"
    )


def _assert_task_runner_entry_lists_requirements_file(name: str, spec: DataSourceSpec) -> None:
    """The second half: where a record declares *both* coordinates, they have to agree.

    Fires only when both are declared - see the split's rationale above.
    """
    if spec.task_runner_marker is None or spec.dev_requirements_file is None:
        return
    entry = MARKER_DEPENDENCY_MAP.get(spec.task_runner_marker)
    assert entry is not None, (
        f"{name} declares task-runner marker {spec.task_runner_marker!r} but "
        f"MARKER_DEPENDENCY_MAP in {TASKS_PY.resolve()} has no such key"
    )
    assert spec.dev_requirements_file in entry.requirement_files, (
        f"{name} declares dev_requirements_file "
        f"{spec.dev_requirements_file!r} but MARKER_DEPENDENCY_MAP"
        f"[{spec.task_runner_marker!r}] in {TASKS_PY.resolve()} does not list it among "
        f"{entry.requirement_files!r}"
    )


def _assert_workflow_job_and_lane_token(name: str, spec: DataSourceSpec) -> None:
    """Verify a *declared* lane. A record declaring no lane is not failed here.

    The obligation to declare a lane at all is not dropped, only relocated to where it is actually
    owed: `_assert_tier_claim_carries_evidence` below requires a lane of every record that claims a
    tier. Demanding one of every record would fail Azure Blob Storage and the postgres-compatible
    flavors, which run nowhere and say so.
    """
    lane = spec.ci_lane
    if lane is None:
        return
    job = _ci_workflow_jobs().get(lane.workflow_job)
    assert job is not None, (
        f"{name} declares CI workflow job {lane.workflow_job!r} but "
        f"{CI_WORKFLOW.resolve()} has no such job under 'jobs'"
    )
    tokens = _tokens(str(job))
    assert lane.marker_token in tokens, (
        f"{name} declares CI lane token {lane.marker_token!r} in "
        f"workflow job {lane.workflow_job!r}, but that token does not appear in "
        f"{CI_WORKFLOW.resolve()}; tokens found: {list(tokens)}"
    )


def _assert_container_service_wired(name: str, spec: DataSourceSpec) -> None:
    """Verify a *declared* compose service.

    Local-container provisioning with no named service is a legal declaration for a record claiming
    no tier - Citus is distributed as a container image and this repository has no compose file for
    it - so the service, not the provisioning member alone, is the coordinate this fires on.
    """
    if spec.provisioning is not DataSourceProvisioning.LOCAL_CONTAINER:
        return
    service = spec.container_service
    if service is None:
        return
    compose_path = COMPOSE_ROOT / service / "docker-compose.yml"
    assert compose_path.is_file(), (
        f"{name} declares container service {service!r} but {compose_path.resolve()} does not exist"
    )
    entry = MARKER_DEPENDENCY_MAP.get(spec.task_runner_marker) if spec.task_runner_marker else None
    assert entry is not None and service in entry.services, (
        f"{name} declares container service {service!r} but "
        f"MARKER_DEPENDENCY_MAP[{spec.task_runner_marker!r}] in {TASKS_PY.resolve()} does not "
        f"list it among its services"
    )


def _assert_tier_claim_carries_evidence(name: str, spec: DataSourceSpec) -> None:
    """A tier claim is the point at which a marker and a lane stop being optional.

    Every other assertion in this module verifies a coordinate the record chose to state, so a
    record stating nothing is checked for nothing - correctly, because "this data source ships and
    no tier's suite proves it" is an honest declaration. This assertion is what stops that scaling
    from being a way to claim coverage cheaply: a tier claim asserts that a suite runs somewhere,
    and a suite that no marker selects and no lane runs does not.
    """
    if not spec.tiers:
        return
    claimed = ", ".join(sorted(tier.value for tier in spec.tiers))
    assert spec.marker is not None, (
        f"{name} claims tier membership ({claimed}) but declares no marker, so nothing in the "
        f"markers list in {PYPROJECT_TOML.resolve()} selects its tests. A tier claim asserts that "
        f"a suite runs somewhere; declare the marker that selects it, or claim no tier"
    )
    assert spec.ci_lane is not None, (
        f"{name} claims tier membership ({claimed}) but declares no CI lane, so no job in "
        f"{CI_WORKFLOW.resolve()} runs its tests. A tier claim asserts that a suite runs "
        f"somewhere; declare the lane that runs it, or claim no tier"
    )


# --------------------------------------------------------------------------------------------
# One pytest test per assertion, parametrized over the registry so each record is its own case
# and each failure names exactly one record. A skip is reported as SKIPPED, not folded silently
# into a pass, so an undeclared coordinate stays visible in the test run - which is what keeps
# the claim-scaling auditable rather than merely quiet.
# --------------------------------------------------------------------------------------------


@pytest.mark.parametrize("spec", _REGISTERED_RECORDS, ids=_record_label)
def test_marker_is_registered_in_pyproject(spec: DataSourceSpec) -> None:
    if spec.marker is None:
        pytest.skip(f"{_registered_name(spec)} declares no marker")
    _assert_marker_is_registered_in_pyproject(_registered_name(spec), spec)


@pytest.mark.parametrize("spec", _REGISTERED_RECORDS, ids=_record_label)
def test_marker_is_a_required_marker(spec: DataSourceSpec) -> None:
    if spec.marker is None:
        pytest.skip(f"{_registered_name(spec)} declares no marker")
    _assert_marker_is_a_required_marker(_registered_name(spec), spec)


@pytest.mark.parametrize("spec", _REGISTERED_RECORDS, ids=_record_label)
def test_dev_requirements_file_exists(spec: DataSourceSpec) -> None:
    if spec.dev_requirements_file is None:
        pytest.skip(f"{_registered_name(spec)} declares no dev_requirements_file")
    _assert_dev_requirements_file_exists(_registered_name(spec), spec)


@pytest.mark.parametrize("spec", _REGISTERED_RECORDS, ids=_record_label)
def test_task_runner_entry_exists(spec: DataSourceSpec) -> None:
    if spec.task_runner_marker is None:
        pytest.skip(f"{_registered_name(spec)} declares no task_runner_marker")
    _assert_task_runner_entry_exists(_registered_name(spec), spec)


@pytest.mark.parametrize("spec", _REGISTERED_RECORDS, ids=_record_label)
def test_task_runner_entry_lists_requirements_file(spec: DataSourceSpec) -> None:
    if spec.task_runner_marker is None or spec.dev_requirements_file is None:
        pytest.skip(
            f"{_registered_name(spec)} does not declare both a task_runner_marker and a "
            f"dev_requirements_file, so there is no agreement between the two to check"
        )
    _assert_task_runner_entry_lists_requirements_file(_registered_name(spec), spec)


@pytest.mark.parametrize("spec", _REGISTERED_RECORDS, ids=_record_label)
def test_workflow_job_and_lane_token_are_wired(spec: DataSourceSpec) -> None:
    if spec.ci_lane is None:
        pytest.skip(f"{_registered_name(spec)} declares no CI lane")
    _assert_workflow_job_and_lane_token(_registered_name(spec), spec)


@pytest.mark.parametrize("spec", _REGISTERED_RECORDS, ids=_record_label)
def test_container_service_is_wired(spec: DataSourceSpec) -> None:
    if spec.provisioning is not DataSourceProvisioning.LOCAL_CONTAINER:
        pytest.skip(f"{_registered_name(spec)} does not use local-container provisioning")
    if spec.container_service is None:
        pytest.skip(f"{_registered_name(spec)} names no container_service")
    _assert_container_service_wired(_registered_name(spec), spec)


@pytest.mark.parametrize("spec", _REGISTERED_RECORDS, ids=_record_label)
def test_tier_claim_carries_evidence(spec: DataSourceSpec) -> None:
    if not spec.tiers:
        pytest.skip(f"{_registered_name(spec)} claims no tier")
    _assert_tier_claim_carries_evidence(_registered_name(spec), spec)


# --------------------------------------------------------------------------------------------
# Failure paths: synthetic, deliberately-broken declarations. Each proves that the matching
# assertion above actually fires, and that its message names the record, the missing element, and
# the file the element belongs in. Without these, a claim-scaled assertion is indistinguishable
# from one that returns early for every record in the registry.
#
# Where the record is one the registry itself accepts, it is registered inside the isolation seam,
# which exercises the same registration path a real declaration takes. Where the registry rejects
# it outright, it is only constructed - see `test_tier_claimed_with_no_lane_names_the_record`.
# --------------------------------------------------------------------------------------------


def _make_spec(**overrides: object) -> DataSourceSpec:
    defaults: Dict[str, Any] = dict(
        label="ghost-backend",
        public_name="Ghost Backend",
        marker="ghost_backend",
        provisioning=DataSourceProvisioning.LOCAL_FILE,
        ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="ghost_backend"),
    )
    defaults.update(overrides)
    return DataSourceSpec(**defaults)


def _ghost_name(spec: DataSourceSpec) -> str:
    """The name a record with no config class is given - the same shape the registry uses."""
    return _name_for_record(spec, None)


class TestWiringDriftFailurePaths:
    def test_missing_requirements_file_names_record_and_path(self) -> None:
        before = iter_data_sources()
        with isolated_registry():
            spec = _make_spec(
                label="ghost-reqs",
                marker="ghost_reqs",
                dev_requirements_file="reqs/requirements-dev-ghost-backend.txt",
            )
            register_data_source(spec)

            with pytest.raises(AssertionError) as excinfo:
                _assert_dev_requirements_file_exists(_ghost_name(spec), spec)

            message = str(excinfo.value)
            assert spec.dev_requirements_file is not None
            expected_path = (PROJECT_ROOT / spec.dev_requirements_file).resolve()
            assert "ghost-reqs" in message
            assert "reqs/requirements-dev-ghost-backend.txt" in message
            assert str(expected_path) in message
        assert iter_data_sources() == before

    def test_unknown_task_runner_key_names_record_and_path(self) -> None:
        before = iter_data_sources()
        with isolated_registry():
            spec = _make_spec(
                label="ghost-task-runner",
                marker="ghost_task_runner",
                dev_requirements_file="reqs/requirements-dev-mysql.txt",
                task_runner_marker="not_a_real_task_runner_key",
            )
            register_data_source(spec)

            with pytest.raises(AssertionError) as excinfo:
                _assert_task_runner_entry_exists(_ghost_name(spec), spec)

            message = str(excinfo.value)
            assert "ghost-task-runner" in message
            assert "not_a_real_task_runner_key" in message
            assert str(TASKS_PY.resolve()) in message
        assert iter_data_sources() == before

    def test_unknown_task_runner_key_fires_with_no_requirements_file_declared(self) -> None:
        """The half of the split that would otherwise never be reached.

        A record declaring a task-runner marker and no requirements file is exactly the shape the
        old single assertion could not express - and it is a real shape, not a hypothetical one:
        Amazon S3 has it. This proves the entry-exists half still fires for such a record, so the
        split bought expressiveness without dropping a check.
        """
        before = iter_data_sources()
        with isolated_registry():
            spec = _make_spec(
                label="ghost-task-runner-no-reqs",
                marker="ghost_task_runner_no_reqs",
                task_runner_marker="not_a_real_task_runner_key",
            )
            register_data_source(spec)
            assert spec.dev_requirements_file is None

            with pytest.raises(AssertionError) as excinfo:
                _assert_task_runner_entry_exists(_ghost_name(spec), spec)

            message = str(excinfo.value)
            assert "ghost-task-runner-no-reqs" in message
            assert "not_a_real_task_runner_key" in message
            assert str(TASKS_PY.resolve()) in message

            # ...and the second half stays silent for it, rather than asserting that `None` is
            # among the entry's requirement files. This is the assertion that fails, on a valid
            # key, if the split is ever collapsed back into one.
            _assert_task_runner_entry_lists_requirements_file(
                _ghost_name(spec), _make_spec(label="ghost-s3-shaped", task_runner_marker="spark")
            )
        assert iter_data_sources() == before

    def test_declared_requirements_file_absent_from_task_runner_entry_names_both(self) -> None:
        before = iter_data_sources()
        with isolated_registry():
            spec = _make_spec(
                label="ghost-task-runner-mismatch",
                marker="ghost_task_runner_mismatch",
                dev_requirements_file="reqs/requirements-dev-mysql.txt",
                task_runner_marker="spark",
            )
            register_data_source(spec)

            with pytest.raises(AssertionError) as excinfo:
                _assert_task_runner_entry_lists_requirements_file(_ghost_name(spec), spec)

            message = str(excinfo.value)
            assert "ghost-task-runner-mismatch" in message
            assert "reqs/requirements-dev-mysql.txt" in message
            assert str(TASKS_PY.resolve()) in message
        assert iter_data_sources() == before

    def test_unknown_workflow_job_names_record_and_path(self) -> None:
        before = iter_data_sources()
        with isolated_registry():
            spec = _make_spec(
                label="ghost-workflow-job",
                marker="ghost_workflow_job",
                ci_lane=CiLaneRef(
                    workflow_job="not-a-real-workflow-job", marker_token="ghost_workflow_job"
                ),
            )
            register_data_source(spec)

            with pytest.raises(AssertionError) as excinfo:
                _assert_workflow_job_and_lane_token(_ghost_name(spec), spec)

            message = str(excinfo.value)
            assert "ghost-workflow-job" in message
            assert "not-a-real-workflow-job" in message
            assert str(CI_WORKFLOW.resolve()) in message
        assert iter_data_sources() == before

    def test_absent_container_service_names_record_and_path(self) -> None:
        before = iter_data_sources()
        with isolated_registry():
            spec = _make_spec(
                label="ghost-container",
                marker="ghost_container",
                provisioning=DataSourceProvisioning.LOCAL_CONTAINER,
                container_service="not-a-real-container-service",
            )
            register_data_source(spec)

            with pytest.raises(AssertionError) as excinfo:
                _assert_container_service_wired(_ghost_name(spec), spec)

            message = str(excinfo.value)
            expected_path = COMPOSE_ROOT / "not-a-real-container-service" / "docker-compose.yml"
            assert "ghost-container" in message
            assert "not-a-real-container-service" in message
            assert str(expected_path.resolve()) in message
        assert iter_data_sources() == before

    def test_tier_claimed_with_no_lane_names_the_record(self) -> None:
        """The tier-evidence assertion, proven to fire.

        This ghost is constructed and never registered, deliberately: the registry rejects a tier
        claim with no lane at registration time, so it cannot be enrolled even inside the isolation
        seam. That makes this assertion the second line rather than the first - it is what would
        catch such a record if the registry's own validator ever regressed, and a second line no
        one has watched fail is not a second line.
        """
        spec = _make_spec(
            label="ghost-tier-no-lane",
            marker="ghost_tier_no_lane",
            ci_lane=None,
            tiers=frozenset({SupportTier.CURATED_SQL}),
        )

        with pytest.raises(AssertionError) as excinfo:
            _assert_tier_claim_carries_evidence(_ghost_name(spec), spec)

        message = str(excinfo.value)
        assert "ghost-tier-no-lane" in message
        assert "curated_sql" in message
        assert str(CI_WORKFLOW.resolve()) in message

    def test_tier_claimed_with_no_marker_names_the_record(self) -> None:
        """The other half of the same assertion, on the same terms."""
        spec = _make_spec(
            label="ghost-tier-no-marker",
            marker=None,
            tiers=frozenset({SupportTier.CANONICAL_EXPECTATIONS}),
        )

        with pytest.raises(AssertionError) as excinfo:
            _assert_tier_claim_carries_evidence(_ghost_name(spec), spec)

        message = str(excinfo.value)
        assert "ghost-tier-no-marker" in message
        assert "canonical_expectations" in message
        assert str(PYPROJECT_TOML.resolve()) in message

    @pytest.mark.parametrize(
        "overrides",
        [
            # Azure Blob Storage and the postgres-compatible flavors: a record that states
            # nothing beyond its identity and where it lives.
            pytest.param(
                dict(
                    label="ghost-declares-nothing",
                    marker=None,
                    ci_lane=None,
                    provisioning=DataSourceProvisioning.EXTERNAL_CREDENTIALS,
                ),
                id="declares-nothing",
            ),
            # Citus-shaped: local-container provisioning naming no compose service, which is
            # legal for a record claiming no tier and is why the container assertion fires on the
            # service rather than on the provisioning member.
            pytest.param(
                dict(
                    label="ghost-container-no-service",
                    marker=None,
                    ci_lane=None,
                    provisioning=DataSourceProvisioning.LOCAL_CONTAINER,
                ),
                id="local-container-with-no-service",
            ),
            # Amazon-S3-shaped: a real task-runner key and no requirements file. This is the
            # record the pre-split assertion could not express, so it is the control that fails if
            # the split is ever collapsed back into one.
            pytest.param(
                dict(
                    label="ghost-task-runner-only",
                    marker=None,
                    ci_lane=None,
                    provisioning=DataSourceProvisioning.EXTERNAL_CREDENTIALS,
                    task_runner_marker="spark",
                ),
                id="task-runner-key-with-no-requirements-file",
            ),
        ],
    )
    def test_an_undeclared_coordinate_is_not_demanded(self, overrides: Dict[str, Any]) -> None:
        """The negative control for the scaling itself.

        Without this, "each assertion fires only when the record declares the coordinate it checks"
        is asserted only by the assertions that fire, and every early return in this module would
        be unreachable from any test - the parametrized cases above skip before reaching them.
        Each shape here registers through the real path, so the registry agrees the declaration is
        well-formed, and then every assertion is run against it directly.
        """
        before = iter_data_sources()
        with isolated_registry():
            spec = _make_spec(**overrides)
            register_data_source(spec)
            name = _ghost_name(spec)

            for assertion in (
                _assert_marker_is_registered_in_pyproject,
                _assert_marker_is_a_required_marker,
                _assert_dev_requirements_file_exists,
                _assert_task_runner_entry_exists,
                _assert_task_runner_entry_lists_requirements_file,
                _assert_workflow_job_and_lane_token,
                _assert_container_service_wired,
                _assert_tier_claim_carries_evidence,
            ):
                assertion(name, spec)
        assert iter_data_sources() == before


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
