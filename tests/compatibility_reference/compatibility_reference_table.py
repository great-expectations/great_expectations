"""What the public support tiers mean, declared once.

Each data source is placed in one of three public tiers by the criteria it meets. This module
is the only place that states which declarations satisfy each criterion and which combination
of criteria yields each tier. Tier placement and the per-row display of criteria both read it,
so a row's tier and the criteria shown beside it cannot disagree.

Principles the declarations below encode:

* Tier names do not reuse the names of the declarations behind them. The top tier is a
  conjunction of two criteria and no single declaration names a conjunction, so printing a
  declaration name would imply a correspondence that cannot exist.
* The lowest tier asserts nothing about who wrote or maintains a connection path. Most paths in
  it are maintained in the shipped package.
* The datasource API criterion is a claim about managing a data source, made with connection
  testing neutralized. It says nothing about whether expectations run, so it is displayed but
  never lifts a row out of the lowest tier.
* The full-gallery criterion without the datasource API criterion is the middle tier. Such a
  data source has passed every expectation the shipped package registers, which is more than the
  middle tier asks for; placing it lowest because it lacks a criterion about managing a data
  source would understate it. No record produces that combination today, and the mapping is
  specified over every combination anyway: a mapping correct only for the combinations that
  happen to occur is wrong the first time one changes.
* A declaration no criterion names is legal and silent. Rows are unaffected by it.
* A row is one public name, which several records may share. A criterion is met by a row only
  when every record under that name declares it, so one tested variant cannot advertise coverage
  for an untested sibling. A declared continuous-integration lane is evidence of nothing: it
  means dependencies are installed and something runs, whereas a tier means a suite passes.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from enum import Enum
from pathlib import PurePosixPath
from typing import (
    TYPE_CHECKING,
    Dict,
    Final,
    FrozenSet,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
)

from tests.compatibility_reference import upstream_declarations
from tests.compatibility_reference.upstream_declarations import (
    CASE_DESCRIPTIONS,
    CONNECTION_PATH_DESCRIPTIONS,
    DataSourceProvisioning,
    SupportTier,
    UpstreamDeclarationError,
    UpstreamFacts,
)

if TYPE_CHECKING:
    from tests.compatibility_reference.upstream_declarations import DataSourceSpec


class PublicTier(Enum):
    FULLY_SUPPORTED = "Fully supported"
    TESTED = "Tested"
    BEST_EFFORT = "Best effort"


@dataclass(frozen=True)
class Criterion:
    key: str
    label: str
    """The public phrase printed on a row."""
    declarations: FrozenSet[SupportTier]
    """The declarations that satisfy this criterion. Any one of them suffices."""


GALLERY_KEY: Final = "gallery"
EXPECTATION_SUITE_KEY: Final = "expectation_suite"
DATASOURCE_API_KEY: Final = "datasource_api"

CRITERIA: Final[Tuple[Criterion, ...]] = (
    Criterion(
        key=GALLERY_KEY,
        label="Every shipped expectation",
        declarations=frozenset({SupportTier.GALLERY}),
    ),
    Criterion(
        key=EXPECTATION_SUITE_KEY,
        label="Expectation suite",
        declarations=frozenset({SupportTier.CANONICAL_EXPECTATIONS, SupportTier.CURATED_SQL}),
    ),
    Criterion(
        key=DATASOURCE_API_KEY,
        label="Datasource API contract",
        declarations=frozenset({SupportTier.FLUENT_API}),
    ),
)
"""Ordered; the display order of the criteria cell."""


def criteria_met_by(declarations: Iterable[SupportTier]) -> FrozenSet[str]:
    """The criterion keys satisfied by a collection of declarations.

    A declaration no criterion names contributes nothing and is not an error.
    """
    held = frozenset(declarations)
    return frozenset(
        criterion.key for criterion in CRITERIA if not criterion.declarations.isdisjoint(held)
    )


def tier_for(met: FrozenSet[str]) -> PublicTier:
    """The public tier yielded by a set of met criterion keys.

    Total over every subset of the criterion keys, including combinations no record produces.
    A key that names no criterion is ignored.
    """
    gallery = GALLERY_KEY in met
    suite = EXPECTATION_SUITE_KEY in met
    api = DATASOURCE_API_KEY in met
    if gallery and api:
        return PublicTier.FULLY_SUPPORTED
    if gallery or suite:
        return PublicTier.TESTED
    return PublicTier.BEST_EFFORT


@dataclass(frozen=True)
class PublishedRow:
    public_name: str
    specs: Tuple[DataSourceSpec, ...]
    """Every record carrying this public name, ordered by label."""
    criteria_met: FrozenSet[str]
    """Criterion keys every record in the row declares."""
    criteria_partial: FrozenSet[str]
    """Met, but with a recorded exclusion inside that criterion's suite.

    Assembly fills it: a met criterion is partial when a contributing record excludes a case
    under a declaration that satisfies the criterion, or, for the datasource API criterion,
    when a fluent type a contributing record names has excluded cases. A criterion that is not
    met is never partial, because it claims nothing for an exclusion to qualify. Assembly only
    marks the criterion; saying which case is excluded, and why, is left to the note derivation.
    """
    tier: PublicTier
    notes: Tuple[str, ...]
    """Where variants disagree about a criterion, one note per such criterion, in criterion
    order, naming the variants that fall short by what they connect to."""


def _variant_description(spec: DataSourceSpec) -> str:
    """A reader-facing phrase for one record, from the fluent types it declares.

    Neither the harness label (internal) nor the public name (shared by every variant) can tell
    two variants apart to a reader, and a type literal is an internal identifier, so the phrase
    comes from the declared description of each type. A record reached through several types is
    described by all of them.
    """
    if not spec.fluent_types:
        raise UpstreamDeclarationError(
            f"Records under {spec.public_name!r} disagree about a criterion, but the record "
            f"labelled {spec.label!r} declares no fluent datasource type, so nothing user-facing "
            f"can tell it apart from its siblings. Declare the type it is reached through."
        )
    undescribed = sorted(spec.fluent_types - CONNECTION_PATH_DESCRIPTIONS.keys())
    if undescribed:
        raise UpstreamDeclarationError(
            f"The record labelled {spec.label!r} declares fluent datasource type(s) {undescribed} "
            f"that CONNECTION_PATH_DESCRIPTIONS does not describe; an internal identifier must "
            f"not be printed in its place."
        )
    return " and ".join(sorted(CONNECTION_PATH_DESCRIPTIONS[t] for t in spec.fluent_types))


def _partial_criteria(
    met: FrozenSet[str], ordered: Tuple[DataSourceSpec, ...], facts: UpstreamFacts
) -> FrozenSet[str]:
    """The met criteria that carry a recorded per-case exclusion."""
    partial = set()
    for criterion in CRITERIA:
        if criterion.key not in met:
            continue
        recorded = any(
            exclusions and tier in criterion.declarations
            for spec in ordered
            for tier, exclusions in spec.tier_case_exclusions.items()
        )
        fluent = criterion.key == DATASOURCE_API_KEY and any(
            facts.fluent_case_exclusions.get(fluent_type)
            for spec in ordered
            for fluent_type in spec.fluent_types
        )
        if recorded or fluent:
            partial.add(criterion.key)
    return frozenset(partial)


def _assemble_row(
    public_name: str, specs: Iterable[DataSourceSpec], facts: UpstreamFacts
) -> PublishedRow:
    ordered = tuple(sorted(specs, key=lambda spec: spec.label))
    # One call per record, then an intersection. Calling it on the union of every record's
    # declarations would let two records that each lack a criterion jointly satisfy it.
    per_record = [criteria_met_by(spec.tiers) for spec in ordered]
    met = frozenset.intersection(*per_record)
    notes: List[str] = []
    for criterion in CRITERIA:
        short = [
            spec
            for spec, held in zip(ordered, per_record, strict=True)
            if criterion.key not in held
        ]
        if short and criterion.key in frozenset.union(*per_record):
            meeting = [spec for spec in ordered if spec not in short]
            for spec in short:
                shared = sorted(
                    spec.fluent_types & frozenset().union(*(m.fluent_types for m in meeting))
                )
                if shared:
                    raise UpstreamDeclarationError(
                        f"Records under {public_name!r} disagree about {criterion.label!r}, but "
                        f"the record labelled {spec.label!r} shares fluent datasource type(s) "
                        f"{shared} with a record that meets it, so no public-facing description "
                        f"distinguishes the variant that falls short from the ones that do not. "
                        f"Give the records distinct fluent types, or declare the criterion "
                        f"consistently across them."
                    )
            described = {
                _single_line(
                    _variant_description(spec),
                    f"The variant description in a disagreement note for {public_name!r}",
                )
                for spec in short
            }
            variants = "; ".join(sorted(described))
            notes.append(f"{criterion.label}: not met for {variants}.")
    row = PublishedRow(
        public_name=public_name,
        specs=ordered,
        criteria_met=met,
        criteria_partial=_partial_criteria(met, ordered, facts),
        tier=tier_for(met),
        notes=tuple(notes),
    )
    # Derived qualifications go after the disagreement notes, never in place of them.
    return replace(row, notes=row.notes + row_notes(row, facts))


def assemble_rows(facts: UpstreamFacts) -> Tuple[PublishedRow, ...]:
    """One row per distinct public name, ordered case-insensitively by that name.

    Raises:
        ValueError: when the registry yields no record, so no row, or when a record carries no
            public name. An empty table would read as a data source list that legitimately
            shrank, and a dropped record is how a data source vanishes from a page nothing checks.
    """
    if not facts.specs:
        raise ValueError(
            "The data source registry yields no record, so there is no row to publish. An empty "
            "table would claim GX supports nothing; check that the registry is populated."
        )
    by_name: Dict[str, List[DataSourceSpec]] = {}
    for spec in facts.specs:
        if not spec.public_name or not spec.public_name.strip():
            raise ValueError(
                f"The record labelled {spec.label!r} carries no public name. It cannot be "
                f"dropped, since a dropped record disappears from the published table unnoticed, "
                f"and no name can be derived for it."
            )
        if spec.public_name != spec.public_name.strip():
            raise ValueError(
                f"The public name {spec.public_name!r} (record labelled {spec.label!r}) has "
                f"leading or trailing whitespace, so it would publish a row that looks identical "
                f"to one without. Remove the whitespace from the declaration."
            )
        by_name.setdefault(spec.public_name, []).append(spec)
    folded: Dict[str, str] = {}
    for name in sorted(by_name):
        if folded.setdefault(name.casefold(), name) != name:
            raise ValueError(
                f"The public names {folded[name.casefold()]!r} and {name!r} differ only in "
                f"letter case, so they would publish two rows that look like one. Declare one "
                f"spelling for the data source."
            )
    return tuple(
        _assemble_row(name, by_name[name], facts)
        for name in sorted(by_name, key=lambda name: (name.casefold(), name))
    )


_LINE_BREAKS: Final = ("\n", "\r", "\x0b", "\x0c", "\x85", "\u2028", "\u2029")


def _single_line(text: str, what: str) -> str:
    """Return ``text`` unchanged, or fail if it spans more than one line.

    A line break inside a table cell breaks the row, and no escaping can repair that, so a
    declared string containing one is a failure to fix at its source rather than a row to mangle.
    """
    if any(character in text for character in _LINE_BREAKS):
        raise ValueError(
            f"{what} contains a line break, which would break the table row it is printed in. "
            f"Rewrite it as a single line where it is declared: {text!r}"
        )
    return text


def covered_but_unable_to_claim_notes(row: PublishedRow, facts: UpstreamFacts) -> Tuple[str, ...]:
    """Say, for a record the fluent suite covers but whose declaration cannot claim a tier, both
    what the suite verifies and that what is missing is a declaration.

    A reader told only that the criterion is not shown as met would infer the suite does not
    cover the data source, which is false: the suite runs, and the record just declares no lane
    for it.
    """
    if not any(spec.label in facts.covered_but_unable_to_claim for spec in row.specs):
        return ()
    return (
        "The datasource API contract is verified for this data source, but that criterion is "
        "not shown as met because no continuous-integration lane is declared for it; what is "
        "missing is that declaration, not test evidence.",
    )


def _describe_type(fluent_type: str) -> str:
    try:
        return CONNECTION_PATH_DESCRIPTIONS[fluent_type]
    except KeyError:
        raise UpstreamDeclarationError(
            f"The fluent datasource type {fluent_type!r} carries a recorded case exclusion but "
            f"CONNECTION_PATH_DESCRIPTIONS does not describe it; an internal identifier must not "
            f"be printed in its place."
        ) from None


def _describe_case(case: str) -> str:
    try:
        return CASE_DESCRIPTIONS[case]
    except KeyError:
        raise UpstreamDeclarationError(
            f"The case {case!r} carries a recorded exclusion but CASE_DESCRIPTIONS does not "
            f"describe it; a case key must not be printed in its place."
        ) from None


def _exclusions_of_criterion(
    row: PublishedRow, criterion: Criterion, facts: UpstreamFacts
) -> Iterator[Tuple[str, str, str]]:
    """Each ``(scope, case, reason)`` recorded inside one criterion's suite for the row.

    When a row has several records, or a record several types, each exclusion is scoped to the
    variant it belongs to: one variant's exclusion must not read as the whole data source's.
    """
    several = len(row.specs) > 1
    for spec in row.specs:
        scope = _variant_description(spec) if several else ""
        for tier, cases in spec.tier_case_exclusions.items():
            if tier in criterion.declarations:
                for case, reason in cases.items():
                    yield scope, case, reason
        if criterion.key != DATASOURCE_API_KEY:
            continue
        for fluent_type in sorted(spec.fluent_types):
            scoped = several or len(spec.fluent_types) > 1
            typed_scope = _describe_type(fluent_type) if scoped else ""
            for case, reason in facts.fluent_case_exclusions.get(fluent_type, {}).items():
                yield typed_scope, case, reason


def recorded_exclusion_notes(row: PublishedRow, facts: UpstreamFacts) -> Tuple[str, ...]:
    """Name each case a criterion's suite does not run for the row, with the recorded reason.

    Only criteria the row meets and that assembly marked partial are described, so a criterion
    the row does not claim carries no note about a suite it does not claim. Cases sharing a
    scope and a reason are named together.

    A case is named by its user-facing description, never by its key. Groups are ordered by the
    smallest case key in each, and the cases within a group are listed in key order, so rewording
    a description never reorders a note.
    """
    found: Dict[Tuple[int, str, str], Dict[str, str]] = {}
    for position, criterion in enumerate(CRITERIA):
        if criterion.key not in row.criteria_partial:
            continue
        for scope, case, reason in _exclusions_of_criterion(row, criterion, facts):
            if scope:
                _single_line(scope, f"The variant description for {row.public_name!r}")
            description = _single_line(
                _describe_case(case), f"The description of case {case!r} for {row.public_name!r}"
            )
            _single_line(reason, f"The recorded reason for case {case!r} of {row.public_name!r}")
            found.setdefault((position, scope, reason), {})[case] = description
    notes = []
    for (position, scope, reason), cases in sorted(
        found.items(), key=lambda item: (item[0][0], item[0][1], min(item[1]), item[0][2])
    ):
        described = [cases[case] for case in sorted(cases)]
        heading = CRITERIA[position].label + (f", {scope}" if scope else "")
        if len(described) == 1:
            named = f"not run for {described[0]}"
        else:
            named = f"not run for {', '.join(described[:-1])} and {described[-1]}"
        notes.append(f"{heading}: {named}. Recorded reason: {reason}")
    return tuple(notes)


def uncovered_connection_paths_note(facts: UpstreamFacts) -> Optional[str]:
    """The footnote naming connection paths that ship but that no row covers, printed once.

    Returns None when there are none. Fails, naming the path, if a path has no user-facing
    description: printing the raw type literal would put an internal identifier on a public
    page, and omitting it would recreate the invisible gap this footnote exists to show.
    """
    if not facts.fluent_types_named_by_no_record:
        return None
    missing = sorted(facts.fluent_types_named_by_no_record - CONNECTION_PATH_DESCRIPTIONS.keys())
    if missing:
        raise UpstreamDeclarationError(
            f"The connection paths no record names include {missing}, which "
            f"CONNECTION_PATH_DESCRIPTIONS does not describe. Add a user-facing description "
            f"for each; an internal identifier must not be printed in its place."
        )
    described = sorted(
        CONNECTION_PATH_DESCRIPTIONS[t] for t in facts.fluent_types_named_by_no_record
    )
    return _single_line(
        "Connection paths GX ships that no row above covers: " + "; ".join(described) + ".",
        "The uncovered connection paths footnote",
    )


def managed_service_notes(row: PublishedRow, facts: UpstreamFacts) -> Tuple[str, ...]:
    """Say no lane exercises the real service, for a data source reachable only with credentials.

    Keyed on external-credential provisioning together with a recorded provisioning note.
    Provisioning alone is wrong: some credential-gated data sources do have lanes against the
    real service. The note alone is wrong too: the field is free text upstream and also holds,
    for example, the cost of running a local container.

    Known residual: an externally provisioned record that carries no recorded provisioning note
    gets no managed-service note, even though reaching it needs credentials this repository does
    not hold. The count is deliberately not stated here, since it would go stale; it follows from
    the declarations. The remedy lies upstream: a provisioning note on each such record, or a
    field that marks a managed service. When the record's owner adds either, the record is
    covered here, or this producer can key on the field and drop the free-text condition.
    """
    if not any(
        spec.provisioning is DataSourceProvisioning.EXTERNAL_CREDENTIALS
        and (spec.provisioning_note or "").strip()
        for spec in row.specs
    ):
        return ()
    return (
        "No continuous-integration lane exercises the real service: reaching it needs "
        "credentials this repository does not provision.",
    )


def version_bound_notes(row: PublishedRow, facts: UpstreamFacts) -> Tuple[str, ...]:
    """The version a lane attests to, taken verbatim from the declared map.

    This is the one note not derived from a declaration, because no record field carries the
    version a lane runs against.

    The note can go stale: if the database version a lane runs against changes, nothing here
    notices. Of the two guards that check the map, one fires on a key that has no published row
    and the other on a data source with no lane; neither catches a version change. The remedy is
    a field on the data source record carrying the version a lane runs against, filled in by
    whoever owns the lane. At that point this note becomes derivable and the map can go.
    """
    note = upstream_declarations.TESTED_VERSION_NOTES.get(row.public_name)
    if note is None:
        return ()
    return (_single_line(note, f"The version note for {row.public_name!r}"),)


def row_notes(row: PublishedRow, facts: UpstreamFacts) -> Tuple[str, ...]:
    """Every derived qualification for a row, in a fixed order."""
    return (
        *covered_but_unable_to_claim_notes(row, facts),
        *recorded_exclusion_notes(row, facts),
        *managed_service_notes(row, facts),
        *version_bound_notes(row, facts),
    )


# --- rendering --------------------------------------------------------------------------------

REGENERATION_COMMAND: Final = "invoke docs-tables --sync"
"""The one command that rewrites the generated table. The notice, the drift check's failure
message and the maintainer documentation all quote this string, so it is spelled once."""

GENERATED_PARTIAL_PATH: Final = PurePosixPath(
    "docs/docusaurus/docs/help/_data_source_support_table.md"
)
"""The one file the command above writes, relative to the repository root. The task runner and the
drift check both take the path from here, so the command and the check cannot name different
files."""

GENERATED_NOTICE: Final = (
    f"{{/* Generated file. Do not edit by hand. Regenerate with: {REGENERATION_COMMAND} */}}"
)
"""An MDX expression comment. The documentation site compiles every Markdown file as MDX, where
an HTML comment is a parse error rather than a comment."""

COLUMN_HEADERS: Final = ("Data source", "Support tier", "Criteria met", "Notes")
PARTIAL_SUFFIX: Final = " (with exceptions)"
NO_CRITERIA: Final = "No criteria met"
CELL_LINE_SEPARATOR: Final = "<br/>"
"""Joins the entries of one cell. Written directly, never passed through ``_escape_cell``: it is
the one piece of markup the table emits on purpose, and it is a fixed string, not dynamic."""

_MARKUP_CHARACTERS: Final = "\\`*_~[]<>{}|&"
_MARKUP_PATTERN: Final = re.compile("[" + re.escape(_MARKUP_CHARACTERS) + "]")


def _escape_cell(text: str) -> str:
    """Make one dynamic string safe inside an MDX table cell.

    Each character that Markdown or MDX would read as markup, an expression, a tag, a link, an
    entity or a cell boundary is preceded by a backslash, which renders it literally: the
    backslash itself, the backtick, ``*``, ``_``, ``~``, the square brackets, the angle
    brackets, the braces, the pipe and the ampersand. Every other character is left alone.
    Escaping prevents markup injection and a failed documentation build; it does not stop GitHub
    Flavored Markdown from autolinking a ``http://`` address, a ``www.`` address or an email
    address in free text, so a URL or an email address in a name or a reason may still render as
    a link.

    Raises ValueError on a line break, which no escaping can make safe in a table row.
    """
    _single_line(text, "A string printed in the table")
    return _MARKUP_PATTERN.sub(lambda match: "\\" + match.group(0), text)


def _criteria_cell(row: PublishedRow) -> str:
    entries = [
        _escape_cell(
            criterion.label + (PARTIAL_SUFFIX if criterion.key in row.criteria_partial else "")
        )
        for criterion in CRITERIA
        if criterion.key in row.criteria_met
    ]
    return CELL_LINE_SEPARATOR.join(entries) if entries else _escape_cell(NO_CRITERIA)


def _row_line(row: PublishedRow) -> str:
    cells = (
        _escape_cell(row.public_name),
        _escape_cell(row.tier.value),
        _criteria_cell(row),
        CELL_LINE_SEPARATOR.join(_escape_cell(note) for note in row.notes),
    )
    return "| " + " | ".join(cells) + " |"


def render_table(facts: UpstreamFacts) -> str:
    """The complete partial: notice, blank line, table, footnote, trailing newline.

    The output is a pure function of ``facts``: columns, criteria and notes have a fixed order
    and rows are ordered by public name, so identical declarations give identical bytes. The
    footnote is printed once, below the table, and omitted when no connection path is uncovered.
    """
    lines = [
        "| " + " | ".join(COLUMN_HEADERS) + " |",
        "| " + " | ".join("---" for _ in COLUMN_HEADERS) + " |",
        *(_row_line(row) for row in assemble_rows(facts)),
    ]
    blocks = [GENERATED_NOTICE, "\n".join(lines)]
    footnote = uncovered_connection_paths_note(facts)
    if footnote is not None:
        blocks.append(_escape_cell(footnote))
    return "\n\n".join(blocks) + "\n"
