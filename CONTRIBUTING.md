# Contributing

`fhir2meds` converts FHIR resources to MEDS in two stages: a FHIR-specific **pre-MEDS** step that flattens
each resource type into a table, and a generic **MEDS-Extract 0.7** step that builds events from those tables.
[Design choices](docs/design.md) explains why; this page covers how to work on it.

## Where things live

| To change                                          | Edit                                                        |
| -------------------------------------------------- | ----------------------------------------------------------- |
| which FHIR elements are read into pre-MEDS tables  | `src/fhir2meds/configs/flatten.yaml`                        |
| which MEDS events are built from those tables      | `src/fhir2meds/configs/messy.yaml`                          |
| where the MIMIC-IV FHIR (demo) download comes from | `src/fhir2meds/configs/sources.yaml`                        |
| CLI defaults                                       | `src/fhir2meds/configs/main.yaml`                           |
| parsing, flattening, ids, times, references        | `src/fhir2meds/pre_MEDS.py`, `src/fhir2meds/fhir_parser.py` |
| how MEDS-Extract is invoked                        | `src/fhir2meds/extract.py`                                  |

Most mapping changes need only the two YAML files; [Customizing the mapping](docs/customizing.md) walks
through both.

## Editing `messy.yaml`

Event fields are [dftly](https://github.com/mmcdermott/dftly) expressions. Three things bite newcomers, in
rough order of how often:

1. **`$` is load-bearing.** `$value` reads the `value` column; a bare `value` is the string literal
    `"value"`. This fails silently — wrong data, no error.
2. **A null code component drops the row.** `f"Observation//{$code}"` discards every row with a null
    `code`. Coalesce with `?? 'UNK'` when the row must be kept.
3. **There is no null test.** dftly has no `is_null`. The working idiom is `X if $c == $c`, which is true for
    any present value (including `0`, `inf` and `NaN`) and null for a missing one. A bare `null` parses as
    the *string* `"null"`. Tracked in [dftly#113](https://github.com/mmcdermott/dftly/issues/113).

Also worth knowing: a table may declare **exactly one** `_table.join`, `_table.cols` entries are evaluated
in order so later ones may reference earlier ones, and adding a Duration to a Datetime must be written as
`anchor - (-$offset)::minutes` because `+` does not lower correctly.

## Development

The project uses [uv](https://docs.astral.sh/uv/) for its environment, lockfile, build, and commands.
`--all-extras` installs the test, docs and pre-commit dependencies.

```bash
uv sync --all-extras
uv run pre-commit install
uv run pre-commit run --all-files
uv run pytest
uv run mkdocs build --strict
```

A plain `pre-commit run` checks only staged files, so hooks legitimately skip when nothing matching is
staged. Use `--all-files` for a full repository check.

`pytest` runs offline against the Synthea fixtures in `tests/fixtures/synthea`. The tests that download the
MIMIC-IV FHIR demo and the public Synthea samples are skipped unless `FHIR_MEDS_RUN_DOWNLOAD_TESTS=1`; CI runs
them weekly in the `Integration (public datasets)` workflow, and it can be started by hand from the Actions
tab.

## Changing what the ETL emits

Any change to codes, timestamps or which rows are emitted is a change to everybody's downstream cohort.
State the measured effect in the PR — row counts, subject counts, code-vocabulary size before and after, on
the Synthea fixtures or the MIMIC-IV FHIR demo — rather than only describing the intent.

## Release

The version is the static `version` in `pyproject.toml`. Release from an up-to-date `main` with a clean
working tree, and bump and commit the version before creating the matching tag:

```bash
git switch main
git pull --ff-only
uv version --bump patch # 0.2.0 → 0.2.1; use minor or major for the other components
uv run pytest
uv build --clear
git add pyproject.toml uv.lock
git commit -m "Release $(uv version --short)"
VERSION=$(uv version --short)
git tag -a "$VERSION" -m "Release $VERSION"
git push origin main "$VERSION"
```

Tags are the bare version (`0.1.0`, not `v0.1.0`). The tag workflow rejects a tag that does not match
`pyproject.toml`, then publishes the distributions to PyPI and creates the GitHub release.

Run the block once per release. A tag that has been pushed is already published to PyPI, and PyPI never
accepts the same version twice, so if a later step fails, fix that step instead of rerunning the block.
