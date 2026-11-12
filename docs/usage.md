# Usage

```bash
pip install fhir2meds
meds-extract-run spec=FHIR output_dir=fhir_meds do_download=false input_dir=path/to/fhir \
	dataset_version=1.0 "overrides=['etl_metadata.dataset_name=MY_DATA']"
```

The ETL is one [MEDS-Extract](https://github.com/mmcdermott/MEDS_extract) spec, registered as `FHIR`
(`src/fhir2meds/configs/messy.yaml`), so it runs with MEDS-Extract's own command, `meds-extract-run`. Every option
is given as `key=value`. Quote arguments with brackets: shells such as zsh would otherwise expand them.

## Inputs

`input_dir` is searched recursively for:

- `*.ndjson`, `*.ndjson.gz`: FHIR bulk-data export files (e.g. MIMIC-IV FHIR, SMART bulk export samples).
- `*.json`, `*.json.gz`: single resources or `Bundle`s (e.g. Synthea's one Bundle per patient).

Files can be mixed freely; only each resource's `resourceType` matters. Your FHIR files are never modified.

## Outputs

```text
fhir_meds/
├── data/{train,tuning,held_out}/*.parquet
├── metadata/{dataset.json,codes.parquet,subject_splits.parquet}   # codes.parquet enriched by suMEDS
├── fhir_pre_meds/
│   ├── Patient.parquet, Observation.parquet, ...   # flat per-type tables (empty for types the input lacks)
│   └── subject_id_map.parquet                       # FHIR Patient id -> MEDS subject_id
├── <stage>/                                         # each MEDS-Extract stage's own output and logs
├── .meds_extract_run/pipeline.yaml                  # the pipeline this run ran, for provenance
└── .logs/pipeline.log
```

`do_cleanup_intermediates=true` deletes the stage directories (`fhir_pre_meds/` included) once the run has
succeeded. Rows dropped by MEDS-Extract (e.g. null time) are counted per event in
`fhir_meds/convert_to_MEDS_events/.logs/`. A run that is started again resumes, and skips the stages it has
finished; to convert again from scratch, use a new `output_dir`.

## Options

| option                                           | default                       | description                                                                                                 |
| ------------------------------------------------ | ----------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `output_dir`                                     | required                      | where the MEDS dataset is written                                                                           |
| `do_download=false input_dir=DIR`                |                               | your FHIR files (see [Downloads](#downloads) for MIMIC-IV FHIR)                                             |
| `"overrides=['etl_metadata.dataset_name=NAME']"` | `FHIR`                        | recorded in `dataset.json`                                                                                  |
| `dataset_version=VERSION`                        | `unknown:<fhir2meds version>` | recorded in `dataset.json`                                                                                  |
| `'skip_stages=[sumeds_enrich]'`                  |                               | leave out the suMEDS enrichment (see below)                                                                 |
| `stage_runner_fp=runner.yaml`                    |                               | MEDS-Transforms stage runner YAML, e.g. for joblib parallelism (`pip install fhir2meds[local_parallelism]`) |
| `do_cleanup_intermediates=true`                  | `false`                       | delete the stage directories after a successful run                                                         |

Options of the `fhir_pre_meds` stage, set for one run with `"overrides=['++stage_cfg.<option>=<value>']"`:

| option                           | default                   | description                                                     |
| -------------------------------- | ------------------------- | --------------------------------------------------------------- |
| `fhir_version`                   | `R4`                      | `R4` or `R5` element names                                      |
| `keep_local_time`                | `false`                   | keep local wall-clock time instead of converting offsets to UTC |
| `subject_id_identifier_suffixes` | `["/identifier/patient"]` | identifier systems whose integer value becomes `subject_id`     |
| `max_events`                     | `null`                    | read at most N resources per type (debugging)                   |

The same syntax sets options of MEDS-Extract's stages, e.g. `"overrides=['++stage_cfg.n_subjects_per_shard=1000']"`,
and of the `sumeds_enrich` stage (below). The overrides reach every stage, which is why they need `++`. They pass
through a shell unquoted, so a value must not contain spaces or braces; for a mapping such as `split_fracs`, copy
the spec, set it in the copy's `etl:` block, and run `spec=./my_messy.yaml`.

## Enrichment with suMEDS, and sites without internet

The spec's last stage, suMEDS's `sumeds_enrich`, adds vocabulary parents (and titles where FHIR has no `display`) to
`metadata/codes.parquet`, with the rules this package registers as `FHIR`. It never fails the run: if it fails, for
example on a hospital server without internet, `codes.parquet` stays as MEDS-Extract wrote it, the warning is
written to `fhir_meds/.logs/pipeline.log`, and `metadata/dataset.json` records `sumeds_enrichment.enriched: false`
with the error and the retry command. Running `meds-extract-run` again does not retry it (the stage is done); this
does, with the rules and options of the run:

```bash
suMEDS-enrich fhir_meds
```

- **Without enrichment:** `'skip_stages=[sumeds_enrich]'`; `suMEDS-enrich fhir_meds --rules FHIR` enriches such a
    build later.
- **Fail instead:** `"overrides=['++stage_cfg.strict=true']"`.
- **Your own Athena download** (SNOMED, RxNorm, NDC, ... and OMOP concept ids):
    `"overrides=['++stage_cfg.athena_csv=/path/to/athena']"`.
- **Compute nodes without internet:** on a machine with internet and the same cache directory, run
    `XDG_CACHE_HOME=/shared/cache suMEDS-enrich --prefetch --rules FHIR`; then run the ETL with that
    `XDG_CACHE_HOME` and `"overrides=['++stage_cfg.offline=true']"`. A server that never has internet can use a
    copy of the packs directory: `SUMEDS_ONTOLOGY_SOURCE=/path/to/sumeds-ontologies/ontologies`.

## Downloads

MIMIC-IV FHIR demo (open access), downloaded from PhysioNet into `fhir_meds/.meds_extract_run/raw_input` (or
`download_dest_dir=`):

```bash
meds-extract-run spec=FHIR output_dir=mimic_demo_meds dataset_key=demo
```

The credentialed MIMIC-IV FHIR release, with `PHYSIONET_USER` / `PHYSIONET_PASSWORD` in the environment:

```bash
meds-extract-run spec=FHIR output_dir=mimic_meds dataset_key=mimic
```

## Examples

A Synthea output directory (default per-patient Bundles):

```bash
meds-extract-run spec=FHIR output_dir=synthea_meds do_download=false input_dir=synthea/output/fhir \
	"overrides=['etl_metadata.dataset_name=SYNTHEA']"
```

Very small datasets (fewer than ~3 patients) cannot populate all three default splits: copy
`src/fhir2meds/configs/messy.yaml`, add `split_fracs: {train: 1.0}` to its `etl:` block, and run it with
`spec=./messy.yaml`.

## Upgrading from the `fhir2meds` command

`fhir2meds`, `FHIR_MEDS` and `MEDS_ON_FHIR` are deprecated and will be removed in the next release. They now run
`meds-extract-run spec=FHIR` (and print the command), translating these arguments. Their output stays in
`ROOT/MEDS_cohort`; the flattened tables move from `ROOT/pre_MEDS` to `ROOT/MEDS_cohort/fhir_pre_meds`.

| `fhir2meds`                                                                          | `meds-extract-run spec=FHIR`                                                |
| ------------------------------------------------------------------------------------ | --------------------------------------------------------------------------- |
| `raw_input_dir=RAW root_output_dir=ROOT`                                             | `output_dir=ROOT/MEDS_cohort do_download=false input_dir=RAW`               |
| `do_download=true do_demo=true` / without `do_demo`                                  | `dataset_key=demo` / `dataset_key=mimic`                                    |
| `dataset_name=NAME` (or `$DATASET_NAME`)                                             | `"overrides=['etl_metadata.dataset_name=NAME']"`                            |
| `dataset_version=V` (or `$RAW_DATASET_VERSION`)                                      | `dataset_version=V`                                                         |
| `fhir_version=R5`, `keep_local_time`, `subject_id_identifier_suffixes`, `max_events` | `"overrides=['++stage_cfg.fhir_version=R5']"`, ...                          |
| `do_enrich=false`                                                                    | `'skip_stages=[sumeds_enrich]'`                                             |
| `enrich_args=[--strict]`, `[--offline]`, `[--athena-csv,DIR]`                        | `"overrides=['++stage_cfg.strict=true']"`, `offline=true`, `athena_csv=DIR` |
| `etl_options={...}`                                                                  | `"overrides=['++stage_cfg.<option>=<value>']"`, or a copy of the spec       |
| `do_overwrite=true`                                                                  | a new `output_dir`                                                          |

`dataset.json` now records `etl_name: MEDS_transforms` (and its version), as for every registered MEDS-Extract
spec; the fhir2meds version is part of `dataset_version`. The effective spec is no longer copied next to the
tables: `.meds_extract_run/pipeline.yaml` records the spec and the stages of the run.
