# Usage

```bash
pip install fhir2meds
fhir2meds raw_input_dir=path/to/fhir root_output_dir=output dataset_name=MY_DATA dataset_version=1.0
```

fhir2meds is a [Hydra](https://hydra.cc) application, so every option is given as `key=value`.

## Inputs

`raw_input_dir` is searched recursively for:

- `*.ndjson`, `*.ndjson.gz`: FHIR bulk-data export files (e.g. MIMIC-IV FHIR, SMART bulk export samples).
- `*.json`, `*.json.gz`: single resources or `Bundle`s (e.g. Synthea's one Bundle per patient).

Files can be mixed freely; only each resource's `resourceType` matters.

## Outputs

```text
output/
├── pre_MEDS/
│   ├── Patient.parquet, Observation.parquet, ...   # flat per-type tables
│   ├── subject_id_map.parquet                       # FHIR Patient id -> MEDS subject_id
│   └── messy.yaml                                   # the MEDS-Extract spec used for this run
├── MEDS_cohort/
│   ├── data/{train,tuning,held_out}/*.parquet
│   └── metadata/{dataset.json,codes.parquet,subject_splits.parquet}
└── .logs/
```

Rows dropped by MEDS-Extract (e.g. null time) are counted per event in
`MEDS_cohort/convert_to_MEDS_events/.logs/`.

## Options

| option                            | default                                                      | description                                                                                                                                                          |
| --------------------------------- | ------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `root_output_dir`                 | required                                                     | output root                                                                                                                                                          |
| `raw_input_dir`                   | `${root_output_dir}/raw_input`                               | FHIR input directory                                                                                                                                                 |
| `dataset_name`, `dataset_version` | `$DATASET_NAME` / `FHIR`, `$RAW_DATASET_VERSION` / `unknown` | recorded in `dataset.json`                                                                                                                                           |
| `do_overwrite`                    | `false`                                                      | replace existing `pre_MEDS/` and `MEDS_cohort/` (the run refuses otherwise)                                                                                          |
| `do_download`, `do_demo`          | `false`                                                      | download MIMIC-IV FHIR from PhysioNet first; `do_demo=true` for the open demo (2.1.0), otherwise the credentialed release with `PHYSIONET_USER`/`PHYSIONET_PASSWORD` |
| `fhir_version`                    | `R4`                                                         | `R4` or `R5` element names                                                                                                                                           |
| `keep_local_time`                 | `false`                                                      | keep local wall-clock time instead of converting offsets to UTC                                                                                                      |
| `subject_id_identifier_suffixes`  | `["/identifier/patient"]`                                    | identifier systems whose integer value becomes `subject_id`                                                                                                          |
| `etl_options`                     | `null`                                                       | MEDS-Extract `etl:` options, e.g. `{n_subjects_per_shard:1000,split_fracs:{train:0.8,tuning:0.1,held_out:0.1}}`                                                      |
| `stage_runner_fp`                 | `null`                                                       | MEDS-Transforms stage runner YAML, e.g. for joblib parallelism (`pip install fhir2meds[local_parallelism]`)                                                          |
| `max_events`                      | `null`                                                       | read at most N resources per type (debugging)                                                                                                                        |

## Examples

MIMIC-IV FHIR demo, downloaded from PhysioNet:

```bash
fhir2meds root_output_dir=mimic_demo do_download=true do_demo=true \
	dataset_name=MIMIC_IV_FHIR_DEMO dataset_version=2.1.0
```

A Synthea output directory (default per-patient Bundles):

```bash
fhir2meds raw_input_dir=synthea/output/fhir root_output_dir=synthea_meds dataset_name=SYNTHEA
```

Very small datasets (fewer than ~3 patients) cannot populate all three default splits:

```bash
fhir2meds raw_input_dir=tiny root_output_dir=out "etl_options={split_fracs:{train:1.0}}"
```
