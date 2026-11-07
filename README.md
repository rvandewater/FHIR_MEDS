<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="static/meds_on_fhir_logo_light.svg">
    <source media="(prefers-color-scheme: light)" srcset="static/meds_on_fhir_logo_light.svg">
    <img width="200" height="200" alt="MEDS Logo" src="static/meds_on_fhir_logo_light.svg">
  </picture>
</p>

## MEDS on FHIR

[![PyPI - Version](https://img.shields.io/pypi/v/fhir2meds)](https://pypi.org/project/fhir2meds/)
[![Documentation Status](https://readthedocs.org/projects/fhir2meds/badge/?version=latest)](https://fhir2meds.readthedocs.io/en/stable/?badge=stable)
[![codecov](https://codecov.io/gh/rvandewater/FHIR_MEDS/branch/main/graph/badge.svg?token=REPO_TOKEN)](https://codecov.io/gh/rvandewater/FHIR_MEDS)
[![tests](https://github.com/rvandewater/FHIR_MEDS/actions/workflows/tests.yaml/badge.svg)](https://github.com/rvandewater/FHIR_MEDS/actions/workflows/tests.yml)
[![code-quality](https://github.com/rvandewater/FHIR_MEDS/actions/workflows/code-quality-main.yaml/badge.svg)](https://github.com/rvandewater/FHIR_MEDS/actions/workflows/code-quality-main.yaml)
![python](https://img.shields.io/badge/-Python_3.12-blue?logo=python&logoColor=white)
[![license](https://img.shields.io/badge/License-MIT-green.svg?labelColor=gray)](https://github.com/rvandewater/FHIR_MEDS#license)
[![PRs](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/rvandewater/FHIR_MEDS/pulls)
[![contributors](https://img.shields.io/github/contributors/rvandewater/FHIR_MEDS.svg)](https://github.com/rvandewater/FHIR_MEDS/graphs/contributors)
[![DOI](https://zenodo.org/badge/1021967233.svg)](https://doi.org/10.5281/zenodo.16269017)

## Overview

> [!CAUTION]
> This package is in alpha development. Use at your own risk.

**MEDS on FHIR** (`fhir2meds`) converts HL7 FHIR (R4/R5) resources into the
[MEDS](https://github.com/Medical-Event-Data-Standard/meds) format. It is tested with the MIMIC-IV FHIR demo and
with [Synthea](https://github.com/synthetichealth/synthea) synthetic data.

The pipeline has three steps:

1. **pre-MEDS** (FHIR-specific): stream FHIR resources from bulk-export NDJSON or per-patient Bundles (optionally
    gzipped), flatten them into one typed parquet table per resource type (`configs/flatten.yaml`), assign integer
    `subject_id`s, resolve `medicationReference`s and parse FHIR timestamps.
2. **MEDS-Extract** ([`meds-extract-run`](https://github.com/mmcdermott/MEDS_extract) 0.8): turn those tables
    into MEDS events according to a declarative MESSY spec (`configs/messy.yaml`), then shard by subject, split into
    train/tuning/held_out and write MEDS metadata.
3. **suMEDS** ([`suMEDS-enrich`](https://github.com/rvandewater/SumMEDS) 0.6, on by default): add vocabulary
    titles and parents to `metadata/codes.parquet` (see
    [Enriching code metadata with suMEDS](#enriching-code-metadata-with-sumeds)). A failed enrichment (e.g.
    without internet) does not fail the run.

Documentation, including the [design choices](https://fhir2meds.readthedocs.io/en/latest/design/) behind the
mapping and a [review of Synthea datasets](https://fhir2meds.readthedocs.io/en/latest/synthea/) for testing, is at
[fhir2meds.readthedocs.io](https://fhir2meds.readthedocs.io). Build it locally with
`pip install -e ".[docs]" && mkdocs serve`.

## Features

- Reads FHIR bulk-data NDJSON (`*.ndjson[.gz]`) and resources/Bundles (`*.json[.gz]`, e.g. Synthea output)
- Streams input, so large exports don't need to fit in memory
- Codes of the form `<ResourceType>//<VOCAB>//<code>` (e.g. `Observation//LOINC//8867-4`), with code descriptions
    taken from the FHIR `display`, and vocabulary parents (and missing titles) from suMEDS
- `MEDS_BIRTH`, `MEDS_DEATH` and static demographic events, Observation components, encounter start/end
- Configurable: edit `flatten.yaml` to change which FHIR elements are read, and `messy.yaml` to change the
    resulting MEDS events

______________________________________________________________________

## Installation

```bash
pip install fhir2meds
# or for local development
pip install -e ".[tests]"
```

______________________________________________________________________

## Usage

```bash
fhir2meds raw_input_dir=path/to/fhir root_output_dir=output dataset_name=MY_FHIR_DATA dataset_version=1.0
```

Output layout:

```text
output/
├── pre_MEDS/        # flat per-resource-type parquet tables, subject_id_map.parquet, effective messy.yaml
└── MEDS_cohort/     # the MEDS dataset: data/{train,tuning,held_out}/*.parquet and metadata/
```

Common options (Hydra overrides):

- `raw_input_dir`: Directory with FHIR files, searched recursively (default: `${root_output_dir}/raw_input`)
- `do_download=true do_demo=true`: download the MIMIC-IV FHIR demo (2.1.0) from PhysioNet first. Without
    `do_demo`, the credentialed MIMIC-IV FHIR release is downloaded using `PHYSIONET_USER` / `PHYSIONET_PASSWORD`
- `do_overwrite=true`: replace existing `pre_MEDS/` and `MEDS_cohort/` output
- `fhir_version=R5`: use the R5 element names in `flatten.yaml`
- `keep_local_time=true`: keep local wall-clock times instead of converting offsets to UTC
- `etl_options={n_subjects_per_shard:1000,split_fracs:{train:0.8,tuning:0.1,held_out:0.1}}`: MEDS-Extract options
- `stage_runner_fp=runner.yaml`: MEDS-Transforms stage runner config, e.g. for joblib parallelism
- `max_events=N`: only read the first N resources per type (debugging)
- `do_enrich=false`: skip the suMEDS enrichment and keep the plain ETL output
- `enrich_args=[--athena-csv,/path/to/athena]`: further `suMEDS-enrich` options (see below)

MEDS requires integer `subject_id`s. If a Patient has an integer identifier whose system ends in
`/identifier/patient` (MIMIC-IV's `subject_id`), it is used; otherwise the `subject_id` is a stable SHA-256-derived
integer of the Patient's logical id. The mapping is written to `pre_MEDS/subject_id_map.parquet`.

### Enriching code metadata with suMEDS

Vocabulary work lives in [suMEDS](https://github.com/rvandewater/SumMEDS) rather than in this converter. The
package ships the rules for it, [`src/fhir2meds/sumeds_rules.yaml`](src/fhir2meds/sumeds_rules.yaml), registered in
suMEDS's `sumeds.rules` entry-point group as `FHIR`, so `--rules FHIR` works for any suMEDS command. `fhir2meds`
ends by running `suMEDS-enrich` with these rules on `MEDS_cohort/metadata/codes.parquet`, which is replaced only
once the enriched table is complete and stays valid MEDS code metadata. By default the LOINC and ICD data comes
from suMEDS's ontology packs, fetched from GitHub on first use and cached; no Athena download is needed. suMEDS
options go in `enrich_args`:

```bash
# Also look codes up in your own OHDSI Athena download (SNOMED, RxNorm, NDC, CPT-4, ... and OMOP concept ids):
fhir2meds raw_input_dir=path/to/fhir root_output_dir=output "enrich_args=[--athena-csv,/path/to/athena]"
# Use only already cached packs (no network):
fhir2meds raw_input_dir=path/to/fhir root_output_dir=output "enrich_args=[--offline]"
# Fail the run if the enrichment fails:
fhir2meds raw_input_dir=path/to/fhir root_output_dir=output "enrich_args=[--strict]"
# The plain ETL, without suMEDS:
fhir2meds raw_input_dir=path/to/fhir root_output_dir=output do_enrich=false
```

Enrichment is on by default and soft-fails: if it fails, for example on a hospital server without internet access,
`metadata/codes.parquet` stays as the ETL wrote it, a warning gives the command to enrich it later, and the run
still succeeds. Whether a build was enriched is recorded in `metadata/dataset.json`, under
`sumeds_enrichment.enriched` (`true`, or `false` with the `error` and the `retry` command). To enrich a build
afterwards, with the rules and options of the run, and update `dataset.json` to match:

```bash
suMEDS-enrich "$OUT/MEDS_cohort"
# Without internet, from a copy of the sumeds-ontologies packs directory:
suMEDS-enrich "$OUT/MEDS_cohort" --ontology-source /path/to/sumeds-ontologies/ontologies --offline
```

The codes start with the resource type, so suMEDS cannot read the vocabulary from them on its own; the rules point
it at the `<VOCAB>//<code>` part. They validate LOINC codes and the ICD-10-CM, ICD-9-CM, ICD-10-PCS and ICD-9
procedure codes (MIMIC's undotted codes included) against suMEDS's versioned packs, and add the official title
where a code has no FHIR `display`, and the vocabulary code and its ancestors as `parent_codes`. On the MIMIC-IV
FHIR demo this gives parents to all 1,742 diagnosis and procedure codes (5,308 parent entries); on the Synthea test
sample (`tests/fixtures/synthea`), to 96 of its 111 LOINC codes (the LOINC pack leaves out terms under third-party
copyright, such as PHQ-2). SNOMED, RxNorm, CVX, NDC, CPT-4 and HCPCS codes have no public pack and are looked up
only with `--athena-csv`, which on the MIMIC demo also fills the missing titles of 646 NDC codes. The same rules
work with `suMEDS … --rules FHIR` to build a privacy-masked code catalog of the output.

______________________________________________________________________

## Testing

```bash
pip install -e ".[tests]"
pytest
```

The tests run offline: the suMEDS step reads tiny local ontology packs built in `tests/conftest.py`.

______________________________________________________________________

## References

- [HL7 FHIR Resource List](https://www.hl7.org/fhir/resourcelist.html)
- [MIMIC-IV FHIR Demo](https://physionet.org/content/mimic-iv-fhir-demo/2.0/)
- [MEDS Format & Schema](https://github.com/Medical-Event-Data-Standard/meds)
- [ETL_MEDS_Template](https://github.com/Medical-Event-Data-Standard/ETL_MEDS_Template)
    R4 Resource List:
    https://hl7.org/fhir/R4/resourcelist.html

______________________________________________________________________

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

______________________________________________________________________

## Contributing

Pull requests are welcome! For major changes, please open an issue first to discuss what you would like to change.
