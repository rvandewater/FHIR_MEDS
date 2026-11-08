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

The whole ETL is one [MEDS-Extract](https://github.com/mmcdermott/MEDS_extract) spec, registered as `FHIR`, so it
runs with MEDS-Extract's own command (`meds-extract-run spec=FHIR`, MEDS-Extract 0.8.1). It has three steps:

1. **`fhir_pre_meds`** (a MEDS-Extract pre-stage, the only FHIR-specific step): stream FHIR resources from
    bulk-export NDJSON or per-patient Bundles (optionally gzipped), flatten them into one typed parquet table per
    resource type (`configs/flatten.yaml`), assign integer `subject_id`s, resolve `medicationReference`s and parse
    FHIR timestamps.
2. **MEDS-Extract**'s canonical stages: turn those tables into MEDS events according to a declarative MESSY spec
    (`configs/messy.yaml`), then shard by subject, split into train/tuning/held_out and write MEDS metadata.
3. **`sumeds_enrich`** ([suMEDS](https://github.com/rvandewater/SumMEDS) 0.6.2, on by default): add vocabulary
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
meds-extract-run spec=FHIR output_dir=fhir_meds do_download=false input_dir=path/to/fhir \
	dataset_version=1.0 "overrides=['etl_metadata.dataset_name=MY_FHIR_DATA']"
```

Output layout:

```text
fhir_meds/
├── data/{train,tuning,held_out}/*.parquet   # the MEDS dataset
├── metadata/                                # dataset.json, codes.parquet, subject_splits.parquet
└── fhir_pre_meds/                           # flat per-resource-type parquet tables, subject_id_map.parquet
```

Common options (quote arguments with brackets, which shells such as zsh would otherwise expand):

- `input_dir`: Directory with FHIR files, searched recursively (with `do_download=false`)
- `dataset_key=demo`: download the MIMIC-IV FHIR demo (2.1.0) from PhysioNet instead; `dataset_key=mimic` downloads
    the credentialed MIMIC-IV FHIR release using `PHYSIONET_USER` / `PHYSIONET_PASSWORD`
- `"overrides=['etl_metadata.dataset_name=NAME']"`, `dataset_version=VERSION`: recorded in `metadata/dataset.json`
    (default `FHIR` and `unknown:<fhir2meds version>`)
- `"overrides=['++stage_cfg.fhir_version=R5']"`: use the R5 element names in `flatten.yaml`; likewise
    `keep_local_time=true` (keep local wall-clock times instead of converting offsets to UTC) and `max_events=N`
    (only read the first N resources per type, for debugging)
- `"overrides=['++stage_cfg.n_subjects_per_shard=1000']"`: MEDS-Extract stage options
- `stage_runner_fp=runner.yaml`: MEDS-Transforms stage runner config, e.g. for joblib parallelism
- `'skip_stages=[sumeds_enrich]'`: skip the suMEDS enrichment and keep the plain ETL output

A run that is started again resumes, and skips the stages it has finished; to convert again, use a new
`output_dir`. See the [usage documentation](docs/usage.md) for all options, and for upgrading from the deprecated
`fhir2meds` command, which now runs `meds-extract-run spec=FHIR` with its old arguments for one more release.

MEDS requires integer `subject_id`s. If a Patient has an integer identifier whose system ends in
`/identifier/patient` (MIMIC-IV's `subject_id`), it is used; otherwise the `subject_id` is a stable SHA-256-derived
integer of the Patient's logical id. The mapping is written to `fhir_pre_meds/subject_id_map.parquet`.

### Enriching code metadata with suMEDS

Vocabulary work lives in [suMEDS](https://github.com/rvandewater/SumMEDS) rather than in this converter. The
package ships the rules for it, [`src/fhir2meds/sumeds_rules.yaml`](src/fhir2meds/sumeds_rules.yaml), registered in
suMEDS's `sumeds.rules` entry-point group as `FHIR`, so `--rules FHIR` works for any suMEDS command. The spec's last
stage, suMEDS's `sumeds_enrich`, runs with these rules on `metadata/codes.parquet`, which is replaced only once the
enriched table is complete and stays valid MEDS code metadata. By default the LOINC and ICD data comes from suMEDS's
ontology packs, fetched from GitHub on first use and cached; no Athena download is needed. The stage's options are
those of `suMEDS-enrich`, spelled with underscores, and set for one run with `overrides=`:

```bash
OUT=fhir_meds
RUN="meds-extract-run spec=FHIR output_dir=$OUT do_download=false input_dir=path/to/fhir"
# Also look codes up in your own OHDSI Athena download (SNOMED, RxNorm, NDC, CPT-4, ... and OMOP concept ids):
$RUN "overrides=['++stage_cfg.athena_csv=/path/to/athena']"
# Fail the run if the enrichment fails:
$RUN "overrides=['++stage_cfg.strict=true']"
# The plain ETL, without suMEDS:
$RUN 'skip_stages=[sumeds_enrich]'
```

Enrichment is on by default and soft-fails: if it fails, for example on a hospital server without internet access,
`metadata/codes.parquet` stays as the ETL wrote it, and the run still succeeds. The warning, with the command to
enrich it later, is written to `$OUT/.logs/pipeline.log`, not to the console. Whether a build was enriched is
recorded in `metadata/dataset.json`, under `sumeds_enrichment.enriched` (`true`, or `false` with the `error` and the
`retry` command):

```bash
python -c "import json, sys; print(json.load(open(sys.argv[1]))['sumeds_enrichment']['enriched'])" \
	$OUT/metadata/dataset.json
```

Running `meds-extract-run` again does not retry a failed enrichment (the stage is done). To enrich a build
afterwards, with the rules and options of the run, and update `dataset.json` to match:

```bash
suMEDS-enrich $OUT
# A build made with 'skip_stages=[sumeds_enrich]' has no record of the rules:
suMEDS-enrich $OUT --rules FHIR
```

**Without internet.** On a cluster whose compute nodes are offline, fill the pack cache on a login node and run
offline, with the same cache directory, so that nothing waits on the network:

```bash
export XDG_CACHE_HOME=/shared/project/cache   # the same on the login and the compute nodes
suMEDS-enrich --prefetch --rules FHIR         # on the login node: the pinned pack release (~30 MB)
$RUN "overrides=['++stage_cfg.offline=true']" # on a compute node
```

A server that never has internet can use a copy of the
[sumeds-ontologies](https://github.com/rvandewater/sumeds-ontologies) packs directory instead, with
`SUMEDS_ONTOLOGY_SOURCE=/path/to/sumeds-ontologies/ontologies` for the run, or afterwards with
`suMEDS-enrich $OUT --ontology-source /path/to/sumeds-ontologies/ontologies --offline`.

The codes start with the resource type, so suMEDS cannot read the vocabulary from them on its own; the rules point
it at the `<VOCAB>//<code>` part. They validate LOINC codes and the ICD-10-CM, ICD-9-CM, ICD-10-PCS and ICD-9
procedure codes (MIMIC's undotted codes included) against suMEDS's versioned packs, and add the official title
where a code has no FHIR `display`, and the vocabulary code and its ancestors as `parent_codes`. On the MIMIC-IV
FHIR demo this gives parents to all 1,742 diagnosis and procedure codes (5,308 parent entries); on the Synthea test
sample (`tests/fixtures/synthea`), to 96 of its 111 LOINC codes (the LOINC pack leaves out terms under third-party
copyright, such as PHQ-2). SNOMED, RxNorm, CVX, NDC, CPT-4 and HCPCS codes have no public pack and are looked up
only with an Athena download (`athena_csv`), which on the MIMIC demo also fills the missing titles of 646 NDC codes. The same rules
work with `suMEDS … --rules FHIR` to build a privacy-masked code catalog of the output.

______________________________________________________________________

## Testing

```bash
pip install -e ".[tests]"
pytest
```

The tests run the ETL as users do, with `meds-extract-run spec=FHIR`, offline: the suMEDS stage reads tiny local
ontology packs built in `tests/conftest.py` (`SUMEDS_ONTOLOGY_SOURCE`).

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
