# fhir2meds

[![PyPI - Version](https://img.shields.io/pypi/v/fhir2meds)](https://pypi.org/project/fhir2meds/)
[![Documentation Status](https://readthedocs.org/projects/fhir2meds/badge/?version=latest)](https://fhir2meds.readthedocs.io/en/stable/?badge=stable)
[![codecov](https://codecov.io/gh/rvandewater/FHIR_MEDS/branch/main/graph/badge.svg?token=REPO_TOKEN)](https://codecov.io/gh/rvandewater/FHIR_MEDS)
[![tests](https://github.com/rvandewater/FHIR_MEDS/actions/workflows/tests.yaml/badge.svg)](https://github.com/rvandewater/FHIR_MEDS/actions/workflows/tests.yml)
[![code-quality](https://github.com/rvandewater/FHIR_MEDS/actions/workflows/code-quality-main.yaml/badge.svg)](https://github.com/rvandewater/FHIR_MEDS/actions/workflows/code-quality-main.yaml)
![python](https://img.shields.io/badge/-Python_3.12-blue?logo=python&logoColor=white)
[![license](https://img.shields.io/badge/License-MIT-green.svg?labelColor=gray)](https://github.com/rvandewater/FHIR_MEDS#license)
[![PRs](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/rvandewater/FHIR_MEDS/pulls)
[![contributors](https://img.shields.io/github/contributors/rvandewater/FHIR_MEDS.svg)](https://github.com/rvandewater/FHIR_MEDS/graphs/contributors)

---

## Overview

**fhir2meds** is a Python package for converting HL7 FHIR (v4/v5) resources—especially from the MIMIC-IV FHIR demo—into the [MEDS](https://github.com/Medical-Event-Data-Standard/meds) sharded Parquet format. It supports scalable, schema-validated event extraction and is designed for research and interoperability projects.

---

## Features
- Parses and processes all FHIR resource types (v4/v5)
- Robust mapping from FHIR Observation to MEDS event schema
- Handles patient ID resolution and vocabulary mapping
- Outputs sharded Parquet files, validated against the MEDS schema
- Parallelized, high-performance pipeline using polars and pyarrow
- Extensible: add mapping for new FHIR resource types easily
- Comprehensive test suite for FHIR resource parsing

---

## Installation

```bash
pip install fhir2meds
# or for local development
pip install -e .
```

---

## Usage

```bash
fhir2meds --input_dir path/to/mimic-fhir --output_dir path/to/meds_output --max_observations 100
```

- `--input_dir`: Directory containing FHIR .ndjson/.json files (e.g., MIMIC-IV FHIR demo)
- `--output_dir`: Output directory for MEDS Parquet shards
- `--max_observations`: (Optional) Limit number of observations for debugging

---

## Testing

Run the test suite (requires pytest):

```bash
PYTHONPATH=src pytest tests/
```

---

## References
- [HL7 FHIR Resource List](https://www.hl7.org/fhir/resourcelist.html)
- [MIMIC-IV FHIR Demo](https://physionet.org/content/mimic-iv-fhir-demo/2.0/)
- [MEDS Format & Schema](https://github.com/Medical-Event-Data-Standard/meds)
- [ETL_MEDS_Template](https://github.com/Medical-Event-Data-Standard/ETL_MEDS_Template)

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

## Contributing

Pull requests are welcome! For major changes, please open an issue first to discuss what you would like to change. 

from fhir2meds.metadata_writer import write_dataset_metadata, write_codes_metadata, write_subject_splits

write_dataset_metadata(
    output_dir=args.output_dir,
    dataset_name="MIMIC-IV FHIR Demo",
    dataset_version="2.0",
    etl_name="fhir2meds",
    etl_version="0.1.0",
    meds_version="0.4.0",
    license="MIT",
    location_uri=args.output_dir,
    description_uri=None,
)
write_codes_metadata(args.output_dir, all_events)
write_subject_splits(args.output_dir, all_events) 