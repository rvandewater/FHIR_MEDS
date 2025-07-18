# fhir2meds

Convert HL7 FHIR (v4/v5) Observation resources to the MEDS sharded Parquet format. Designed for use with the MIMIC-IV FHIR demo dataset.

## Features
- Parses FHIR v4 and v5 Observation resources
- Maps to the MEDS event schema
- Outputs sharded Parquet files compatible with the MEDS standard

## Installation
```bash
pip install .
```

## Usage
```bash
fhir2meds --input_dir path/to/fhir/observations --output_dir path/to/meds_output
```

## References
- [MEDS Format & Schema](https://github.com/Medical-Event-Data-Standard/meds)
- [MIMIC-IV FHIR Demo](https://physionet.org/content/mimic-iv-fhir-demo/2.0/)
- [ETL_MEDS_Template](https://github.com/Medical-Event-Data-Standard/ETL_MEDS_Template) 