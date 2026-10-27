# Testing with Synthea

[Synthea](https://github.com/synthetichealth/synthea) generates realistic but fully synthetic patient histories as
FHIR. That makes it the natural non-MIMIC test bed for fhir2meds: its data can be committed, shared and downloaded
in CI without a data use agreement.

## What the test suite uses

| test | data | runs |
|---|---|---|
| `tests/test_synthea.py` (fixture tests) | 5 patients vendored in `tests/fixtures/synthea/`, in **both** bulk NDJSON and per-patient Bundle form | every CI run |
| `tests/test_synthea.py::test_external_synthea_dataset` | SMART `100-patients` (bulk) and the Synthea "latest" R4 sample (Bundles), pinned to commits | opt-in: `FHIR_MEDS_RUN_DOWNLOAD_TESTS=1`, weekly `integration.yaml` |
| `tests/e2e_demo_test.py` | MIMIC-IV FHIR demo 2.1.0 from PhysioNet | opt-in, weekly |

The fixture tests compare MEDS output against events computed **independently** from the raw JSON with the Python
standard library. They also check that the bulk and Bundle layouts give identical MEDS data. See
`tests/fixtures/synthea/README.md` for the fixture's provenance and how to regenerate it.

## Review of public Synthea datasets

Survey date: October 2026. Sizes and patient counts in the first table were measured by running fhir2meds on each
dataset. The other entries had their links and published metadata checked, but were not run.

### Run through fhir2meds

| dataset | layout | patients | size (zip / unzipped) | fhir2meds result | license |
|---|---|---|---|---|---|
| [SMART sample-bulk-fhir-datasets](https://github.com/smart-on-fhir/sample-bulk-fhir-datasets) `10-patients` | bulk NDJSON (R4) | 11 (10 alive + 1 deceased) | ~0.8 MB / ~9 MB | 8,792 events, 6 s | CC0 (data) |
| same, `100-patients` @ `47b9d0b` | bulk NDJSON (R4) | 120 | 15 MB / 181 MB | 178,985 events, 6.5 s | CC0 (data) |
| [Synthea sample data](https://synthetichealth.github.io/synthea-sample-data/) "latest" FHIR R4 @ `84f5980` | one transaction Bundle per patient, US Core 6.1.0 | 112 | 23 MB / 313 MB | 101,751 events, 6.3 s | free to use |

**Recommendations**

- **CI fixture:** a handful of patients vendored from the SMART 10-patient set (what `tests/fixtures/synthea` is).
  It's small, CC0 and pinned. Keep both layouts to test Bundle and bulk handling.
- **Integration test:** the SMART 100-patient set (bulk) and the Synthea "latest" sample (Bundles), always pinned
  to a commit. The "latest" sample is rebuilt on every Synthea release *without a fixed seed*, so its content
  changes between commits.
- **Scale test:** SMART `1000-patients` (183 MB zip / 1.3 GB per its README), or locally generated data (below).

### Other datasets (links and metadata checked, not run)

| dataset | notes | suitability |
|---|---|---|
| [Coherent Data Set](https://registry.opendata.aws/synthea-coherent-data/) | 1,278 patients, R4 Bundles plus DICOM, genomics, ECG `valueSampledData` and clinical notes; 9.2 GB zip; CC-BY 4.0; 2022 | Multimodal edge cases. Single bundles (1–4 MB each) can be fetched individually from `synthea-open-data.s3.amazonaws.com/coherent/unzipped/fhir/` |
| Synthea 1K samples (Sep 2019) on [synthea.mitre.org/downloads](https://synthea.mitre.org/downloads) | ~1,000 patients, R4 Bundles, ~85 MB zip; older Synthea and US Core | Older format variations |
| SyntheticMass v1/v2 | 1M patients, **FHIR STU3 / 1.8**, 21–28 GB | Only after STU3 support exists |
| COVID-19 10K/100K | CSV only | Not FHIR |
| AWS `synthea-omop` | OMOP CDM, not FHIR | For OMOP ETLs |

## Synthea quirks that matter for an ETL

These are handled by fhir2meds and covered by tests:

- **Reference styles differ by layout.** Bundles use `urn:uuid:<id>`, while bulk NDJSON uses `Patient/<id>`. Both
  are resolved.
- **No integer patient identifier.** Synthea Patients have UUID ids and identifiers like
  `https://github.com/synthetichealth/synthea`, MRN, SSN and passport, none of which is MIMIC's
  `/identifier/patient`. fhir2meds derives a stable SHA-256 subject id. Before this was fixed, Synthea input produced
  **zero** events.
- **The patient pointer field varies.** It is `subject` on Encounter, Condition, Observation, Procedure, … but
  `patient` on AllergyIntolerance, Immunization, Device, Claim, EpisodeOfCare.
- **Timestamps** use local offsets (`-04:00` / `-05:00`) in the SMART and Coherent sets and `+00:00` in recent
  samples. fhir2meds converts them to UTC. `birthDate` is date-only.
- **`Procedure.performedPeriod`** (not `performedDateTime`) and **multi-component Observations** (blood pressure)
  are common.
- **SMART dataset specifics.** `EpisodeOfCare`, `ServiceRequest`, `Specimen` and `MedicationDispense` are added by
  the SMART generation script, not by Synthea. Its Specimens have no collection time, so they are dropped. In the
  10-patient set, all `AllergyIntolerance`s reference patients that are missing from the export.
- **Practitioner/Organization references** are conditional (`Practitioner?identifier=…`). fhir2meds ignores them.

## Generating data locally

To generate a deterministic sample (Synthea ≥ 4.0 requires Java 17+):

```bash
curl -LO https://github.com/synthetichealth/synthea/releases/download/v4.0.0/synthea-with-dependencies.jar
java -jar synthea-with-dependencies.jar -s 42 -cs 42 -r 20250101 -p 10 \
  --exporter.baseDirectory ./synthea_out \
  --exporter.fhir.bulk_data true \
  --exporter.fhir.excluded_resources Claim,ExplanationOfBenefit,Provenance \
  --exporter.hospital.fhir.export false --exporter.practitioner.fhir.export false \
  Massachusetts
fhir2meds raw_input_dir=synthea_out/fhir root_output_dir=synthea_meds dataset_name=SYNTHEA
```

- `-s` seeds the patients, `-cs` the clinicians, and `-r` fixes the reference date. Pin the jar version as well,
  because the same seed gives different output across Synthea versions.
- Drop `--exporter.fhir.bulk_data true` to get Synthea's default per-patient Bundles instead.
- `-p` counts living patients, so deceased patients come on top. Add `--generate.only_alive_patients true` for an
  exact count.
