import os
from pathlib import Path

import pytest

from fhir2meds.fhir_parser import get_sample_resources_by_type, load_event_config

# Point this at a local copy of the MIMIC-IV FHIR demo (https://physionet.org/content/mimic-iv-fhir-demo/).
FHIR_DIR = Path(os.environ.get("FHIR_MEDS_DEMO_DIR", "mimic-fhir"))
EVENT_CONFIG = load_event_config(fhir_version="R4")

# List of common FHIR resource types to test (extend as needed)
RESOURCE_TYPES = [
    "Patient",
    "Observation",
    "Condition",
    "Encounter",
    "Procedure",
    "MedicationRequest",
    "Medication",
    "AllergyIntolerance",
    "DiagnosticReport",
    "Immunization",
    "DocumentReference",
]


@pytest.fixture(scope="module")
def samples_by_type():
    if not FHIR_DIR.is_dir():
        pytest.skip(f"No local FHIR demo data at {FHIR_DIR}; set FHIR_MEDS_DEMO_DIR to enable.")
    return get_sample_resources_by_type(str(FHIR_DIR), event_config=EVENT_CONFIG, fhir_version="R4", n=3)


@pytest.mark.parametrize("resource_type", RESOURCE_TYPES)
def test_fhir_resource_parsing(resource_type, samples_by_type):
    if resource_type not in samples_by_type:
        pytest.skip(f"No {resource_type} resources found in {FHIR_DIR}")
    samples = samples_by_type[resource_type]
    assert len(samples) > 0, f"No samples found for {resource_type}"
    for resource in samples:
        assert resource["resourceType"] == resource_type
        assert "id" in resource
