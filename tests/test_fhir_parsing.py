import pytest
from fhir2meds.fhir_parser import get_sample_resources_by_type

FHIR_DIR = "mimic-fhir"

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
    # Add more as needed
]

@pytest.mark.parametrize("resource_type", RESOURCE_TYPES)
def test_fhir_resource_parsing(resource_type):
    samples_by_type = get_sample_resources_by_type(FHIR_DIR, n=3)
    if resource_type not in samples_by_type:
        pytest.skip(f"No {resource_type} resources found in {FHIR_DIR}")
    samples = samples_by_type[resource_type]
    assert len(samples) > 0, f"No samples found for {resource_type}"
    for resource in samples:
        assert resource["resourceType"] == resource_type
        assert "id" in resource
        # Optionally, add more asserts for key fields per resource type 