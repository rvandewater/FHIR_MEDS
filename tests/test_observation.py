import os
import json
from fhir2meds.fhir_parser import load_fhir_observations_from_bundle
from fhir2meds.observation_mapper import observation_to_meds_event

def test_observation_mapping():
    # Example FHIR Observation JSON (minimal)
    obs_json = {
        "resourceType": "Observation",
        "id": "obs1",
        "subject": {"reference": "Patient/123"},
        "effectiveDateTime": "2020-01-01T12:00:00Z",
        "code": {"coding": [{"code": "LAB123"}]},
        "valueQuantity": {"value": 5.6}
    }
    # Write to temp bundle
    bundle = {
        "resourceType": "Bundle",
        "entry": [{"resource": obs_json}]
    }
    fname = "test_bundle.json"
    with open(fname, "w") as f:
        json.dump(bundle, f)
    obs = load_fhir_observations_from_bundle(fname)
    assert len(obs) == 1
    event = observation_to_meds_event(obs[0])
    assert event["subject_id"] == "123"
    assert event["time"] == "2020-01-01T12:00:00Z"
    assert event["code"] == "LAB123"
    assert event["numeric_value"] == 5.6
    os.remove(fname) 