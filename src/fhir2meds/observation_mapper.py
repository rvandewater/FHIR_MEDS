import os
import json
from typing import Dict, Any
from fhir.resources.observation import Observation

def load_fhir_observations_from_dir(input_dir: str):
    observations = []
    for root, dirs, files in os.walk(input_dir):
        for fname in files:
            if fname.endswith(".json") or fname.endswith(".ndjson"):
                fpath = os.path.join(root, fname)
                with open(fpath) as f:
                    data = json.load(f)
                # If this is a single Observation resource
                print(f"Processing {fpath}")
                if data.get("resourceType") == "Observation":
                    try:
                        observations.append(Observation.parse_obj(data))
                    except Exception as e:
                        print(f"Failed to parse {fpath}: {e}")
                # If this is a Bundle, extract Observations from entries
                elif data.get("resourceType") == "Bundle":
                    for entry in data.get("entry", []):
                        resource = entry.get("resource", {})
                        if resource.get("resourceType") == "Observation":
                            try:
                                observations.append(Observation.parse_obj(resource))
                            except Exception as e:
                                print(f"Failed to parse entry in {fpath}: {e}")
    return observations

def observation_to_meds_event(obs: Observation, uuid_to_int: dict) -> Dict[str, Any]:
    # Extract subject_id using the patient UUID to int map
    subject_id = None
    if obs.subject and obs.subject.reference:
        patient_uuid = obs.subject.reference.split("/")[-1]
        subject_id = uuid_to_int.get(patient_uuid)
    if subject_id is None:
        print(f"Warning: No subject_id found for Observation with subject reference {getattr(obs.subject, 'reference', None)}")
        return None
    # Extract time
    time = getattr(obs, "effectiveDateTime", None) or getattr(obs, "effectivePeriod", None)
    # Extract code with vocabulary
    code = None
    if obs.code and obs.code.coding:
        coding = obs.code.coding[0]
        vocab = None
        if hasattr(coding, "system") and coding.system:
            if "loinc" in coding.system.lower():
                vocab = "LOINC"
            elif "snomed" in coding.system.lower():
                vocab = "SNOMED"
            elif "icd" in coding.system.lower():
                vocab = "ICD"
            else:
                vocab = coding.system.split("/")[-1].upper()
        if vocab:
            code = f"{vocab}//{coding.code}"
        else:
            code = coding.code
    # Extract numeric value
    numeric_value = getattr(obs, "valueQuantity", None)
    if numeric_value:
        numeric_value = numeric_value.value
    # Extract text value
    text_value = getattr(obs, "valueString", None)
    if not text_value and getattr(obs, "valueCodeableConcept", None):
        text_value = obs.valueCodeableConcept.text
    return {
        "subject_id": subject_id,
        "time": time,
        "code": code,
        "numeric_value": numeric_value,
        "text_value": text_value,
    } 