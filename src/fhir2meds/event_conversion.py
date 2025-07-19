import json


def build_patient_id_map(patient_ndjson_path):
    uuid_to_int = {}
    with open(patient_ndjson_path) as f:
        for line in f:
            if not line.strip():
                continue
            data = json.loads(line)
            if data.get("resourceType") == "Patient":
                uuid = data["id"]
                for ident in data.get("identifier", []):
                    if ident.get("system", "").endswith("/identifier/patient"):
                        try:
                            uuid_to_int[uuid] = int(ident["value"])
                        except Exception:
                            pass
    return uuid_to_int


def safe_str(val):
    if val is None:
        return None
    if isinstance(val, str):
        return val
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return str(val)


def generic_fhir_to_meds_event(resource, uuid_to_int):
    """
    Attempt to map any FHIR resource to a MEDS event dict.
    Extracts subject_id, time, code, numeric_value, text_value if possible.
    Returns None if subject_id cannot be resolved.
    """
    # TODO: check if proper parsing of resource, especially coding dict.
    # subject_id
    subject_id = None
    if resource.get("resourceType") == "Patient":
        # For Patient, subject_id is the integer patient ID
        for ident in resource.get("identifier", []):
            if ident.get("system", "").endswith("/identifier/patient"):
                try:
                    subject_id = int(ident["value"])
                except Exception:
                    pass
    elif "subject" in resource and resource["subject"].get("reference", "").startswith("Patient/"):
        patient_uuid = resource["subject"]["reference"].split("/")[-1]
        subject_id = uuid_to_int.get(patient_uuid)
    if subject_id is None:
        return None
    # time
    time = resource.get("effectiveDateTime") or resource.get("performedDateTime") or resource.get("onsetDateTime") or resource.get("issued") or resource.get("authoredOn") or resource.get("dateRecorded") or resource.get("dateAsserted") or resource.get("recordedDate") or resource.get("occurrenceDateTime") or resource.get("start") or resource.get("end")
    # code
    code = None
    if "code" in resource and resource["code"].get("coding"):
        coding = resource["code"]["coding"][0]
        system = None
        vocab = None
        if "system" in coding and coding["system"]:
            if "loinc" in coding["system"].lower():
                vocab = "LOINC"
            elif "snomed" in coding["system"].lower():
                vocab = "SNOMED"
            elif "icd" in coding["system"].lower():
                vocab = "ICD"
            else:
                # vocab = None
                vocab = coding["system"].split("/")[-1].upper()
        if vocab:
            code = f"{vocab}//{coding['code']}"
        else:
            code = coding["code"]
    elif "identifier" in resource and resource["identifier"]:
        code = resource["identifier"][0].get("value")
        vocab = resource["identifier"][0].get("system")
        if vocab:
            vocab = vocab.split("/")[-1].upper()
            code = f"{vocab}//{code}"
    # numeric_value
    numeric_value = None
    if "valueQuantity" in resource and resource["valueQuantity"]:
        try:
            numeric_value = float(resource["valueQuantity"]["value"])
        except Exception:
            numeric_value = None
    elif "value" in resource:
        try:
            numeric_value = float(resource["value"])
        except Exception:
            numeric_value = None
    # text_value
    text_value = resource.get("valueString")
    if not text_value and resource.get("valueCodeableConcept"):
        text_value = resource["valueCodeableConcept"].get("text")
    elif not text_value and resource.get("note"):
        # Some resources use 'note' for text
        notes = resource["note"]
        if isinstance(notes, list) and notes:
            text_value = notes[0].get("text")
    code = safe_str(code)
    text_value = safe_str(text_value)
    return {
        "subject_id": subject_id,
        "time": time,
        "code": code,
        "numeric_value": numeric_value,
        "text_value": text_value,
    }
