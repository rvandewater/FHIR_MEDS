import argparse
import os
import json
from fhir2meds.fhir_parser import load_fhir_resources_by_type, filter_subject_resources_by_type
from fhir2meds.meds_writer import write_meds_sharded_parquet
from fhir2meds.metadata_writer import write_dataset_metadata, write_codes_metadata, write_subject_splits

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
        vocab = None
        if "system" in coding and coding["system"]:
            if "loinc" in coding["system"].lower():
                vocab = "LOINC"
            elif "snomed" in coding["system"].lower():
                vocab = "SNOMED"
            elif "icd" in coding["system"].lower():
                vocab = "ICD"
            else:
                vocab = coding["system"].split("/")[-1].upper()
        if vocab:
            code = f"{vocab}//{coding['code']}"
        else:
            code = coding["code"]
    elif "identifier" in resource and resource["identifier"]:
        code = resource["identifier"][0].get("value")
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

def main():
    parser = argparse.ArgumentParser(description="Convert all subject-associated FHIR resources to MEDS Parquet format.")
    parser.add_argument("--input_dir", required=True, help="Directory with FHIR .json/.ndjson files.")
    parser.add_argument("--output_dir", required=True, help="Output directory for MEDS Parquet shards.")
    parser.add_argument("--shard_size", type=int, default=10000, help="Number of rows per Parquet shard.")
    parser.add_argument("--max_events", type=int, default=None, help="Maximum number of events to process per resource type (for debugging).")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging.")
    args = parser.parse_args()

    if args.verbose:
        print(f"Loading FHIR resources from {args.input_dir}...")
    all_resources = load_fhir_resources_by_type(args.input_dir)
    subject_resources = filter_subject_resources_by_type(all_resources)
    if args.verbose:
        print(f"Loaded subject-associated resources for types: {list(subject_resources.keys())}")

    # Build patient UUID to int map
    patient_ndjson_path = os.path.join(args.input_dir, "Patient.ndjson")
    uuid_to_int = build_patient_id_map(patient_ndjson_path)
    if args.verbose:
        print(f"Loaded {len(uuid_to_int)} patient UUID to integer ID mappings.")

    all_events = []
    for rtype, resources in subject_resources.items():
        if args.verbose:
            print(f"\nProcessing {len(resources)} {rtype} resources...")
        if args.max_events is not None and len(resources) > args.max_events:
            if args.verbose:
                print(f"Limiting to first {args.max_events} {rtype} resources for debugging.")
            resources = resources[:args.max_events]
        mapped_events = [generic_fhir_to_meds_event(res, uuid_to_int) for res in resources]
        events = [e for e in mapped_events if e is not None]
        filtered_out = len(mapped_events) - len(events)
        if args.verbose:
            print(f"Mapped {len(events)} events from {rtype}. Filtered out {filtered_out} events due to missing subject_id or other issues.")
        all_events.extend(events)

    print(f"Writing {len(all_events)} MEDS events to {args.output_dir}...")
    write_meds_sharded_parquet(all_events, args.output_dir, shard_size=args.shard_size, verbose=args.verbose)
    print("Done writing MEDS event data.")

    # Write MEDS metadata files
    print("Writing MEDS metadata files...")
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
    print("Done writing MEDS metadata.") 