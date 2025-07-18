import argparse
import os
import json
from fhir2meds.fhir_parser import load_fhir_observations_from_dir
from fhir2meds.observation_mapper import observation_to_meds_event
from fhir2meds.meds_writer import write_meds_sharded_parquet

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

def main():
    parser = argparse.ArgumentParser(description="Convert FHIR Observations to MEDS Parquet format.")
    parser.add_argument("--input_dir", required=True, help="Directory with FHIR Observation bundle JSON files.")
    parser.add_argument("--output_dir", required=True, help="Output directory for MEDS Parquet shards.")
    parser.add_argument("--shard_size", type=int, default=10000, help="Number of rows per Parquet shard.")
    parser.add_argument("--max_observations", type=int, default=None, help="Maximum number of FHIR Observations to process (for debugging).")
    args = parser.parse_args()

    print(f"Loading FHIR Observations from {args.input_dir}...")
    observations = load_fhir_observations_from_dir(args.input_dir)
    if args.max_observations is not None:
        if len(observations) > args.max_observations:
            print(f"Limiting to first {args.max_observations} observations for debugging.")
            observations = observations[:args.max_observations]
    print(f"Loaded {len(observations)} observations.")

    # Build patient UUID to int map
    patient_ndjson_path = os.path.join(args.input_dir, "Patient.ndjson")
    uuid_to_int = build_patient_id_map(patient_ndjson_path)
    print(f"Loaded {len(uuid_to_int)} patient UUID to integer ID mappings.")

    print("Mapping to MEDS events...")
    mapped_events = [observation_to_meds_event(obs, uuid_to_int) for obs in observations]
    events = [e for e in mapped_events if e is not None]
    filtered_out = len(mapped_events) - len(events)
    print(f"Mapped {len(events)} events. Filtered out {filtered_out} events due to missing subject_id or other issues.")

    print(f"Writing MEDS Parquet shards to {args.output_dir}...")
    write_meds_sharded_parquet(events, args.output_dir, shard_size=args.shard_size)
    print("Done.") 