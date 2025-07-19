import argparse
import os

from fhir2meds.event_conversion import build_patient_id_map, generic_fhir_to_meds_event
from .fhir_parser import load_fhir_resources_by_type, filter_subject_resources_by_type
from .meds_writer import write_meds_sharded_parquet
from .metadata_writer import write_dataset_metadata, write_codes_metadata, write_subject_splits
import shutil
import logging


def main():
    parser = argparse.ArgumentParser(description="Convert all subject-associated FHIR resources to MEDS Parquet format.")
    parser.add_argument("--input_dir", required=True, help="Directory with FHIR .json/.ndjson files.")
    parser.add_argument("--output_dir", required=True, help="Output directory for MEDS Parquet shards.")
    parser.add_argument("--shard_size", type=int, default=10000, help="Number of rows per Parquet shard.")
    parser.add_argument("--max_events", type=int, default=None, help="Maximum number of events to process per resource type (for debugging).")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing output directory.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    if args.verbose:
        print(f"Loading FHIR resources from {args.input_dir}...")
    all_resources = load_fhir_resources_by_type(args.input_dir)
    subject_resources = filter_subject_resources_by_type(all_resources)
    if args.verbose:
        print(f"Loaded subject-associated resources for types: {list(subject_resources.keys())}")

    if args.overwrite:
        print("Overwriting existing output directory...")
        shutil.rmtree(args.output_dir)
        os.makedirs(args.output_dir)

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

if __name__ == "__main__":
    main()