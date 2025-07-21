import argparse
import os
from pathlib import Path

from omegaconf import DictConfig

from .event_conversion import build_patient_id_map, build_event
from .fhir_parser import load_fhir_resources_by_type, filter_subject_resources_by_type, load_event_config
from .meds_writer import write_meds_sharded_parquet
from .metadata_writer import write_dataset_metadata, write_codes_metadata, write_subject_splits
import shutil
import logging
from . import MAIN_CFG, dataset_info
import hydra
from .download import download_data
import polars as pl
# Fix MAIN_CFG for hydra.main
import os
MAIN_CFG_PATH = str(MAIN_CFG)
MAIN_CFG_PARENT = os.path.dirname(MAIN_CFG_PATH)
MAIN_CFG_STEM = os.path.splitext(os.path.basename(MAIN_CFG_PATH))[0]

@hydra.main(version_base=None, config_path=MAIN_CFG_PARENT, config_name=MAIN_CFG_STEM)
def main(cfg: DictConfig) -> None:
    # parser = argparse.ArgumentParser(description="Convert all subject-associated FHIR resources to MEDS Parquet format.")
    # parser.add_argument("--input_dir", required=True, help="Directory with FHIR .json/.ndjson files.")
    # parser.add_argument("--output_dir", required=True, help="Output directory for MEDS Parquet shards.")
    # parser.add_argument("--shard_size", type=int, default=10000, help="Number of rows per Parquet shard.")
    # parser.add_argument("--max_events", type=int, default=None, help="Maximum number of events to process per resource type (for debugging).")
    # parser.add_argument("--verbose", action="store_true", help="Enable verbose logging.")
    # parser.add_argument("--overwrite", action="store_true", help="Overwrite existing output directory.")
    # args = parser.parse_args()
    shard_size = cfg.get("shard_size", 10000)
    max_events = cfg.get("max_events", None)
    verbose = cfg.get("verbose", False)
    overwrite = cfg.get("overwrite", False)

    raw_input_dir = Path(cfg.raw_input_dir)
    root_output_dir = Path(cfg.root_output_dir)
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO)
    raw_input_dir = Path(cfg.raw_input_dir)
    # pre_MEDS_dir = Path(cfg.pre_MEDS_dir)
    # MEDS_cohort_dir = Path(cfg.MEDS_cohort_dir)
    # stage_runner_fp = cfg.get("stage_runner_fp", None)
    root_output_dir = Path(cfg.root_output_dir)

    if cfg.do_overwrite and root_output_dir.exists():
        logging.info("Removing existing MEDS cohort directory.")
        shutil.rmtree(root_output_dir)

    # Step 0: Data downloading
    if cfg.do_download:  # pragma: no cover
        if cfg.get("do_demo", False):
            logging.info("Downloading demo data.")
            if isinstance(dataset_info, DictConfig):
                download_data(raw_input_dir, dataset_info, do_demo=True)
        else:
            logging.info("Downloading data.")
            if isinstance(dataset_info, DictConfig):
                download_data(raw_input_dir, dataset_info)
    else:  # pragma: no cover
        logging.info("Skipping data download.")

    # Load event config for the selected FHIR version
    fhir_version = cfg.get("fhir_version", "R4")
    event_config = load_event_config(fhir_version=fhir_version)

    if verbose:
        print(f"Loading FHIR resources from {raw_input_dir}...")
    # Fix Path to str for function arguments
    all_resources = load_fhir_resources_by_type(str(raw_input_dir), event_config, fhir_version)
    subject_resources = filter_subject_resources_by_type(all_resources)
    if verbose:
        print(f"Loaded subject-associated resources for types: {list(subject_resources.keys())}")

    if overwrite:
        print("Overwriting existing output directory...")
        shutil.rmtree(root_output_dir)
        os.makedirs(root_output_dir)

    # Build patient UUID to int map
    patient_ndjson_path = os.path.join(raw_input_dir, "Patient.ndjson")
    uuid_to_int = build_patient_id_map(patient_ndjson_path)
    if verbose:
        print(f"Loaded {len(uuid_to_int)} patient UUID to integer ID mappings.")
    pl.DataFrame(uuid_to_int).unpivot().write_csv(root_output_dir / "uuid_to_int.csv")
    all_events = []
    for rtype, resources in subject_resources.items():
        if verbose:
            print(f"\nProcessing {len(resources)} {rtype} resources...")
        if max_events is not None and len(resources) > max_events:
            if verbose:
                print(f"Limiting to first {max_events} {rtype} resources for debugging.")
            resources = resources[:max_events]
        mapped_events = [build_event(res, event_config.get(rtype, event_config['default']), uuid_to_int, event_config["default"]) for res in resources]
        # Filter out events with missing subject_id
        events = [e for e in mapped_events if e.get("subject_id") not in (None, "", "null")]
        filtered_out = len(mapped_events) - len(events)
        if verbose:
            print(f"Mapped {len(events)} events from {rtype}. Filtered out {filtered_out} events due to missing subject_id or other issues.")
            if filtered_out > 0:
                print(f"Example filtered event: {mapped_events[0] if mapped_events else 'None'}")
        all_events.extend(events)

    print(f"Writing {len(all_events)} MEDS events to {root_output_dir}...")
    write_meds_sharded_parquet(all_events, str(root_output_dir), shard_size=shard_size, verbose=verbose)
    print("Done writing MEDS event data.")

    # Write MEDS metadata files
    print("Writing MEDS metadata files...")
    write_dataset_metadata(
        output_dir=str(root_output_dir),
        dataset_name="MIMIC-IV FHIR Demo",
        dataset_version="2.0",
        etl_name="fhir2meds",
        etl_version="0.1.0",
        meds_version="0.4.0",
        license="MIT",
        location_uri=root_output_dir,
        description_uri=None,
    )
    write_codes_metadata(str(root_output_dir), all_events)
    write_subject_splits(str(root_output_dir), all_events)
    print("Done writing MEDS metadata.")

if __name__ == "__main__":
    main()