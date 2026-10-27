import logging
import os
import shutil
from collections import Counter
from pathlib import Path

import hydra
import meds
import polars as pl
from omegaconf import DictConfig

from . import MAIN_CFG, __version__, dataset_info
from .download import download_data
from .event_conversion import build_event, build_patient_id_map, resolve_medication_references
from .fhir_parser import filter_subject_resources_by_type, load_event_config, load_fhir_resources_by_type
from .meds_writer import write_meds_sharded_parquet
from .metadata_writer import write_codes_metadata, write_dataset_metadata, write_subject_splits

logger = logging.getLogger(__name__)

# Fix MAIN_CFG for hydra.main
MAIN_CFG_PATH = str(MAIN_CFG)
MAIN_CFG_PARENT = os.path.dirname(MAIN_CFG_PATH)
MAIN_CFG_STEM = os.path.splitext(os.path.basename(MAIN_CFG_PATH))[0]

MEDICATION_TYPES = ("MedicationRequest", "MedicationAdministration", "MedicationDispense", "MedicationStatement")


def clear_output_dir(root_output_dir: Path, keep: Path | None = None) -> None:
    """Delete everything in ``root_output_dir`` except ``keep`` (Hydra's live log directory)."""
    if not root_output_dir.exists():
        return
    for child in root_output_dir.iterdir():
        if keep is not None and child.resolve() == keep.resolve():
            continue
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()


@hydra.main(version_base=None, config_path=MAIN_CFG_PARENT, config_name=MAIN_CFG_STEM)
def main(cfg: DictConfig) -> None:
    shard_size = cfg.get("shard_size", 10000)
    max_events = cfg.get("max_events", None)
    verbose = cfg.get("verbose", False)

    raw_input_dir = Path(cfg.raw_input_dir)
    root_output_dir = Path(cfg.root_output_dir)
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO)

    # `overwrite` is kept as a deprecated alias of `do_overwrite`.
    if cfg.get("do_overwrite", False) or cfg.get("overwrite", False):
        logger.info("Removing existing output in the root output directory.")
        clear_output_dir(root_output_dir, keep=Path(cfg.log_dir) if cfg.get("log_dir") else None)
    root_output_dir.mkdir(parents=True, exist_ok=True)

    # Step 0: Data downloading
    if cfg.do_download:  # pragma: no cover
        if cfg.get("do_demo", False):
            logger.info("Downloading demo data.")
            download_data(raw_input_dir, dataset_info, do_demo=True)
        else:
            logger.info("Downloading data.")
            download_data(raw_input_dir, dataset_info)
    else:  # pragma: no cover
        logger.info("Skipping data download.")

    # Load event config for the selected FHIR version
    fhir_version = cfg.get("fhir_version", "R4")
    event_config = load_event_config(fhir_version=fhir_version)

    logger.info(f"Loading FHIR resources from {raw_input_dir}...")
    all_resources = load_fhir_resources_by_type(str(raw_input_dir), event_config, fhir_version)
    medications = all_resources.get("Medication", [])
    subject_resources = filter_subject_resources_by_type(all_resources)
    if not subject_resources.get("Patient"):
        raise ValueError(f"No Patient resources found in {raw_input_dir}; cannot assign MEDS subject_ids.")

    # Build patient logical id -> integer subject_id map from every Patient resource, wherever it was stored.
    uuid_to_int = build_patient_id_map(
        subject_resources["Patient"], cfg.get("subject_id_identifier_suffixes") or ("/identifier/patient",)
    )
    logger.info(f"Loaded {len(uuid_to_int)} patient id to subject_id mappings.")
    pl.DataFrame(
        {"patient_id": list(uuid_to_int.keys()), "subject_id": list(uuid_to_int.values())},
        schema={"patient_id": pl.Utf8, "subject_id": pl.Int64},
    ).write_csv(root_output_dir / "subject_id_map.csv")

    for rtype in MEDICATION_TYPES:
        n_resolved = resolve_medication_references(subject_resources.get(rtype, []), medications)
        if n_resolved:
            logger.info(f"Resolved medicationReference to a Medication code for {n_resolved} {rtype} resources.")

    all_events = []
    descriptions = {}
    for rtype, resources in subject_resources.items():
        if max_events is not None and len(resources) > max_events:
            logger.info(f"Limiting to first {max_events} {rtype} resources for debugging.")
            resources = resources[:max_events]
        rtype_config = event_config.get(rtype, event_config["default"])
        mapped_events = [build_event(res, rtype_config, uuid_to_int, event_config["default"]) for res in resources]
        dropped = Counter()
        for e in mapped_events:
            if e.get("subject_id") is None:
                dropped["no_subject_id"] += 1
            elif not e.get("code"):
                dropped["no_code"] += 1
            else:
                description = e.pop("description", None)
                if description and e["code"] not in descriptions:
                    descriptions[e["code"]] = str(description)
                all_events.append(e)
        if dropped:
            logger.warning(f"{rtype}: dropped {dict(dropped)} of {len(mapped_events)} events.")
        logger.info(f"Mapped {len(mapped_events) - sum(dropped.values())} events from {rtype}.")

    logger.info(f"Writing {len(all_events)} MEDS events to {root_output_dir}...")
    write_meds_sharded_parquet(all_events, str(root_output_dir), shard_size=shard_size, verbose=verbose)

    logger.info("Writing MEDS metadata files...")
    write_dataset_metadata(
        output_dir=str(root_output_dir),
        dataset_name=str(cfg.get("dataset_name") or dataset_info.dataset_name),
        dataset_version=str(cfg.get("dataset_version") or dataset_info.raw_dataset_version),
        etl_name="fhir2meds",
        etl_version=__version__,
        meds_version=meds.__version__,
        location_uri=str(root_output_dir),
    )
    write_codes_metadata(str(root_output_dir), all_events, descriptions)
    write_subject_splits(str(root_output_dir), all_events)
    logger.info("Done writing MEDS metadata.")


if __name__ == "__main__":
    main()
