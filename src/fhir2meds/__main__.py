import logging
import os
import shutil
from pathlib import Path

import hydra
from omegaconf import DictConfig, OmegaConf
from sumeds.cli import build_enrich_parser, enrich_main

from . import MAIN_CFG, __version__
from .extract import download_raw_data, run_meds_extract, write_effective_spec
from .pre_MEDS import run_pre_MEDS

logger = logging.getLogger(__name__)

# Fix MAIN_CFG for hydra.main
MAIN_CFG_PATH = str(MAIN_CFG)
MAIN_CFG_PARENT = os.path.dirname(MAIN_CFG_PATH)
MAIN_CFG_STEM = os.path.splitext(os.path.basename(MAIN_CFG_PATH))[0]


@hydra.main(version_base=None, config_path=MAIN_CFG_PARENT, config_name=MAIN_CFG_STEM)
def main(cfg: DictConfig) -> None:
    """Run the full FHIR -> MEDS pipeline: (download) -> pre-MEDS flattening -> MEDS-Extract -> suMEDS."""
    raw_input_dir = Path(cfg.raw_input_dir)
    pre_MEDS_dir = Path(cfg.pre_MEDS_dir)
    MEDS_cohort_dir = Path(cfg.MEDS_cohort_dir)

    # suMEDS-enrich arguments; a later --rules in enrich_args overrides the packaged rules. Checked now, not
    # after the pipeline has run. A failed enrichment (e.g. no internet) leaves codes.parquet as it was,
    # records that in metadata/dataset.json and exits 0, as `suMEDS-extract` does; `enrich_args=[--strict]`
    # fails instead.
    codes_fp = str(MEDS_cohort_dir.resolve() / "metadata" / "codes.parquet")
    enrich_args = [str(arg) for arg in cfg.enrich_args]
    enrich_argv = [codes_fp, "-o", codes_fp, "--rules", "FHIR", *enrich_args]
    if {"--soft-fail", "--strict"}.isdisjoint(enrich_args):
        enrich_argv.append("--soft-fail")
    if cfg.do_enrich:
        build_enrich_parser().parse_args(enrich_argv)

    if cfg.do_overwrite:
        for d in (pre_MEDS_dir, MEDS_cohort_dir):
            if d.exists():
                logger.info(f"Removing existing {d}.")
                shutil.rmtree(d)
    elif MEDS_cohort_dir.exists() and any(MEDS_cohort_dir.iterdir()):
        raise FileExistsError(f"{MEDS_cohort_dir} is not empty; pass do_overwrite=true to replace it.")

    # Step 0: Data downloading
    if cfg.do_download:  # pragma: no cover
        logger.info(f"Downloading {'demo ' if cfg.do_demo else ''}data to {raw_input_dir}.")
        download_raw_data(raw_input_dir, do_demo=cfg.do_demo)
    else:
        logger.info("Skipping data download.")

    # Step 1: FHIR -> flat, typed pre-MEDS tables (the only FHIR-specific step).
    row_counts = run_pre_MEDS(
        raw_input_dir,
        pre_MEDS_dir,
        fhir_version=cfg.fhir_version,
        subject_id_suffixes=tuple(cfg.subject_id_identifier_suffixes),
        keep_local_time=cfg.keep_local_time,
        max_resources_per_type=cfg.get("max_events"),
    )

    # Step 2: pre-MEDS tables -> MEDS with MEDS-Extract, driven by configs/messy.yaml.
    spec_fp = pre_MEDS_dir / "messy.yaml"
    etl_options = OmegaConf.to_container(cfg.etl_options, resolve=True) if cfg.get("etl_options") else None
    tables = write_effective_spec(
        pre_MEDS_dir, spec_fp, str(cfg.dataset_name), str(cfg.dataset_version), etl_options
    )
    logger.info(
        f"Extracting MEDS events from {len(tables)} tables: {tables} ({sum(row_counts.values())} rows)."
    )
    run_meds_extract(spec_fp, pre_MEDS_dir, MEDS_cohort_dir, __version__, cfg.get("stage_runner_fp"))

    # Step 3: vocabulary titles and parents for metadata/codes.parquet, in place (see sumeds_rules.yaml),
    # recorded in metadata/dataset.json.
    if cfg.do_enrich:
        enrich_main(enrich_argv)
    logger.info(f"Done. MEDS cohort written to {MEDS_cohort_dir}.")


if __name__ == "__main__":
    main()
