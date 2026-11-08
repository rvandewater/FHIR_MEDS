"""FHIR parsing as a MEDS-Extract pre-stage, so that the whole ETL is one registered spec.

The spec registered as `FHIR` (`configs/messy.yaml`) runs with one command,
`meds-extract-run spec=FHIR output_dir=OUT do_download=false input_dir=FHIR_DIR`:

1. `fhir_pre_meds` (this module's pre-stage) streams the FHIR resources in `input_dir` (NDJSON or Bundles,
   optionally gzipped) and writes one flat parquet table per resource type into `OUT/fhir_pre_meds`, where
   MEDS-Extract reads them;
2. MEDS-Extract's canonical stages turn them into MEDS events, as `configs/messy.yaml` says;
3. `sumeds_enrich` (registered by suMEDS) adds vocabulary titles and parents to `metadata/codes.parquet` with
   the rules registered as `FHIR`, and records that in `metadata/dataset.json`.

Every stage process of a pipeline imports this module, so its top-level imports stay light: the FHIR parsing
code is imported when the stage runs.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from MEDS_transforms.stages import Stage
from omegaconf import DictConfig

PRE_MEDS_STAGE = "fhir_pre_meds"

#: Options of `fhir_pre_meds`. Set them in the spec (`- fhir_pre_meds: {fhir_version: R5}`) or for one run
#: with `"overrides=['++stage_cfg.fhir_version=R5']"`.
PRE_MEDS_DEFAULTS: dict[str, Any] = {
    # R4 or R5: selects the section of configs/flatten.yaml.
    "fhir_version": "R4",
    # Patient.identifier systems (suffix match) whose integer value is used as subject_id (MIMIC-IV:
    # /identifier/patient). Patients without one get a stable SHA-256-derived id.
    "subject_id_identifier_suffixes": ["/identifier/patient"],
    # By default timestamps with a UTC offset are converted to UTC; true keeps local wall-clock time instead.
    "keep_local_time": False,
    # Read at most this many resources per type (for debugging); null reads all.
    "max_events": None,
}


def _pre_meds(cfg: DictConfig) -> None:
    """Flatten the FHIR resources in `input_dir` into one parquet table per resource type.

    Every table the spec reads is written, without rows if the input has no resources of that type, so that
    the one registered spec fits every FHIR export. See `PRE_MEDS_DEFAULTS` for the options and
    `fhir2meds.pre_MEDS.run_pre_MEDS` for the flattening.
    """
    if cfg.worker != 0:
        return
    from MEDS_extract.config import MessyConfig

    from .pre_MEDS import run_pre_MEDS

    stage_cfg = cfg.stage_cfg
    max_events = stage_cfg.get("max_events")
    run_pre_MEDS(
        Path(cfg.input_dir),
        Path(stage_cfg.output_dir),
        fhir_version=str(stage_cfg.get("fhir_version", "R4")),
        subject_id_suffixes=tuple(stage_cfg.get("subject_id_identifier_suffixes") or ()),
        keep_local_time=bool(stage_cfg.get("keep_local_time", False)),
        max_resources_per_type=int(max_events) if max_events is not None else None,
        required_tables=sorted(MessyConfig.load(cfg.MESSY_config_fp).needed_source_columns()),
    )


pre_meds_stage = Stage.register(
    main_fn=_pre_meds,
    stage_name=PRE_MEDS_STAGE,
    is_metadata=False,
    default_config=PRE_MEDS_DEFAULTS,
)
