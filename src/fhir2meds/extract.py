"""
extract.py
----------
Thin wrappers around the MEDS-Extract command line tools (``meds-extract-download`` and ``meds-extract-run``).
"""

import logging
import os
import subprocess
import sys
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

CONFIGS_DIR = Path(__file__).parent / "configs"
MESSY_CFG = CONFIGS_DIR / "messy.yaml"
SOURCES_CFG = CONFIGS_DIR / "sources.yaml"
RESERVED_KEYS = ("etl", "sources", "_defaults")


def write_effective_spec(
    pre_MEDS_dir: Path,
    out_fp: Path,
    dataset_name: str,
    dataset_version: str,
    etl_options: dict | None = None,
    messy_fp: Path = MESSY_CFG,
) -> list[str]:
    """Write the MESSY spec actually used for this run and return the tables it extracts.

    Tables without a matching ``<pre_MEDS_dir>/<table>.parquet`` are dropped (MEDS-Extract requires every
    listed table
    to exist), and the ``etl:`` block is filled in. YAML anchors are expanded so the saved file is self-
    contained.

    Examples:
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     d = Path(d)
        ...     for t in ("Patient", "Observation"):
        ...         (d / f"{t}.parquet").touch()
        ...     opts = {"n_subjects_per_shard": 10}
        ...     tables = write_effective_spec(d, d / "messy.yaml", "TEST", "1.0", opts)
        ...     spec = yaml.safe_load((d / "messy.yaml").read_text())
        >>> tables
        ['Patient', 'Observation']
        >>> spec["etl"]
        {'dataset_name': 'TEST', 'raw_dataset_version': '1.0', 'n_subjects_per_shard': 10}
        >>> spec["Observation"]["observation"]["time"]
        '$start'
    """
    with open(messy_fp) as f:
        spec = yaml.safe_load(f)
    tables = [k for k in spec if k not in RESERVED_KEYS and (pre_MEDS_dir / f"{k}.parquet").exists()]
    effective = {
        "etl": {
            **(spec.get("etl") or {}),
            "dataset_name": dataset_name,
            "raw_dataset_version": dataset_version,
            **(etl_options or {}),
        },
        "_defaults": spec["_defaults"],
        **{t: spec[t] for t in tables},
    }
    out_fp.parent.mkdir(parents=True, exist_ok=True)

    class _NoAliasDumper(yaml.SafeDumper):
        def ignore_aliases(self, data):
            return True

    with open(out_fp, "w") as f:
        yaml.dump(effective, f, Dumper=_NoAliasDumper, sort_keys=False)
    return tables


def _cli(name: str) -> str:
    """Path of a console script installed next to the running interpreter."""
    return str(Path(sys.executable).parent / name)


def _run(cmd: list[str]) -> None:
    # MEDS-Transforms launches each stage as a `MEDS_transform-stage` subprocess resolved from PATH, so make
    # sure the
    # current environment's scripts directory comes first even when it is not activated.
    env = {
        **os.environ,
        "PATH": os.pathsep.join([str(Path(sys.executable).parent), os.environ.get("PATH", "")]),
    }
    logger.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, env=env)
    if result.returncode != 0:
        raise RuntimeError(f"{Path(cmd[0]).name} failed with exit code {result.returncode}")


def download_raw_data(
    output_dir: Path, do_demo: bool, sources_fp: Path = SOURCES_CFG
) -> None:  # pragma: no cover
    _run(
        [
            _cli("meds-extract-download"),
            f"spec={sources_fp}",
            f"output_dir={output_dir}",
            f"key={'demo' if do_demo else 'dataset'}",
        ]
    )


def run_meds_extract(
    spec_fp: Path,
    pre_MEDS_dir: Path,
    MEDS_cohort_dir: Path,
    package_version: str,
    stage_runner_fp: str | None = None,
) -> None:
    """Run the canonical MEDS-Extract pipeline over the pre-MEDS tables."""
    overrides = [
        "etl_metadata.package_name=fhir2meds",
        f"etl_metadata.package_version={package_version}",
    ]
    cmd = [
        _cli("meds-extract-run"),
        f"spec={spec_fp}",
        f"output_dir={MEDS_cohort_dir}",
        "do_download=false",
        f"input_dir={pre_MEDS_dir}",
        "overrides=[" + ",".join(f"'{o}'" for o in overrides) + "]",
    ]
    if stage_runner_fp:
        cmd.append(f"stage_runner_fp={stage_runner_fp}")
    _run(cmd)
