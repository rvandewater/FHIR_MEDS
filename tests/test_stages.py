"""The registered `FHIR` spec, its `fhir_pre_meds` pre-stage, and the deprecated `fhir2meds` command."""

import doctest
import json
import os
import subprocess
from importlib.metadata import entry_points
from importlib.resources import files
from pathlib import Path

import pytest
import yaml
from MEDS_extract.config import MessyConfig
from omegaconf import OmegaConf

import fhir2meds.__main__ as deprecated
import fhir2meds.pre_MEDS
from fhir2meds import MESSY_CFG
from fhir2meds.stages import PRE_MEDS_DEFAULTS, PRE_MEDS_STAGE, _pre_meds
from tests.test_e2e import SCRIPTS_DIR, assert_valid_meds_cohort

SPEC_TABLES = sorted(MessyConfig.load("FHIR").needed_source_columns())


def _stage_names(stages: list) -> list[str]:
    return [s if isinstance(s, str) else next(iter(s)) for s in stages]


def test_spec_is_registered_as_fhir():
    (ep,) = (ep for ep in entry_points(group="MEDS_extract.pipelines") if ep.name == "FHIR")
    messy = MessyConfig.load("FHIR")

    assert ep.value == "fhir2meds.configs:messy.yaml"
    assert Path(messy.source_fp) == Path(str(MESSY_CFG))
    assert messy.dataset_name == "FHIR"
    messy.etl.check_extension_stages()


def test_pipeline_is_the_canonical_one_between_the_extension_stages():
    canonical = yaml.safe_load(files("MEDS_extract.configs").joinpath("_extract.yaml").read_text())["stages"]
    cfg = MessyConfig.load("FHIR").pipeline_config(input_dir=Path("/raw"), output_dir=Path("/out"))

    assert _stage_names(cfg["stages"]) == [PRE_MEDS_STAGE, *_stage_names(canonical), "sumeds_enrich"]
    (enrich,) = (s for s in cfg["stages"] if isinstance(s, dict) and "sumeds_enrich" in s)
    assert enrich["sumeds_enrich"]["rules"] == "FHIR"


def test_downloads_and_version_stamps():
    messy = MessyConfig.load("FHIR")
    version = {key: messy.dataset_version_for(key) for key in ("dataset", "demo", "mimic")}

    assert version["dataset"].startswith("unknown:")
    assert version["demo"].startswith("2.1.0:")
    assert version["mimic"].startswith("2.1:")
    assert [type(s).__name__ for s in messy.selected_sources("demo")] == ["PhysioNetSource"]


def _stage_cfg(tmp_path: Path, worker: int = 0, **options):
    return OmegaConf.create(
        {
            "worker": worker,
            "input_dir": str(tmp_path / "raw"),
            "output_dir": str(tmp_path / "out"),
            "MESSY_config_fp": "pkg://fhir2meds.configs.messy.yaml",
            "stage_cfg": {
                **PRE_MEDS_DEFAULTS,
                **options,
                "output_dir": str(tmp_path / "out" / PRE_MEDS_STAGE),
            },
        }
    )


def test_pre_stage_passes_its_options_and_the_spec_tables(tmp_path: Path, monkeypatch):
    seen = []
    monkeypatch.setattr(fhir2meds.pre_MEDS, "run_pre_MEDS", lambda *a, **kw: seen.append((a, kw)))

    _pre_meds(_stage_cfg(tmp_path, fhir_version="R5", keep_local_time=True, max_events=7))

    ((args, kwargs),) = seen
    assert args == (tmp_path / "raw", tmp_path / "out" / PRE_MEDS_STAGE)
    assert kwargs["fhir_version"] == "R5"
    assert kwargs["keep_local_time"] is True
    assert kwargs["max_resources_per_type"] == 7
    assert kwargs["subject_id_suffixes"] == ("/identifier/patient",)
    assert kwargs["required_tables"] == SPEC_TABLES


def test_pre_stage_works_only_in_worker_0(tmp_path: Path, monkeypatch):
    seen = []
    monkeypatch.setattr(fhir2meds.pre_MEDS, "run_pre_MEDS", lambda *a, **kw: seen.append(a))

    _pre_meds(_stage_cfg(tmp_path, worker=1))

    assert seen == []


def test_deprecated_command_translation():
    """The examples of `fhir2meds.__main__.translate` (pytest collects no doctests from `__main__.py`)."""
    result = doctest.testmod(deprecated, optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
    assert result.attempted >= 3
    assert result.failed == 0


def test_deprecated_command_runs_the_spec(mixed_raw_dir: Path, tmp_path: Path):
    root = tmp_path / "root"
    command = [
        str(SCRIPTS_DIR / "fhir2meds"),
        f"raw_input_dir={mixed_raw_dir}",
        f"root_output_dir={root}",
        "dataset_name=OLD_NAME",
        "dataset_version=0.9",
        "do_enrich=false",
    ]
    env = {**os.environ, "PATH": os.pathsep.join([str(SCRIPTS_DIR), os.environ.get("PATH", "")])}
    result = subprocess.run(command, capture_output=True, text=True, env=env)

    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-3000:]
    assert "fhir2meds is deprecated" in result.stderr
    cohort = root / "MEDS_cohort"
    assert_valid_meds_cohort(cohort)
    dataset = json.loads((cohort / "metadata" / "dataset.json").read_text())
    assert (dataset["dataset_name"], dataset["dataset_version"]) == ("OLD_NAME", "0.9")
    assert "sumeds_enrichment" not in dataset
    assert (cohort / PRE_MEDS_STAGE / "Patient.parquet").exists()


@pytest.mark.parametrize("arg", ["enrich_args=[--strict]", "etl_options={n_subjects_per_shard:10}"])
def test_deprecated_command_rejects_removed_options(arg: str, tmp_path: Path):
    result = subprocess.run(
        [str(SCRIPTS_DIR / "fhir2meds"), f"root_output_dir={tmp_path}", arg], capture_output=True, text=True
    )

    assert result.returncode != 0
    assert f"no longer takes {arg.split('=')[0]}=" in result.stderr
    assert not (tmp_path / "MEDS_cohort").exists()
