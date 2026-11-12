import json
import os
from pathlib import Path

import pytest

from tests.test_e2e import assert_valid_meds_cohort, run_fhir


@pytest.mark.skipif(
    os.environ.get("FHIR_MEDS_RUN_DOWNLOAD_TESTS") != "1",
    reason="Downloads the MIMIC-IV FHIR demo from PhysioNet; set FHIR_MEDS_RUN_DOWNLOAD_TESTS=1 to run.",
)
def test_e2e(tmp_path: Path, monkeypatch):
    """`meds-extract-run spec=FHIR dataset_key=demo` downloads the MIMIC-IV FHIR demo and converts it."""
    # The published packs, not the test fixture's (conftest.py).
    monkeypatch.delenv("SUMEDS_ONTOLOGY_SOURCE", raising=False)
    out = tmp_path / "MEDS_cohort"
    result = run_fhir(None, out, "dataset_key=demo")
    assert result.returncode == 0, f"stdout:\n{result.stdout[-5000:]}\nstderr:\n{result.stderr[-5000:]}"

    data = assert_valid_meds_cohort(out)
    assert data["subject_id"].n_unique() == 100
    dataset = json.loads((out / "metadata" / "dataset.json").read_text())
    assert dataset["dataset_version"].startswith("2.1.0:")
    # Enriched, or soft-failed (e.g. GitHub unreachable): either way the run succeeds and says so.
    assert dataset["sumeds_enrichment"]["rules"] == "FHIR"
