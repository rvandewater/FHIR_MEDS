"""Build the vendored Synthea test fixtures in tests/fixtures/synthea from the SMART sample bulk dataset.

Source: https://github.com/smart-on-fhir/sample-bulk-fhir-datasets (Synthea-generated, data licensed CC0),
pinned to the commit below. The script selects a few patients, strips base64 clinical notes to keep the
fixture small, and writes the same resources twice:

- ``bulk/<Type>.000.ndjson.gz``: FHIR bulk-data export layout (``Patient/<id>`` references).
- ``bundles/<patient_id>.json.gz``: one Synthea-style transaction Bundle per patient (``urn:uuid:<id>``
  references).

Usage (downloads ~0.8 MB):

    python scripts/make_synthea_fixtures.py tests/fixtures/synthea
"""

import argparse
import gzip
import io
import json
import re
import urllib.request
import zipfile
from collections import defaultdict
from pathlib import Path

SOURCE_REPO = "smart-on-fhir/sample-bulk-fhir-datasets"
SOURCE_COMMIT = "54fa822575407ffc3255bc1d510749a28c29eb38"  # branch 10-patients, 2026-03-30
SOURCE_URL = f"https://github.com/{SOURCE_REPO}/archive/{SOURCE_COMMIT}.zip"

N_ALIVE_PATIENTS = 4  # plus every deceased patient, so MEDS_DEATH is exercised
N_ORPHANS_PER_TYPE = (
    2  # resources whose patient is not in the export (a quirk of this dataset) are kept to test drops
)
SKIP_TYPES = {"Location", "Organization", "Practitioner", "PractitionerRole"}
REFERENCE_RE = re.compile(r"^([A-Z][A-Za-z]+)/([^/]+)$")


def download_resources() -> dict[str, list[dict]]:
    with urllib.request.urlopen(SOURCE_URL) as response:
        archive = zipfile.ZipFile(io.BytesIO(response.read()))
    by_type = defaultdict(list)
    for name in sorted(archive.namelist()):
        if not name.endswith(".ndjson") or name.endswith("log.ndjson"):
            continue
        for line in archive.read(name).decode().splitlines():
            if line.strip():
                resource = json.loads(line)
                by_type[resource["resourceType"]].append(resource)
    return by_type


def patient_of(resource: dict) -> str | None:
    for field in ("subject", "patient"):
        ref = (resource.get(field) or {}).get("reference", "")
        if ref.startswith("Patient/"):
            return ref.split("/", 1)[1]
    return None


def strip_notes(resource: dict) -> dict:
    """Drop base64 attachments (Synthea clinical notes); they are large and not used by the ETL."""
    resource = json.loads(json.dumps(resource))
    resource.pop("presentedForm", None)
    for content in resource.get("content") or []:
        (content.get("attachment") or {}).pop("data", None)
    return resource


def to_bundle_references(obj):
    """Rewrite ``Type/id`` references to Bundle-internal ``urn:uuid:id`` ones, as in Synthea's Bundles."""
    if isinstance(obj, dict):
        out = {}
        for key, value in obj.items():
            if key == "reference" and isinstance(value, str) and REFERENCE_RE.match(value):
                out[key] = f"urn:uuid:{REFERENCE_RE.match(value).group(2)}"
            else:
                out[key] = to_bundle_references(value)
        return out
    if isinstance(obj, list):
        return [to_bundle_references(v) for v in obj]
    return obj


def main(out_dir: Path) -> None:
    by_type = download_resources()
    patients = sorted(by_type["Patient"], key=lambda p: p["id"])
    deceased = [p["id"] for p in patients if p.get("deceasedDateTime")]
    alive = [p["id"] for p in patients if not p.get("deceasedDateTime")][:N_ALIVE_PATIENTS]
    keep = set(deceased + alive)
    known = {p["id"] for p in patients}

    selected: dict[str, list[dict]] = {}
    for rtype, resources in sorted(by_type.items()):
        if rtype in SKIP_TYPES:
            continue
        rows, orphans = [], 0
        for r in resources:
            pid = r["id"] if rtype == "Patient" else patient_of(r)
            if pid in keep:
                rows.append(strip_notes(r))
            elif pid not in known and orphans < N_ORPHANS_PER_TYPE:
                rows.append(strip_notes(r))
                orphans += 1
        if rows:
            selected[rtype] = rows

    bulk_dir, bundle_dir = out_dir / "bulk", out_dir / "bundles"
    for d in (bulk_dir, bundle_dir):
        d.mkdir(parents=True, exist_ok=True)
        for old in d.iterdir():
            old.unlink()
    for rtype, rows in selected.items():
        with gzip.GzipFile(bulk_dir / f"{rtype}.000.ndjson.gz", "wb", mtime=0) as f:
            f.write("".join(json.dumps(r, sort_keys=True) + "\n" for r in rows).encode())

    entries_by_patient = defaultdict(list)
    for rtype, rows in selected.items():
        for r in rows:
            pid = r["id"] if rtype == "Patient" else patient_of(r)
            if pid in keep:
                entries_by_patient[pid].append(
                    {
                        "fullUrl": f"urn:uuid:{r['id']}",
                        "resource": to_bundle_references(r),
                        "request": {"method": "POST", "url": rtype},
                    }
                )
    for pid, entries in sorted(entries_by_patient.items()):
        # Synthea puts the Patient first in each Bundle.
        entries.sort(key=lambda e: e["resource"]["resourceType"] != "Patient")
        bundle = {"resourceType": "Bundle", "type": "transaction", "entry": entries}
        with gzip.GzipFile(bundle_dir / f"{pid}.json.gz", "wb", mtime=0) as f:
            f.write(json.dumps(bundle, indent=1, sort_keys=True).encode())

    summary = {rtype: len(rows) for rtype, rows in selected.items()}
    print(f"Patients: {len(keep)} ({len(deceased)} deceased). Resources per type: {summary}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("out_dir", type=Path)
    main(parser.parse_args().out_dir)
