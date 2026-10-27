"""
fhir_parser.py
--------------
Generalized FHIR resource loader for the fhir2meds pipeline.
Loads all FHIR resources by type from a directory, and provides utilities for filtering and sampling.

Supported inputs (searched recursively, in sorted order so runs are reproducible):

- ``*.ndjson`` / ``*.ndjson.gz``: FHIR bulk-data export files, one resource per line.
- ``*.json`` / ``*.json.gz``: a single resource or a ``Bundle`` (e.g. Synthea's one-Bundle-per-patient output).

Bundles are unpacked recursively, so the resources inside them are treated exactly like bulk-export resources.
"""
import gzip
import json
import logging
import os
from collections import Counter, defaultdict
from collections.abc import Container, Iterator
from importlib import import_module
from pathlib import Path
from typing import Any

from omegaconf import OmegaConf

logger = logging.getLogger(__name__)

# Hydra config loading will be handled by the main entrypoint, but allow direct config loading for testing
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "configs", "event_configs.yaml")

# fhir.resources >= 7 ships R5 as the default namespace and R4B as the closest available R4 model.
FHIR_VERSION_MODULES = {
    "R4": "fhir.resources.R4B",
    "R5": "fhir.resources",
}

FHIR_SUFFIXES = (".ndjson", ".ndjson.gz", ".json", ".json.gz")


def load_event_config(config_path: str = CONFIG_PATH, fhir_version: str = "R4") -> dict[str, Any]:
    cfg = OmegaConf.load(config_path)
    if fhir_version not in cfg:
        raise ValueError(f"FHIR version {fhir_version} not found in config")
    section = OmegaConf.to_container(cfg[fhir_version], resolve=True)
    if not isinstance(section, dict):
        raise TypeError(f"Config section for {fhir_version} must be a dict, got {type(section)}")
    # Ensure all keys are strings
    if not all(isinstance(k, str) for k in section.keys()):
        raise TypeError(f"All config keys for {fhir_version} must be strings")
    return dict(section)  # type: ignore


def get_fhir_resource_class(resource_type: str, fhir_version: str = "R4"):
    """Dynamically import the correct FHIR resource class for the given type and version.

    Examples:
        >>> get_fhir_resource_class("Observation", "R4").__module__
        'fhir.resources.R4B.observation'
        >>> get_fhir_resource_class("Observation", "R5").__module__
        'fhir.resources.observation'
        >>> get_fhir_resource_class("Observation", "STU3")
        Traceback (most recent call last):
            ...
        ValueError: Unsupported FHIR version: STU3
    """
    if fhir_version not in FHIR_VERSION_MODULES:
        raise ValueError(f"Unsupported FHIR version: {fhir_version}")
    module = import_module(f"{FHIR_VERSION_MODULES[fhir_version]}.{resource_type.lower()}")
    return getattr(module, resource_type)


def iter_fhir_files(fhir_dir: str | Path) -> list[Path]:
    """Return all FHIR input files below ``fhir_dir`` in a deterministic (sorted) order."""
    return sorted(p for p in Path(fhir_dir).rglob("*") if p.is_file() and p.name.endswith(FHIR_SUFFIXES))


def _open_text(fpath: Path):
    if fpath.name.endswith(".gz"):
        return gzip.open(fpath, "rt", encoding="utf-8")
    return open(fpath, encoding="utf-8")


def unpack_bundles(resource: Any) -> Iterator[dict]:
    """Yield ``resource``, or every resource nested inside it if it is a ``Bundle``.

    Examples:
        >>> bundle = {"resourceType": "Bundle", "entry": [
        ...     {"resource": {"resourceType": "Patient", "id": "p1"}},
        ...     {"resource": {"resourceType": "Bundle", "entry": [{"resource": {"resourceType": "Condition"}}]}},
        ...     {"request": {"method": "GET"}},
        ... ]}
        >>> [r["resourceType"] for r in unpack_bundles(bundle)]
        ['Patient', 'Condition']
        >>> list(unpack_bundles({"resourceType": "Patient", "id": "p1"}))
        [{'resourceType': 'Patient', 'id': 'p1'}]
        >>> list(unpack_bundles([1, 2]))
        []
    """
    if not isinstance(resource, dict):
        return
    if resource.get("resourceType") == "Bundle":
        for entry in resource.get("entry") or []:
            yield from unpack_bundles(entry.get("resource"))
    elif "resourceType" in resource:
        yield resource


def iter_resources(fpath: str | Path, stats: Counter | None = None) -> Iterator[dict]:
    """Yield every FHIR resource (as a dict) stored in ``fpath``.

    Malformed lines are skipped and counted in ``stats["unparseable_lines"]``; their content is never logged, as it
    may contain protected health information.
    """
    fpath = Path(fpath)
    stats = stats if stats is not None else Counter()
    with _open_text(fpath) as f:
        if fpath.name.endswith((".json", ".json.gz")):
            text = f.read()
            try:
                yield from unpack_bundles(json.loads(text))
                return
            except json.JSONDecodeError:
                # Some exporters write NDJSON with a .json suffix; fall back to line-by-line parsing.
                lines = text.splitlines()
        else:
            lines = f
        for line in lines:
            if not line.strip():
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                stats["unparseable_lines"] += 1
                continue
            yield from unpack_bundles(data)


def load_fhir_resources_by_type(
    fhir_dir: str,
    event_config: dict[str, Any],
    fhir_version: str = "R4",
    validate_with_fhir_resources: bool = False,
) -> dict[str, list[Any]]:
    """
    Load and parse FHIR resources by type using fhir.resources and config.
    Only loads resource types specified in the config.
    If validate_with_fhir_resources is False, loads raw dicts instead of validated objects.

    Resources are de-duplicated on ``(resourceType, id)``: some exports (e.g. the MIMIC-IV FHIR demo's
    ``Specimen.ndjson`` and ``SpecimenLab.ndjson``) ship the same resources in more than one file.
    """
    resources = defaultdict(list)
    resource_types = set(event_config["resources"])
    seen: set[tuple[str, str]] = set()
    stats: Counter = Counter()
    for fpath in iter_fhir_files(fhir_dir):
        logger.info(f"Parsing file {fpath}")
        for data in iter_resources(fpath, stats):
            rtype = data.get("resourceType")
            if rtype not in resource_types:
                stats["skipped_unconfigured_type"] += 1
                continue
            rid = data.get("id")
            if rid is not None:
                if (rtype, rid) in seen:
                    stats["duplicates"] += 1
                    continue
                seen.add((rtype, rid))
            if validate_with_fhir_resources:
                try:
                    resource_class = get_fhir_resource_class(rtype, fhir_version)
                    resources[rtype].append(resource_class.model_validate(data))
                    continue
                except Exception as e:
                    logger.warning(f"Failed to validate a {rtype} resource with fhir.resources: {type(e).__name__}")
            resources[rtype].append(data)
    if stats:
        logger.info(f"Resource loading stats: {dict(stats)}")
    return resources


def parse_patient_reference(ref: Any, known_patient_ids: Container[str] | None = None) -> str | None:
    """Return the Patient logical id a FHIR reference string points to, or None.

    Handles relative (``Patient/123``), absolute (``https://x/fhir/Patient/123``), versioned
    (``Patient/123/_history/2``) and Bundle-internal (``urn:uuid:123``) references. Because a ``urn:uuid:`` reference
    does not name the resource type, it is only accepted if ``known_patient_ids`` is not given or contains the id.

    Examples:
        >>> parse_patient_reference("Patient/123")
        '123'
        >>> parse_patient_reference("http://example.org/fhir/Patient/123/_history/2")
        '123'
        >>> parse_patient_reference("urn:uuid:abc")
        'abc'
        >>> parse_patient_reference("urn:uuid:abc", known_patient_ids={"def"}) is None
        True
        >>> parse_patient_reference("Group/1") is None
        True
        >>> parse_patient_reference(None) is None
        True
    """
    if not isinstance(ref, str) or not ref:
        return None
    if ref.startswith("urn:uuid:"):
        pid = ref[len("urn:uuid:") :]
        if known_patient_ids is not None and pid not in known_patient_ids:
            return None
        return pid
    parts = ref.split("/")
    if "_history" in parts:
        parts = parts[: parts.index("_history")]
    if len(parts) >= 2 and parts[-2] == "Patient" and parts[-1]:
        return parts[-1]
    return None


def _get(resource: Any, field: str) -> Any:
    if isinstance(resource, dict):
        return resource.get(field)
    return getattr(resource, field, None)


def get_patient_id(resource: Any, known_patient_ids: Container[str] | None = None) -> str | None:
    """Return the logical id of the Patient a resource belongs to (the Patient itself for Patient resources).

    Examples:
        >>> get_patient_id({"resourceType": "Patient", "id": "p1"})
        'p1'
        >>> get_patient_id({"resourceType": "Observation", "subject": {"reference": "Patient/p1"}})
        'p1'
        >>> get_patient_id({"resourceType": "AllergyIntolerance", "patient": {"reference": "urn:uuid:p1"}})
        'p1'
        >>> get_patient_id({"resourceType": "Observation", "subject": {"reference": "Group/g"}}) is None
        True
    """
    rtype = _get(resource, "resourceType") or _get(resource, "resource_type")
    if rtype == "Patient":
        return _get(resource, "id")
    for field in ("subject", "patient"):
        ref_obj = _get(resource, field)
        pid = parse_patient_reference(_get(ref_obj, "reference") if ref_obj else None, known_patient_ids)
        if pid is not None:
            return pid
    return None


def is_subject_associated(resource: Any) -> bool:
    return get_patient_id(resource) is not None


def filter_subject_resources_by_type(resources_by_type: dict[str, list[Any]]) -> dict[str, list[Any]]:
    """
    Filter all loaded resources globally, keeping only those associated with a subject.
    Logs the number of skipped resources per type.
    """
    filtered = {}
    for rtype, resources in resources_by_type.items():
        subject_resources = [res for res in resources if is_subject_associated(res)]
        skipped = len(resources) - len(subject_resources)
        if skipped > 0:
            logger.info(f"Skipped {skipped} of {len(resources)} {rtype} resources (not associated with a subject)")
        filtered[rtype] = subject_resources
    logger.info(f"Subject-associated types: {[(k, len(v)) for k, v in filtered.items()]}")
    return filtered


def get_sample_resources_by_type(
    fhir_dir: str, event_config: dict, fhir_version: str = "R4", n: int = 3
) -> dict[str, list[Any]]:
    """
    Return up to n samples for each resource type found in the directory.
    """
    all_resources = load_fhir_resources_by_type(fhir_dir, event_config, fhir_version)
    return {rtype: resources[:n] for rtype, resources in all_resources.items()}
