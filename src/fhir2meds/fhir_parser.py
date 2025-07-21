"""
fhir_parser.py
--------------
Generalized FHIR resource loader for the fhir2meds pipeline.
Loads all FHIR resources by type from a directory, and provides utilities for filtering and sampling.
"""
import os
import logging
from collections import defaultdict
from typing import List, Dict, Any, cast
from omegaconf import OmegaConf
from importlib import import_module

# Hydra config loading will be handled by the main entrypoint, but allow direct config loading for testing
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'configs', 'event_configs.yaml')

FHIR_VERSION_MODULES = {
    'R4': 'fhir.resources',
    'R5': 'fhir.resources.R5',
}

def load_event_config(config_path: str = CONFIG_PATH, fhir_version: str = 'R4') -> Dict[str, Any]:
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

def get_fhir_resource_class(resource_type: str, fhir_version: str = 'R4'):
    """
    Dynamically import the correct FHIR resource class for the given type and version.
    """
    if fhir_version == 'R4':
        module = import_module(f"fhir.resources.{resource_type.lower()}")
    elif fhir_version == 'R5':
        module = import_module(f"fhir.resources.R5.{resource_type.lower()}")
    else:
        raise ValueError(f"Unsupported FHIR version: {fhir_version}")
    return getattr(module, resource_type)

def load_fhir_resources_by_type(fhir_dir: str, event_config: Dict[str, Any], fhir_version: str = 'R4', validate_with_fhir_resources: bool = False) -> Dict[str, List[Any]]:
    """
    Load and parse FHIR resources by type using fhir.resources and config.
    Only loads resource types specified in the config.
    If validate_with_fhir_resources is False, loads raw dicts instead of validated objects.
    """
    event_config = cast(Dict[str, Any], event_config)
    resources = defaultdict(list)
    resource_types = event_config['resources']  # type: ignore
    for root, dirs, files in os.walk(fhir_dir):
        for fname in files:
            if fname.endswith('.ndjson') or fname.endswith('.json'):
                fpath = os.path.join(root, fname)
                logging.info(f"Parsing file {fpath}")
                with open(fpath) as f:
                    for line in f:
                        if not line.strip():
                            continue
                        try:
                            import json
                            data = json.loads(line)
                        except Exception as e:
                            print(f"Failed to parse line in {fpath}: {e}")
                            continue
                        rtype = data.get("resourceType")
                        if rtype in resource_types:
                            if validate_with_fhir_resources:
                                try:
                                    resource_class = get_fhir_resource_class(rtype, fhir_version)
                                    resource_obj = resource_class.parse_obj(data)
                                    resources[rtype].append(resource_obj)
                                    print(f"Loaded {rtype}: {resource_obj}")  # DEBUG
                                except Exception as e:
                                    logging.warning(f"Failed to parse {rtype} with fhir.resources: {e}")
                                    resources[rtype].append(data)
                                    print(f"Loaded {rtype} as dict: {data}")  # DEBUG
                            else:
                                resources[rtype].append(data)
                                # print(f"Loaded {rtype} as dict: {data}")  # DEBUG

    # print(f"Resource types loaded: {list(resources.keys())}")  # DEBUG
    # for k, v in resources.items():
    #     print(f"Loaded {len(v)} resources of type {k}")  # DEBUG
    return resources

def is_subject_associated(resource: Any) -> bool:
    # Handle dicts
    if isinstance(resource, dict):
        if resource.get("resourceType") == "Patient":
            return True
        # Check 'subject' reference
        subject = resource.get("subject", {})
        ref = subject.get("reference", "") if isinstance(subject, dict) else ""
        if ref.startswith("Patient/"):
            return True
        # Check 'patient' reference
        patient = resource.get("patient", {})
        ref = patient.get("reference", "") if isinstance(patient, dict) else ""
        if ref.startswith("Patient/"):
            return True
        return False
    # Handle objects
    rtype = getattr(resource, 'resource_type', None) or getattr(resource, 'resourceType', None)
    if rtype == "Patient":
        return True
    # Check 'subject' reference
    subject = getattr(resource, 'subject', None)
    ref = getattr(subject, 'reference', None) if subject else None
    if ref and ref.startswith("Patient/"):
        return True
    # Check 'patient' reference
    patient = getattr(resource, 'patient', None)
    ref = getattr(patient, 'reference', None) if patient else None
    if ref and ref.startswith("Patient/"):
        return True
    return False

def filter_subject_resources_by_type(resources_by_type: Dict[str, List[Any]]) -> Dict[str, List[Any]]:
    """
    Filter all loaded resources globally, keeping only those associated with a subject.
    Logs the number of skipped resources per type.
    """
    filtered = {}
    for rtype, resources in resources_by_type.items():
        print(f"Filtering {len(resources)} resources of type {rtype}")  # DEBUG
        subject_resources = [res for res in resources if is_subject_associated(res)]
        print(f"Kept {len(subject_resources)} subject-associated resources of type {rtype}")  # DEBUG
        skipped = len(resources) - len(subject_resources)
        if skipped > 0:
            logging.info(f"Skipped {skipped} of {len(resources)} {rtype} resources (not associated with a subject)")
        filtered[rtype] = subject_resources
    print(f"Subject-associated types: {[(k, len(v)) for k, v in filtered.items()]}")  # DEBUG
    return filtered

def get_sample_resources_by_type(fhir_dir: str, event_config: dict, fhir_version: str = 'R4', n: int = 3) -> Dict[str, List[Any]]:
    """
    Return up to n samples for each resource type found in the directory.
    """
    all_resources = load_fhir_resources_by_type(fhir_dir, event_config, fhir_version)
    return {rtype: resources[:n] for rtype, resources in all_resources.items()} 