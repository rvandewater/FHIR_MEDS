"""
fhir_parser.py
--------------
Generalized FHIR resource loader for the fhir2meds pipeline.
Loads all FHIR resources by type from a directory, and provides utilities for filtering and sampling.
"""
import os
import json
from collections import defaultdict
from typing import List, Dict, Any

def load_fhir_resources_by_type(fhir_dir: str) -> Dict[str, List[dict]]:
    """
    Recursively load all FHIR resources by type from a directory.
    Scans all .json and .ndjson files, regardless of name.

    Args:
        fhir_dir (str): Path to the directory containing FHIR files.

    Returns:
        Dict[str, List[dict]]: Mapping from resourceType to list of resource dicts.
    """
    resources = defaultdict(list)
    for root, dirs, files in os.walk(fhir_dir):
        for fname in files:
            if fname.endswith('.ndjson') or fname.endswith('.json'):
                fpath = os.path.join(root, fname)
                with open(fpath) as f:
                    for line in f:
                        if not line.strip():
                            continue
                        try:
                            data = json.loads(line)
                        except Exception as e:
                            print(f"Failed to parse line in {fpath}: {e}")
                            continue
                        rtype = data.get("resourceType")
                        if rtype:
                            resources[rtype].append(data)
    return resources

def is_subject_associated(resource: dict) -> bool:
    """
    Returns True if the resource is associated with a subject (has a 'subject' field referencing a Patient, or is a Patient resource).

    Args:
        resource (dict): FHIR resource dict.

    Returns:
        bool: True if associated with a subject, False otherwise.
    """
    if resource.get("resourceType") == "Patient":
        return True
    # Many FHIR resources use 'subject' to reference a Patient
    if "subject" in resource and resource["subject"].get("reference", "").startswith("Patient/"):
        return True
    # Some resources may use 'patient' or other fields; extend as needed
    return False

def filter_subject_resources_by_type(resources_by_type: Dict[str, List[dict]]) -> Dict[str, List[dict]]:
    """
    Filter all loaded resources globally, keeping only those associated with a subject.
    Logs the number of skipped resources per type.

    Args:
        resources_by_type (Dict[str, List[dict]]): Mapping from resourceType to list of resource dicts.

    Returns:
        Dict[str, List[dict]]: Filtered mapping with only subject-associated resources.
    """
    filtered = {}
    for rtype, resources in resources_by_type.items():
        subject_resources = [res for res in resources if is_subject_associated(res)]
        skipped = len(resources) - len(subject_resources)
        if skipped > 0:
            print(f"Skipped {skipped} of {len(resources)} {rtype} resources (not associated with a subject)")
        filtered[rtype] = subject_resources
    return filtered

def get_sample_resources_by_type(fhir_dir: str, n: int = 3) -> Dict[str, List[dict]]:
    """
    Return up to n samples for each resource type found in the directory.

    Args:
        fhir_dir (str): Path to the directory containing FHIR files.
        n (int): Number of samples per resource type.

    Returns:
        Dict[str, List[dict]]: Mapping from resourceType to list of up to n resource dicts.
    """
    all_resources = load_fhir_resources_by_type(fhir_dir)
    return {rtype: resources[:n] for rtype, resources in all_resources.items()} 