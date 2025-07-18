import os
import json
from collections import defaultdict
from typing import List, Dict, Any
from fhir.resources.observation import Observation


def load_fhir_observations_from_bundle(bundle_path: str) -> List[Observation]:
    """
    Load all Observation resources from a FHIR Bundle JSON file.
    """
    with open(bundle_path) as f:
        data = json.load(f)
    bundle = Bundle.parse_obj(data)
    observations = []
    for entry in bundle.entry:
        if entry.resource.resource_type == "Observation":
            observations.append(Observation.parse_obj(entry.resource.dict()))
    return observations


def load_fhir_observations_from_dir(input_dir: str):
    observations = []
    for root, dirs, files in os.walk(input_dir):
        for fname in files:
            if fname.endswith(".json") or fname.endswith(".ndjson"):
                fpath = os.path.join(root, fname)
                with open(fpath) as f:
                    # If NDJSON, process line by line
                    if fname.endswith(".ndjson"):
                        for line in f:
                            if not line.strip():
                                continue
                            data = json.loads(line)
                            if data.get("resourceType") == "Observation":
                                try:
                                    observations.append(Observation.parse_obj(data))
                                except Exception as e:
                                    print(f"Failed to parse {fpath}: {e}")
                    else:
                        # Standard JSON file
                        data = json.load(f)
                        if data.get("resourceType") == "Observation":
                            try:
                                observations.append(Observation.parse_obj(data))
                            except Exception as e:
                                print(f"Failed to parse {fpath}: {e}")
                        elif data.get("resourceType") == "Bundle":
                            for entry in data.get("entry", []):
                                resource = entry.get("resource", {})
                                if resource.get("resourceType") == "Observation":
                                    try:
                                        observations.append(Observation.parse_obj(resource))
                                    except Exception as e:
                                        print(f"Failed to parse entry in {fpath}: {e}")
    return observations 


def load_fhir_resources_by_type(fhir_dir):
    """
    Recursively load all FHIR resources by type from a directory.
    Returns a dict: {resourceType: [resource_dict, ...]}
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

def get_sample_resources_by_type(fhir_dir, n=3):
    """
    Return up to n samples for each resource type found in the directory.
    """
    all_resources = load_fhir_resources_by_type(fhir_dir)
    return {rtype: resources[:n] for rtype, resources in all_resources.items()} 