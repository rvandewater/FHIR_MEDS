import os
import json
from fhir.resources.observation import Observation
from fhir.resources.bundle import Bundle
from typing import List, Dict, Any


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