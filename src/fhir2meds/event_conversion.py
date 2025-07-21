import json
import re


def build_patient_id_map(patient_ndjson_path):
    uuid_to_int = {}
    with open(patient_ndjson_path) as f:
        for line in f:
            if not line.strip():
                continue
            data = json.loads(line)
            if data.get("resourceType") == "Patient":
                uuid = data["id"]
                for ident in data.get("identifier", []):
                    if ident.get("system", "").endswith("/identifier/patient"):
                        try:
                            uuid_to_int[uuid] = int(ident["value"])
                        except Exception:
                            pass
    return uuid_to_int


def safe_str(val):
    if val is None:
        return None
    if isinstance(val, str):
        return val
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return str(val)


def extract_path(resource, path, column_name=None):
    """
    Resolve a dotted path like 'code.coding[0].code' on a FHIR resource object or dict.
    """
    parts = re.split(r'\.|\[|\]', path)
    obj = resource
    for part in parts:
        if not part:
            continue
        # Check if part is a numeric index for list access
        if part.isdigit():
            try:
                obj = obj[int(part)]
            except (IndexError, TypeError, KeyError):
                return None
        elif isinstance(obj, dict):
            obj = obj.get(part)
        else:
            obj = getattr(obj, part, None)
        if obj is None:
            if column_name == "code":
                print(f"Warning: Unable to resolve path '{path}' in resource {resource.get('resourceType', 'unknown')}")
                print(f"Resource content: {json.dumps(resource, indent=2)}")
            return None
    return obj

def extract_vocab(system_url):
    if not system_url:
        return ''
    if 'loinc' in system_url.lower():
        return 'LOINC'
    if 'snomed' in system_url.lower():
        return 'SNOMED'
    if 'icd' in system_url.lower():
        return system_url.split('-')[-1].upper()
    return system_url.split('/')[-1].upper()

def build_event(resource, config, uuid_to_int=None, default_config=None):
    event = {}
    rtype = resource["resourceType"]
    if default_config:
        for key, value in default_config.items():
            if key not in config:
                config[key] = value
    if rtype == "Medication":
         print(resource)
    for key, exprs in config.items():
        if key == 'subject_id':
            rtype = resource.get('resourceType') if isinstance(resource, dict) else getattr(resource, 'resource_type', None)
            if rtype == "Patient":
                identifiers = resource.get('identifier', []) if isinstance(resource, dict) else getattr(resource, 'identifier', [])
                found = False
                for ident in identifiers:
                    system = ident.get('system') if isinstance(ident, dict) else getattr(ident, 'system', None)
                    value = ident.get('value') if isinstance(ident, dict) else getattr(ident, 'value', None)
                    if system and "identifier/patient" in system and value is not None:
                        try:
                            event['subject_id'] = int(value)
                        except Exception:
                            event['subject_id'] = value
                        found = True
                        break
                if not found:
                    event['subject_id'] = resource.get('id') if isinstance(resource, dict) else getattr(resource, 'id', None)
            else:
                for field in ['subject', 'patient']:
                    obj = resource.get(field) if isinstance(resource, dict) else getattr(resource, field, None)
                    if obj:
                        ref = obj.get('reference') if isinstance(obj, dict) else getattr(obj, 'reference', None)
                        if ref and ref.startswith("Patient/"):
                            patient_uuid = ref.split("/")[-1]
                            if uuid_to_int and patient_uuid in uuid_to_int:
                                event['subject_id'] = uuid_to_int[patient_uuid]
                            else:
                                event['subject_id'] = patient_uuid
                            break
                    else:
                        event['subject_id'] = None
        elif key == 'code' and isinstance(exprs, list):
            parts = []
            for expr in exprs:
                if expr.startswith('const('):
                    val = expr[6:-1]
                    if val == 'resourceType':
                        val = resource.get('resourceType') if isinstance(resource, dict) else getattr(resource, 'resource_type', None)
                    parts.append(str(val))
                elif expr.startswith('col('):
                    val = extract_path(resource, expr[4:-1], column_name='code')
                    if val is not None:
                        parts.append(str(val))
                elif expr.startswith('vocab('):
                    system_url = extract_path(resource, expr[6:-1], column_name='code')
                    parts.append(extract_vocab(system_url))
            event[key] = ''.join([str(x) for x in parts if x not in (None, '', 'null')])
        elif isinstance(exprs, list):
            for expr in exprs:
                if expr.startswith('col('):
                    val = extract_path(resource, expr[4:-1])
                    if val is not None:
                        event[key] = val
                        break
        elif isinstance(exprs, str) and exprs.startswith('col('):
            event[key] = extract_path(resource, exprs[4:-1])
        else:
            event[key] = exprs
    return event
