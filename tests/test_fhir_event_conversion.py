import pytest
from types import SimpleNamespace

# Helper to resolve dotted paths like 'code.coding[0].code' on dicts or objects
def extract_path(obj, path):
    parts = path.split('.')
    for part in parts:
        if '[' in part and ']' in part:
            field, idx = part[:-1].split('[')
            idx = int(idx)
            obj = getattr(obj, field, None) if hasattr(obj, field) else obj.get(field, None)
            if obj is None or not isinstance(obj, (list, tuple)) or len(obj) <= idx:
                return None
            obj = obj[idx]
        else:
            obj = getattr(obj, part, None) if hasattr(obj, part) else obj.get(part, None)
        if obj is None:
            return None
    return obj

# Config for test (subset)
event_config = {
    'resources': ['Patient', 'Observation', 'MedicationRequest', 'Condition', 'Procedure'],
    'Patient': {
        'code': ['identifier[0].value'],
        'time': ['birthDate'],
    },
    'Observation': {
        'code': ['code.coding[0].code', 'identifier[0].value'],
        'time': ['effectiveDateTime', 'effectivePeriod.start', 'issued'],
    },
    'MedicationRequest': {
        'code': ['medicationCodeableConcept.coding[0].code', 'medicationReference.identifier.value'],
        'time': ['authoredOn'],
    },
    'Condition': {
        'code': ['code.coding[0].code'],
        'time': ['onsetDateTime', 'recordedDate'],
    },
    'Procedure': {
        'code': ['code.coding[0].code'],
        'time': ['performedDateTime', 'performedPeriod.start'],
    },
}

def config_driven_event(resource, resource_type, config):
    conf = config[resource_type]
    code = None
    for path in conf['code']:
        code = extract_path(resource, path)
        if code:
            break
    time = None
    for path in conf['time']:
        time = extract_path(resource, path)
        if time:
            break
    return {'code': code, 'time': time}

def test_patient():
    resource = SimpleNamespace(identifier=[SimpleNamespace(value='PAT123')], birthDate='2000-01-01')
    event = config_driven_event(resource, 'Patient', event_config)
    assert event['code'] == 'PAT123'
    assert event['time'] == '2000-01-01'

def test_observation():
    resource = SimpleNamespace(
        code=SimpleNamespace(coding=[SimpleNamespace(code='LOINC123')]),
        effectiveDateTime='2021-01-01T00:00:00Z',
        identifier=[SimpleNamespace(value='OBS456')],
        issued=None
    )
    event = config_driven_event(resource, 'Observation', event_config)
    assert event['code'] == 'LOINC123'
    assert event['time'] == '2021-01-01T00:00:00Z'

def test_medication_request():
    resource = SimpleNamespace(
        medicationCodeableConcept=SimpleNamespace(coding=[SimpleNamespace(code='RX789')]),
        authoredOn='2022-02-02',
        medicationReference=SimpleNamespace(identifier=SimpleNamespace(value='ALT_RX'))
    )
    event = config_driven_event(resource, 'MedicationRequest', event_config)
    assert event['code'] == 'RX789'
    assert event['time'] == '2022-02-02'

def test_condition():
    resource = SimpleNamespace(
        code=SimpleNamespace(coding=[SimpleNamespace(code='COND111')]),
        onsetDateTime='2020-05-05',
        recordedDate=None
    )
    event = config_driven_event(resource, 'Condition', event_config)
    assert event['code'] == 'COND111'
    assert event['time'] == '2020-05-05'

def test_procedure():
    resource = SimpleNamespace(
        code=SimpleNamespace(coding=[SimpleNamespace(code='PROC222')]),
        performedDateTime=None,
        performedPeriod=SimpleNamespace(start='2019-09-09')
    )
    event = config_driven_event(resource, 'Procedure', event_config)
    assert event['code'] == 'PROC222'
    assert event['time'] == '2019-09-09' 