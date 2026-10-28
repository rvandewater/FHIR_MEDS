# Customizing the mapping

There are two configuration files, one for each stage of the [pipeline](design.md):

| file                                 | controls                                                   |
| ------------------------------------ | ---------------------------------------------------------- |
| `src/fhir2meds/configs/flatten.yaml` | which FHIR elements are read into the flat pre-MEDS tables |
| `src/fhir2meds/configs/messy.yaml`   | which MEDS events are built from those tables              |

## Reading another element or resource type

Each resource type in `flatten.yaml` lists FHIR paths to try in order. Paths are dotted, with `[n]` for list
indices:

```yaml
R4:
  Goal:
    code: [description]            # CodeableConcept or Coding paths
    start: [startDate, statusDate]
    end: ['target[0].dueDate']
    value_text: [lifecycleStatus]
    extra:                          # extra string columns available to messy.yaml
      achievement: achievementStatus.coding[0].code
```

The other keys are:

- `value: observation`: use Observation `value[x]` handling, and explode `component`.
- `medication_ref`: resolve `Medication.code` when the resource has no code of its own.
- `start_fallback: encounter_end | encounter_start`: use the encounter's time when no start resolves.

Resource types that aren't listed are ignored.

## Changing the events

`messy.yaml` is a standard [MEDS-Extract MESSY spec](https://github.com/mmcdermott/MEDS_extract#-event-configuration-deep-dive):
one top-level key per pre-MEDS table, with one or more events under it. Values are
[dftly](https://github.com/mmcdermott/dftly) expressions over the table's columns. To emit `Goal` events, add:

```yaml
Goal:
  goal: *coded_event
```

To put units in Observation codes instead of in the `unit` column:

```yaml
Observation:
  observation:
    code: f"{$resource_type}//{$code_vocab}//{$code}//{$value_unit ?? 'UNK'}"
    time: $start
    numeric_value: $value_num
    text_value: $value_text
```

Tables listed in `messy.yaml` that the input doesn't contain are dropped at run time. The spec actually used is
saved to `pre_MEDS/messy.yaml`.

!!! tip
    Inspect `pre_MEDS/<ResourceType>.parquet` to see which columns are available, and rerun only the MEDS-Extract
    stage with `meds-extract-run spec=output/pre_MEDS/messy.yaml input_dir=output/pre_MEDS output_dir=... do_download=false`.
