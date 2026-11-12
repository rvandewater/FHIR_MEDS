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

Every table listed in `messy.yaml` is written by the `fhir_pre_meds` stage, without rows when the input has none
of that resource type, so the spec needs no changes per dataset. Run a modified copy of the spec by path:
`meds-extract-run spec=./my_messy.yaml output_dir=... do_download=false input_dir=path/to/fhir`.

!!! tip

    Inspect `<output_dir>/fhir_pre_meds/<ResourceType>.parquet` to see which columns are available. To rerun only
    the MEDS-Extract stages with a changed spec, delete `<output_dir>/.logs/*.done` except `fhir_pre_meds.done`, and
    run the same command with `spec=./my_messy.yaml`.
