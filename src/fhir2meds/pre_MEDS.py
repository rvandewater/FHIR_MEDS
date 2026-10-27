"""
pre_MEDS.py
-----------
Flatten nested FHIR resources into one flat, typed parquet table per resource type, ready for MEDS-Extract.

This is the only FHIR-specific stage of the pipeline. It:

1. streams every resource out of the input files (NDJSON / Bundles, optionally gzipped; see ``fhir_parser``),
2. extracts the columns described in ``configs/flatten.yaml`` into staged parquet parts,
3. resolves cross-resource links with polars: Patient -> integer ``subject_id``, ``medicationReference`` ->
   ``Medication.code``, and Encounter times for resources without their own time (e.g. MIMIC diagnoses),
4. parses FHIR date/dateTime strings and writes ``<pre_MEDS_dir>/<ResourceType>.parquet``.

Everything after this (event construction, subject sharding, splits, metadata) is done by MEDS-Extract from
``configs/messy.yaml``.
"""

import logging
import shutil
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import polars as pl
import yaml

from .fhir_parser import (
    extract_path,
    iter_fhir_files,
    iter_resources,
    parse_patient_reference,
)
from .subject_ids import build_subject_id_map, subject_id_from_identifiers
from .vocabulary import extract_vocab

logger = logging.getLogger(__name__)

FLATTEN_CFG = Path(__file__).parent / "configs" / "flatten.yaml"

CORE_STRING_COLUMNS = [
    "resource_type",
    "resource_id",
    "patient_id",
    "encounter_id",
    "code_system",
    "code",
    "code_display",
    "start",
    "end",
    "value_unit",
    "value_text",
    "medication_ref",
]
PATIENT_SCHEMA = {
    "resource_type": pl.Utf8,
    "patient_id": pl.Utf8,
    "identifier_subject_id": pl.Int64,
    "birth_date": pl.Utf8,
    "deceased_datetime": pl.Utf8,
    "gender": pl.Utf8,
    "race": pl.Utf8,
    "ethnicity": pl.Utf8,
}
US_CORE_RACE = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race"
US_CORE_ETHNICITY = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity"


def load_flatten_spec(fhir_version: str = "R4", path: str | Path = FLATTEN_CFG) -> dict[str, dict]:
    """Load the per-resource-type flattening spec for ``fhir_version``.

    Examples:
        >>> spec = load_flatten_spec("R4")
        >>> spec["Procedure"]["start"]
        ['performedDateTime', 'performedPeriod.start']
        >>> load_flatten_spec("R5")["Procedure"]["start"]
        ['occurrenceDateTime', 'occurrencePeriod.start']
        >>> load_flatten_spec("R5")["Observation"] == spec["Observation"]
        True
    """
    with open(path) as f:
        cfg = yaml.safe_load(f)
    if fhir_version not in cfg:
        raise ValueError(f"FHIR version {fhir_version} not found in {path}")
    return cfg[fhir_version]


def table_schema(type_spec: dict) -> dict[str, pl.DataType]:
    schema = {c: pl.Utf8 for c in CORE_STRING_COLUMNS}
    schema["value_num"] = pl.Float64
    schema["component_index"] = pl.Int32
    for col in type_spec.get("extra") or {}:
        schema[col] = pl.Utf8
    return schema


def _first(resource: Any, paths: list[str] | None) -> Any:
    for path in paths or []:
        val = extract_path(resource, path)
        if val not in (None, "", []):
            return val
    return None


def first_coding(concept: Any) -> dict | None:
    """Return the first coding with a code from a CodeableConcept, or the Coding itself.

    Examples:
        >>> first_coding({"coding": [{"system": "s"}, {"system": "s", "code": "1"}], "text": "t"})
        {'system': 's', 'code': '1'}
        >>> first_coding({"system": "s", "code": "AMB"})
        {'system': 's', 'code': 'AMB'}
        >>> first_coding({"text": "free text only"}) is None
        True
    """
    if not isinstance(concept, dict):
        return None
    if concept.get("code") is not None:
        return concept
    for coding in concept.get("coding") or []:
        if isinstance(coding, dict) and coding.get("code") is not None:
            return coding
    return None


def _first_coding(resource: Any, paths: list[str] | None) -> tuple[dict | None, str | None]:
    for path in paths or []:
        concept = extract_path(resource, path)
        coding = first_coding(concept)
        if coding is not None:
            text = concept.get("text") if isinstance(concept, dict) else None
            return coding, text
    return None, None


def observation_value(obj: dict) -> tuple[float | None, str | None, str | None]:
    """Extract ``(value_num, value_unit, value_text)`` from an Observation (or component) ``value[x]``.

    Examples:
        >>> observation_value({"valueQuantity": {"value": 5.1, "unit": "mmol/L"}})
        (5.1, 'mmol/L', None)
        >>> positive = {"coding": [{"code": "10828004", "display": "Positive"}]}
        >>> observation_value({"valueCodeableConcept": positive})
        (None, None, 'Positive')
        >>> observation_value({"valueBoolean": False})
        (None, None, 'false')
        >>> observation_value({"valueInteger": 3})
        (3.0, None, None)
    """
    if isinstance(obj.get("valueQuantity"), dict):
        q = obj["valueQuantity"]
        num = q.get("value")
        return (float(num) if isinstance(num, int | float) else None), (q.get("unit") or q.get("code")), None
    if isinstance(obj.get("valueInteger"), int | float):
        return float(obj["valueInteger"]), None, None
    if isinstance(obj.get("valueCodeableConcept"), dict):
        cc = obj["valueCodeableConcept"]
        coding = first_coding(cc) or {}
        return None, None, cc.get("text") or coding.get("display") or coding.get("code")
    if isinstance(obj.get("valueBoolean"), bool):
        return None, None, str(obj["valueBoolean"]).lower()
    for key in ("valueString", "valueDateTime", "valueTime"):
        if isinstance(obj.get(key), str):
            return None, None, obj[key]
    return None, None, None


def _reference_id(ref: Any, resource_type: str) -> str | None:
    """Logical id from a ``<Type>/<id>``, absolute, or ``urn:uuid:<id>`` reference string."""
    if not isinstance(ref, str) or not ref:
        return None
    if ref.startswith("urn:uuid:"):
        return ref[len("urn:uuid:") :]
    parts = ref.split("/")
    if "_history" in parts:
        parts = parts[: parts.index("_history")]
    if len(parts) >= 2 and parts[-2] == resource_type:
        return parts[-1]
    return None


def flatten_resource(resource: dict, type_spec: dict) -> list[dict]:
    """Flatten one non-Patient resource into one or more rows (Observation components become extra rows).

    Examples:
        >>> spec = load_flatten_spec("R4")["Observation"]
        >>> bp = {"resourceType": "Observation", "id": "o1", "subject": {"reference": "urn:uuid:p1"},
        ...       "effectiveDateTime": "2020-01-01T10:00:00+01:00",
        ...       "code": {"coding": [{"system": "http://loinc.org", "code": "85354-9"}]},
        ...       "component": [{"code": {"coding": [{"system": "http://loinc.org", "code": "8480-6"}]},
        ...                      "valueQuantity": {"value": 120, "unit": "mm[Hg]"}}]}
        >>> rows = flatten_resource(bp, spec)
        >>> [(r["code"], r["value_num"], r["component_index"], r["patient_id"]) for r in rows]
        [('85354-9', None, None, 'p1'), ('8480-6', 120.0, 0, 'p1')]
    """
    coding, text = _first_coding(resource, type_spec.get("code"))
    coding = coding or {}
    patient_id = None
    for field in ("subject", "patient"):
        ref = extract_path(resource, f"{field}.reference")
        patient_id = parse_patient_reference(ref)
        if patient_id is not None:
            break
    row = {
        "resource_type": resource.get("resourceType"),
        "resource_id": resource.get("id"),
        "patient_id": patient_id,
        "encounter_id": _reference_id(
            extract_path(resource, "encounter.reference") or extract_path(resource, "context.reference"),
            "Encounter",
        ),
        "code_system": coding.get("system"),
        "code": coding.get("code"),
        "code_display": coding.get("display") or text,
        "start": _first(resource, type_spec.get("start")),
        "end": _first(resource, type_spec.get("end")),
        "value_num": None,
        "value_unit": None,
        "value_text": None,
        "medication_ref": _reference_id(_first(resource, type_spec.get("medication_ref")), "Medication"),
        "component_index": None,
    }
    for col, path in (type_spec.get("extra") or {}).items():
        val = extract_path(resource, path)
        row[col] = None if val is None else str(val)

    if type_spec.get("value") == "observation":
        row["value_num"], row["value_unit"], row["value_text"] = observation_value(resource)
    else:
        num = _first(resource, type_spec.get("value_num"))
        row["value_num"] = float(num) if isinstance(num, int | float) and not isinstance(num, bool) else None
        unit = _first(resource, type_spec.get("value_unit"))
        row["value_unit"] = None if unit is None else str(unit)
        text = _first(resource, type_spec.get("value_text"))
        row["value_text"] = None if text is None else str(text)

    rows = [row]
    if type_spec.get("value") == "observation":
        for idx, component in enumerate(resource.get("component") or []):
            comp_coding, comp_text = _first_coding(component, ["code"])
            if comp_coding is None:
                continue
            num, unit, txt = observation_value(component)
            rows.append(
                {
                    **row,
                    "code_system": comp_coding.get("system"),
                    "code": comp_coding.get("code"),
                    "code_display": comp_coding.get("display") or comp_text,
                    "value_num": num,
                    "value_unit": unit,
                    "value_text": txt,
                    "component_index": idx,
                }
            )
    return rows


def _extension_text(resource: dict, url: str) -> str | None:
    for ext in resource.get("extension") or []:
        if ext.get("url") != url:
            continue
        sub = {s.get("url"): s for s in ext.get("extension") or []}
        if "text" in sub and sub["text"].get("valueString"):
            return sub["text"]["valueString"]
        if "ombCategory" in sub:
            return (sub["ombCategory"].get("valueCoding") or {}).get("display")
    return None


def flatten_patient(resource: dict, subject_id_suffixes: tuple[str, ...]) -> dict:
    """Flatten a Patient into demographics, birth and death columns.

    Examples:
        >>> p = {"resourceType": "Patient", "id": "p1", "gender": "female", "birthDate": "1970",
        ...      "identifier": [{"system": "x/identifier/patient", "value": "42"}],
        ...      "extension": [{"url": US_CORE_RACE, "extension": [{"url": "text", "valueString": "White"}]}]}
        >>> r = flatten_patient(p, ("/identifier/patient",))
        >>> r["identifier_subject_id"], r["birth_date"], r["race"], r["deceased_datetime"]
        (42, '1970', 'White', None)
    """
    return {
        "resource_type": "Patient",
        "patient_id": resource.get("id"),
        "identifier_subject_id": subject_id_from_identifiers(resource.get("identifier"), subject_id_suffixes),
        "birth_date": resource.get("birthDate"),
        "deceased_datetime": resource.get("deceasedDateTime"),
        "gender": resource.get("gender"),
        "race": _extension_text(resource, US_CORE_RACE),
        "ethnicity": _extension_text(resource, US_CORE_ETHNICITY),
    }


def flatten_medication(resource: dict) -> dict:
    coding, text = _first_coding(resource, ["code"])
    coding = coding or {}
    return {
        "medication_id": resource.get("id"),
        "med_code_system": coding.get("system"),
        "med_code": coding.get("code"),
        "med_code_display": coding.get("display") or text,
    }


MEDICATION_SCHEMA = {c: pl.Utf8 for c in ("medication_id", "med_code_system", "med_code", "med_code_display")}


class _PartWriter:
    """Buffer rows per table and spill them to numbered parquet parts with a fixed schema."""

    def __init__(self, staging_dir: Path, schemas: dict[str, dict], flush_every: int = 100_000):
        self.staging_dir = staging_dir
        self.schemas = schemas
        self.flush_every = flush_every
        self.buffers: dict[str, list[dict]] = defaultdict(list)
        self.n_parts: Counter = Counter()

    def add(self, table: str, rows: list[dict]) -> None:
        self.buffers[table].extend(rows)
        if len(self.buffers[table]) >= self.flush_every:
            self.flush(table)

    def flush(self, table: str) -> None:
        rows = self.buffers.pop(table, [])
        if not rows:
            return
        out_dir = self.staging_dir / table
        out_dir.mkdir(parents=True, exist_ok=True)
        df = pl.DataFrame(rows, schema=self.schemas[table])
        df.write_parquet(out_dir / f"part-{self.n_parts[table]:05d}.parquet")
        self.n_parts[table] += 1

    def close(self) -> None:
        for table in list(self.buffers):
            self.flush(table)

    def scan(self, table: str) -> pl.LazyFrame:
        if self.n_parts[table] == 0:
            return pl.LazyFrame(schema=self.schemas[table])
        return pl.scan_parquet(self.staging_dir / table / "*.parquet")


def stage_resources(
    raw_input_dir: Path,
    staging_dir: Path,
    flatten_spec: dict[str, dict],
    subject_id_suffixes: tuple[str, ...],
    max_resources_per_type: int | None = None,
) -> tuple[_PartWriter, Counter]:
    """Stream all input files into per-type staged parquet parts. Returns the writer and per-type counts."""
    schemas = {rtype: table_schema(spec) for rtype, spec in flatten_spec.items()}
    schemas["Patient"] = PATIENT_SCHEMA
    schemas["Medication"] = MEDICATION_SCHEMA
    writer = _PartWriter(staging_dir, schemas)
    counts: Counter = Counter()
    stats: Counter = Counter()
    for fpath in iter_fhir_files(raw_input_dir):
        logger.info(f"Flattening {fpath}")
        for resource in iter_resources(fpath, stats):
            rtype = resource.get("resourceType")
            if max_resources_per_type is not None and counts[rtype] >= max_resources_per_type:
                continue
            if rtype == "Patient":
                writer.add("Patient", [flatten_patient(resource, subject_id_suffixes)])
            elif rtype == "Medication":
                writer.add("Medication", [flatten_medication(resource)])
            elif rtype in flatten_spec:
                writer.add(rtype, flatten_resource(resource, flatten_spec[rtype]))
            else:
                stats[f"ignored:{rtype}"] += 1
                continue
            counts[rtype] += 1
    writer.close()
    if stats:
        logger.info(f"Input stats: {dict(stats)}")
    return writer, counts


def parse_fhir_datetime(col: str | pl.Expr, keep_local_time: bool = False) -> pl.Expr:
    """Parse FHIR ``date``/``dateTime``/``instant`` strings into naive microsecond datetimes.

    Values with a UTC offset are converted to UTC (or, with ``keep_local_time``, keep their local wall-clock
    time); values without one are taken as-is. Partial dates are padded to the start of the period.
    Unparsable -> null.

    Examples:
        >>> df = pl.DataFrame({"t": ["2020-03-01T10:00:00+01:00", "2020-03-01T10:00:00.5Z",
        ...                          "2020-03-01T10:00:00",
        ...                          "2020-03-01", "2020-03", "2020", "garbage", None]})
        >>> df.select(parse_fhir_datetime("t"))["t"].to_list()  # doctest: +NORMALIZE_WHITESPACE
        [datetime.datetime(2020, 3, 1, 9, 0), datetime.datetime(2020, 3, 1, 10, 0, 0, 500000),
         datetime.datetime(2020, 3, 1, 10, 0), datetime.datetime(2020, 3, 1, 0, 0),
         datetime.datetime(2020, 3, 1, 0, 0), datetime.datetime(2020, 1, 1, 0, 0), None, None]
        >>> df.select(parse_fhir_datetime("t", keep_local_time=True))["t"][0]
        datetime.datetime(2020, 3, 1, 10, 0)
    """
    expr = pl.col(col) if isinstance(col, str) else col
    s = (
        expr.cast(pl.Utf8)
        .str.strip_chars()
        .str.replace(r"Z$", "+00:00")
        .str.replace(r"^([0-9]{4})$", "${1}-01-01")
        .str.replace(r"^([0-9]{4}-[0-9]{2})$", "${1}-01")
    )
    if keep_local_time:
        s = s.str.replace(r"[+-][0-9]{2}:[0-9]{2}$", "")
    with_offset = [
        s.str.to_datetime(fmt, time_unit="us", time_zone="UTC", strict=False).dt.replace_time_zone(None)
        for fmt in ("%Y-%m-%dT%H:%M:%S%.f%:z", "%Y-%m-%dT%H:%M%:z")
    ]
    naive = [
        s.str.to_datetime(fmt, time_unit="us", strict=False)
        for fmt in ("%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%dT%H:%M", "%Y-%m-%d")
    ]
    return pl.coalesce(*with_offset, *naive)


def add_code_vocab(df: pl.DataFrame) -> pl.DataFrame:
    """Add ``code_vocab``: a short vocabulary name per ``code_system`` (computed once per distinct system).

    Examples:
        >>> add_code_vocab(pl.DataFrame({"code_system": ["http://loinc.org", None]}))["code_vocab"].to_list()
        ['LOINC', 'UNKNOWN']
    """
    systems = df["code_system"].unique().drop_nulls().to_list()
    mapping = {s: extract_vocab(s) or "UNKNOWN" for s in systems}
    return df.with_columns(
        pl.col("code_system")
        .replace_strict(mapping, default="UNKNOWN", return_dtype=pl.Utf8)
        .alias("code_vocab")
    )


def finalize_tables(
    writer: _PartWriter,
    pre_MEDS_dir: Path,
    flatten_spec: dict[str, dict],
    keep_local_time: bool = False,
) -> dict[str, int]:
    """Resolve links, parse times and write ``<pre_MEDS_dir>/<ResourceType>.parquet``. Returns row counts."""
    pre_MEDS_dir.mkdir(parents=True, exist_ok=True)

    patients = writer.scan("Patient").collect().unique("patient_id", keep="first", maintain_order=True)
    patients = patients.filter(pl.col("patient_id").is_not_null())
    if patients.height == 0:
        raise ValueError("No Patient resources found; cannot assign MEDS subject_ids.")
    id_map = build_subject_id_map(
        patients["patient_id"].to_list(), patients["identifier_subject_id"].to_list()
    )
    subject_ids = pl.DataFrame(
        {"patient_id": list(id_map.keys()), "subject_id": list(id_map.values())},
        schema={"patient_id": pl.Utf8, "subject_id": pl.Int64},
    )
    subject_ids.write_parquet(pre_MEDS_dir / "subject_id_map.parquet")

    row_counts = {}
    patient_out = (
        patients.join(subject_ids, on="patient_id", how="inner")
        .with_columns(
            parse_fhir_datetime("birth_date", keep_local_time).alias("birth_date"),
            parse_fhir_datetime("deceased_datetime", keep_local_time).alias("deceased_datetime"),
        )
        .drop("identifier_subject_id")
    )
    patient_out.write_parquet(pre_MEDS_dir / "Patient.parquet")
    row_counts["Patient"] = patient_out.height

    medications = writer.scan("Medication").unique("medication_id", keep="first")
    encounters = (
        writer.scan("Encounter")
        .filter(pl.col("component_index").is_null() & pl.col("resource_id").is_not_null())
        .select(
            pl.col("resource_id").alias("encounter_id"),
            pl.col("start").alias("encounter_start"),
            pl.col("end").alias("encounter_end"),
        )
        .unique("encounter_id", keep="first")
        if "Encounter" in flatten_spec
        else None
    )

    for rtype, type_spec in flatten_spec.items():
        if writer.n_parts[rtype] == 0:
            continue
        lf = writer.scan(rtype)
        # De-duplicate resources shipped in more than one file (e.g. MIMIC's Specimen and SpecimenLab).
        lf = pl.concat(
            [
                lf.filter(pl.col("resource_id").is_not_null()).unique(
                    ["resource_id", "component_index"], keep="first", maintain_order=True
                ),
                lf.filter(pl.col("resource_id").is_null()),
            ]
        )
        n_in = lf.select(pl.len()).collect().item()
        lf = lf.join(subject_ids.lazy(), on="patient_id", how="inner")

        if type_spec.get("medication_ref"):
            lf = (
                lf.join(medications, left_on="medication_ref", right_on="medication_id", how="left")
                .with_columns(
                    pl.when(pl.col("code").is_null())
                    .then(
                        pl.struct(
                            code_system="med_code_system", code="med_code", code_display="med_code_display"
                        )
                    )
                    .otherwise(pl.struct("code_system", "code", "code_display"))
                    .struct.unnest()
                )
                .drop("med_code_system", "med_code", "med_code_display")
            )

        fallback = type_spec.get("start_fallback")
        if fallback and encounters is not None:
            lf = lf.join(encounters, on="encounter_id", how="left").with_columns(
                pl.coalesce("start", fallback).alias("start")
            )
            lf = lf.drop("encounter_start", "encounter_end")

        lf = lf.with_columns(
            parse_fhir_datetime("start", keep_local_time).alias("start"),
            parse_fhir_datetime("end", keep_local_time).alias("end"),
        )
        df = add_code_vocab(lf.collect())
        dropped = n_in - df.height
        if dropped:
            logger.warning(f"{rtype}: dropped {dropped}/{n_in} rows referencing no known Patient.")
        df.write_parquet(pre_MEDS_dir / f"{rtype}.parquet")
        row_counts[rtype] = df.height
    return row_counts


def run_pre_MEDS(
    raw_input_dir: Path,
    pre_MEDS_dir: Path,
    fhir_version: str = "R4",
    subject_id_suffixes: tuple[str, ...] = ("/identifier/patient",),
    keep_local_time: bool = False,
    max_resources_per_type: int | None = None,
) -> dict[str, int]:
    """Flatten all FHIR resources in ``raw_input_dir`` into ``pre_MEDS_dir``. Returns row counts per table."""
    flatten_spec = load_flatten_spec(fhir_version)
    staging_dir = pre_MEDS_dir / ".staging"
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    writer, counts = stage_resources(
        raw_input_dir, staging_dir, flatten_spec, tuple(subject_id_suffixes), max_resources_per_type
    )
    logger.info(f"Staged resources: {dict(counts)}")
    row_counts = finalize_tables(writer, pre_MEDS_dir, flatten_spec, keep_local_time)
    shutil.rmtree(staging_dir)
    logger.info(f"pre-MEDS tables: {row_counts}")
    return row_counts
