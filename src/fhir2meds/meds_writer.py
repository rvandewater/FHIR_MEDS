import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import polars as pl
import pyarrow.parquet as pq
from meds import DataSchema

logger = logging.getLogger(__name__)


def robust_cast_time_column(pl_df):
    """Parse FHIR date/dateTime strings in ``time`` into naive microsecond datetimes.

    Partial dates (``YYYY``, ``YYYY-MM``) are padded to the first day of the period. The UTC offset is dropped, so
    times are kept as local wall-clock time (as in MIMIC-IV). Values that cannot be parsed become null.

    Examples:
        >>> robust_cast_time_column(pl.DataFrame({"time": ["1950", "1950-03", "1950-03-04", "2148-07-07T16:18:13Z"]}))[
        ...     "time"].to_list()  # doctest: +NORMALIZE_WHITESPACE
        [datetime.datetime(1950, 1, 1, 0, 0), datetime.datetime(1950, 3, 1, 0, 0), datetime.datetime(1950, 3, 4, 0, 0),
         datetime.datetime(2148, 7, 7, 16, 18, 13)]
    """
    if "time" in pl_df.columns:
        time_str = (
            pl.col("time")
            .cast(pl.Utf8, strict=False)  # Cast to String
            .str.replace("Z$", "")
            .str.replace(r"([+-][0-9]{2}:[0-9]{2})$", "")
            .str.replace(r"^([0-9]{4})$", "${1}-01-01")
            .str.replace(r"^([0-9]{4}-[0-9]{2})$", "${1}-01")
        )
        pl_df = pl_df.with_columns(
            pl.coalesce(
                time_str.str.to_datetime("%Y-%m-%dT%H:%M:%S%.f", time_unit="us", strict=False),
                time_str.str.to_datetime("%Y-%m-%dT%H:%M", time_unit="us", strict=False),
                time_str.str.to_datetime("%Y-%m-%d", time_unit="us", strict=False),
            ).alias("time")
        )
    return pl_df


def cast_to_meds_schema(pl_df):
    if "subject_id" in pl_df.columns:
        pl_df = pl_df.with_columns(pl.col("subject_id").cast(pl.Int64, strict=False))
    pl_df = robust_cast_time_column(pl_df)
    if "code" in pl_df.columns:
        pl_df = pl_df.with_columns(pl.col("code").cast(pl.Utf8, strict=False))
    if "numeric_value" in pl_df.columns:
        pl_df = pl_df.with_columns(pl.col("numeric_value").cast(pl.Float32, strict=False))
    if "text_value" in pl_df.columns:
        pl_df = pl_df.with_columns(pl.col("text_value").cast(pl.Utf8, strict=False))
    return pl_df


def cast_arrow_table_to_meds_schema(arrow_table):
    return arrow_table.select(DataSchema.schema().names).cast(DataSchema.schema(), safe=False)


def shard_events_by_subject(events: list[dict[str, Any]], shard_size: int) -> list[list[dict[str, Any]]]:
    """Group events into shards of roughly ``shard_size`` events, never splitting a subject across shards.

    MEDS requires all of a subject's events to live in a single shard. Subjects are assigned to shards in sorted
    order, and a shard is closed once it reaches ``shard_size`` events (so a single very large subject can exceed it).

    Examples:
        >>> evs = [{"subject_id": s} for s in [2, 1, 2, 3, 1, 2]]
        >>> [[e["subject_id"] for e in shard] for shard in shard_events_by_subject(evs, shard_size=2)]
        [[1, 1], [2, 2, 2], [3]]
    """
    by_subject: dict[Any, list[dict[str, Any]]] = {}
    for event in events:
        by_subject.setdefault(event["subject_id"], []).append(event)
    shards, current = [], []
    for subject_id in sorted(by_subject):
        current.extend(by_subject[subject_id])
        if len(current) >= shard_size:
            shards.append(current)
            current = []
    if current:
        shards.append(current)
    return shards


def write_single_shard(shard, required_cols, output_dir, shard_idx, verbose=False):
    data_dir = os.path.join(output_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    pl_df = pl.DataFrame(shard, infer_schema_length=None)
    for col in required_cols:
        if col not in pl_df.columns:
            pl_df = pl_df.with_columns(pl.lit(None).alias(col))
    pl_df = cast_to_meds_schema(pl_df.select(required_cols))
    n_null_subjects = pl_df.select(pl.col("subject_id").is_null().sum()).item()
    if n_null_subjects:
        logger.warning(f"Shard {shard_idx}: dropping {n_null_subjects} events with a null subject_id")
    pl_df = pl_df.filter(pl.col("subject_id").is_not_null()).sort(
        "subject_id", "time", nulls_last=False, maintain_order=True
    )
    arrow_table = cast_arrow_table_to_meds_schema(pl_df.to_arrow())
    DataSchema.validate(arrow_table)
    pq.write_table(arrow_table, os.path.join(data_dir, f"{shard_idx}.parquet"))
    if verbose:
        logger.info(f"Shard {shard_idx} written with {arrow_table.num_rows} events.")


def write_meds_sharded_parquet(
    events: list[dict[str, Any]],
    output_dir: str,
    shard_size: int = 10000,
    max_workers: int = 4,
    verbose: bool = False,
):
    """Write events as MEDS data shards. Any failure while writing a shard is raised, never swallowed."""
    os.makedirs(output_dir, exist_ok=True)
    required_cols = list(DataSchema.schema().names)
    shards = shard_events_by_subject(events, shard_size)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(write_single_shard, shard, required_cols, output_dir, idx, verbose)
            for idx, shard in enumerate(shards)
        ]
        for future in futures:
            future.result()
