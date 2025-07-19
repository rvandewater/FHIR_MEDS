import os
import traceback

import pyarrow.parquet as pq
from typing import List, Dict, Any
import polars as pl
from meds import DataSchema
from concurrent.futures import ThreadPoolExecutor
import pyarrow as pa

def robust_cast_time_column(pl_df):
    if "time" in pl_df.columns:
        # Remove trailing Z and timezone offset, then parse as naive datetime
        print(pl_df.head(5))
        pl_df = pl_df.with_columns(
            pl.when(pl.col("time").is_not_null())
            .then(
                pl.col("time")
                .cast(pl.Utf8, strict=False)  # Cast to String
                .str.replace("Z$", "")
                .str.replace(r"([+-][0-9]{2}:[0-9]{2})$", "")
                # .str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.f", strict=False)
                .str.to_datetime()
                .dt.replace_time_zone(None)  # Remove timezone info
                # .dt.timestamp("us")
                # .cast(pl.Datetime("us"), strict=False)
            )
            .otherwise(None)
            .alias("time")
        )
    print(pl_df.head(5))
    return pl_df

def cast_to_meds_schema(pl_df):
    # Todo: check whats happening with
    # subject_id: Int64
    if "subject_id" in pl_df.columns:
        pl_df = pl_df.with_columns(pl.col("subject_id").cast(pl.Int64, strict=False))
    # time: Datetime (microseconds, no timezone)
    # if "time" in pl_df.columns and pl_df["time"].is_null().sum() > 0:
    pl_df = robust_cast_time_column(pl_df)
    # code: Utf8 (string)
    if "code" in pl_df.columns:
        pl_df = pl_df.with_columns(pl.col("code").cast(pl.Utf8, strict=False))
    # numeric_value: Float32
    if "numeric_value" in pl_df.columns:
        pl_df = pl_df.with_columns(pl.col("numeric_value").cast(pl.Float32, strict=False))
    # text_value: Utf8 (Arrow large_string)
    if "text_value" in pl_df.columns and not pl_df["text_value"].is_null().sum() > 0:
        pl_df = pl_df.with_columns(pl.col("text_value").cast(pl.Utf8, strict=False))
    return pl_df

def cast_arrow_table_to_meds_schema(arrow_table):
    schema = pa.schema([
        pa.field("subject_id", pa.int64(), nullable=False),
        pa.field("time", pa.timestamp("us"), nullable=True),
        pa.field("code", pa.string(), nullable=False),
        pa.field("numeric_value", pa.float32(), nullable=True),
        pa.field("text_value", pa.large_string(), nullable=True),
    ])
    # Only cast columns that exist in the table
    fields = [f for f in schema if f.name in arrow_table.schema.names]
    cast_schema = pa.schema(fields)
    return arrow_table.cast(cast_schema, safe=False)

def cast_arrow_code_to_string(arrow_table):
    schema = arrow_table.schema
    fields = []
    for field in schema:
        if field.name == "code" and pa.types.is_large_string(field.type):
            fields.append(pa.field("code", pa.string()))
        else:
            fields.append(field)
    new_schema = pa.schema(fields)
    # Cast the table to the new schema
    return arrow_table.cast(new_schema)

def build_patient_id_map(patient_dir):
    import os
    import json
    uuid_to_int = {}
    for fname in os.listdir(patient_dir):
        if fname.endswith(".json") or fname.endswith(".ndjson"):
            with open(os.path.join(patient_dir, fname)) as f:
                for line in f:
                    if not line.strip():
                        continue
                    data = json.loads(line)
                    if data.get("resourceType") == "Patient":
                        uuid = data["id"]
                        for ident in data.get("identifier", []):
                            if ident.get("system", "").endswith("/identifier/patient"):
                                uuid_to_int[uuid] = int(ident["value"])
    return uuid_to_int 

def safe_str(val):
    if val is None:
        return None
    if isinstance(val, str):
        return val
    # If it's a datetime, convert to ISO string
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return str(val) 


def write_single_shard(shard, required_cols, output_dir, shard_idx, verbose=False):
    try:
        data_dir = os.path.join(output_dir, "data")

        os.makedirs(data_dir, exist_ok=True)
        if verbose:
            print(f"Writing shard {shard_idx} with {len(shard)} events to {data_dir}")
        if shard_idx ==1:
            print(f"Shard {shard_idx} has {len(shard)} events.")
        pl_df = pl.DataFrame(shard, infer_schema_length=10000)
        if shard_idx == 1:
            print(f"shard test: {shard_idx}")
            print(f"time {shard_idx}: {pl_df['time'].is_null().sum()}")
            print(f"text_value {shard_idx}: {pl_df['text_value'].is_null().sum()}")
            print(f"code {shard_idx}: {pl_df['code'].is_null().sum()}")
            print(f"numeric_value {shard_idx}: {pl_df['numeric_value'].is_null().sum()}")
            print(f"subject_id {shard_idx}: {pl_df['subject_id'].is_null().sum()}")
        for col in required_cols:
            if col not in pl_df.columns:
                pl_df = pl_df.with_columns(pl.lit(None).alias(col))
        pl_df = pl_df.select(required_cols)
        pl_df = cast_to_meds_schema(pl_df)
        # if verbose:
        #     # print("subject_id values before filtering:", pl_df["subject_id"])
        null_rows = pl_df.filter(pl.col("subject_id").is_null())
        if verbose and null_rows.height > 0:
            print("Rows with null subject_id:", null_rows)
        pl_df = pl_df.filter(pl.col("subject_id").is_not_null())
        arrow_table = pl_df.to_arrow()
        arrow_table = cast_arrow_table_to_meds_schema(arrow_table)
        if verbose:
            print("Arrow table schema before validation:", arrow_table.schema)
        if verbose:
            print("Validated table:", arrow_table)
        pq.write_table(arrow_table, os.path.join(data_dir, f"{shard_idx}.parquet"))
        if verbose:
            print(f"Shard {shard_idx} written successfully.")
    except Exception as e:
        print(f"Error writing shard {shard_idx}: {e}")
        traceback.print_exc()

def write_meds_sharded_parquet(events: List[Dict[str, Any]], output_dir: str, shard_size: int = 10000, max_workers: int = 4, verbose: bool = False):
    os.makedirs(output_dir, exist_ok=True)
    required_cols = list(DataSchema.schema().names)
    n = len(events)
    shards = [(events[i:i+shard_size], required_cols, output_dir, i//shard_size, verbose) for i in range(0, n, shard_size)]
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(lambda args: write_single_shard(*args), shards) 