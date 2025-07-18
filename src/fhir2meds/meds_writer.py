import os
import pyarrow.parquet as pq
from typing import List, Dict, Any
import polars as pl
from meds import DataSchema
from concurrent.futures import ThreadPoolExecutor
import pyarrow as pa

def write_single_shard(shard, required_cols, output_dir, shard_idx):
    try:
        print(f"Writing shard {shard_idx} with {len(shard)} events to {output_dir}")
        pl_df = pl.DataFrame(shard)
        for col in required_cols:
            if col not in pl_df.columns:
                pl_df = pl_df.with_columns(pl.lit(None).alias(col))
        pl_df = pl_df.select(required_cols)
        pl_df = cast_to_meds_schema(pl_df)
        # Debug: print subject_id values
        print("subject_id values before filtering:", pl_df["subject_id"])
        # Filter out rows with null subject_id
        null_rows = pl_df.filter(pl.col("subject_id").is_null())
        if null_rows.height > 0:
            print("Rows with null subject_id:", null_rows)
        pl_df = pl_df.filter(pl.col("subject_id").is_not_null())
        arrow_table = pl_df.to_arrow()
        arrow_table = cast_arrow_code_to_string(arrow_table)
        print("Arrow table schema before validation:", arrow_table.schema)
        DataSchema.validate(arrow_table)
        table = arrow_table
        print("Validated table:", table)
        pq.write_table(table, os.path.join(output_dir, f"{shard_idx}.parquet"))
        print(f"Shard {shard_idx} written successfully.")
    except Exception as e:
        print(f"Error writing shard {shard_idx}: {e}")

def write_meds_sharded_parquet(events: List[Dict[str, Any]], output_dir: str, shard_size: int = 10000, max_workers: int = 4):
    os.makedirs(output_dir, exist_ok=True)
    required_cols = list(DataSchema.schema().names)
    n = len(events)
    shards = [(events[i:i+shard_size], required_cols, output_dir, i//shard_size) for i in range(0, n, shard_size)]
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(lambda args: write_single_shard(*args), shards) 

def cast_to_meds_schema(pl_df):
    # subject_id: Int64
    if "subject_id" in pl_df.columns:
        pl_df = pl_df.with_columns(pl.col("subject_id").cast(pl.Int64, strict=False))
    # time: Datetime (microseconds, no timezone)
    if "time" in pl_df.columns:
        # If time is string, parse to datetime; if already datetime, cast to microseconds
        if pl_df["time"].dtype == pl.Utf8:
            pl_df = pl_df.with_columns(
                pl.col("time")
                .str.replace("Z$", "")  # Remove trailing Z if present
                .str.strptime(pl.Datetime, fmt="%Y-%m-%dT%H:%M:%S%.f", strict=False)
                .cast(pl.Datetime("us"), strict=False)
            )
        elif pl_df["time"].dtype == pl.Datetime:
            pl_df = pl_df.with_columns(pl.col("time").cast(pl.Datetime("us"), strict=False))
    # code: Utf8 (string)
    if "code" in pl_df.columns:
        pl_df = pl_df.with_columns(pl.col("code").cast(pl.Utf8, strict=False))
    # numeric_value: Float32
    if "numeric_value" in pl_df.columns:
        pl_df = pl_df.with_columns(pl.col("numeric_value").cast(pl.Float32, strict=False))
    # text_value: Utf8 (string)
    if "text_value" in pl_df.columns:
        pl_df = pl_df.with_columns(pl.col("text_value").cast(pl.Utf8, strict=False))
    return pl_df 

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
    import os, json
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