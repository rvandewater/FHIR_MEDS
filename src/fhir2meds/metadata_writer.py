import datetime
import json
import os

import pyarrow as pa
import pyarrow.parquet as pq
from meds import CodeMetadataSchema, DatasetMetadataSchema, SubjectSplitSchema


def write_dataset_metadata(
    output_dir,
    dataset_name=None,
    dataset_version=None,
    etl_name=None,
    etl_version=None,
    meds_version=None,
    license=None,
    location_uri=None,
    description_uri=None,
    raw_source_id_columns=None,
    code_modifier_columns=None,
    additional_value_modality_columns=None,
    site_id_columns=None,
    other_extension_columns=None,
):
    """
    Write dataset metadata to metadata/dataset.json in the output directory.
    Fields match DatasetMetadataSchema; unset fields are written as JSON null (not the string "None").
    """
    metadata = {
        "dataset_name": dataset_name,
        "dataset_version": dataset_version,
        "etl_name": etl_name,
        "etl_version": etl_version,
        "meds_version": meds_version,
        "created_at": datetime.datetime.now().isoformat(),
        "license": license,
        "location_uri": None if location_uri is None else str(location_uri),
        "description_uri": description_uri,
        "raw_source_id_columns": raw_source_id_columns,
        "code_modifier_columns": code_modifier_columns,
        "additional_value_modality_columns": additional_value_modality_columns,
        "site_id_columns": site_id_columns,
        "other_extension_columns": other_extension_columns,
    }
    metadata = {k: v for k, v in metadata.items() if v is not None}
    DatasetMetadataSchema.validate(metadata)

    output_dir = str(output_dir)
    os.makedirs(os.path.join(output_dir, "metadata"), exist_ok=True)
    with open(os.path.join(output_dir, "metadata", "dataset.json"), "w") as f:
        json.dump(metadata, f, indent=2)


def write_codes_metadata(output_dir, events, descriptions=None):
    """
    Write code metadata to metadata/codes.parquet in the output directory.
    Matches CodeMetadataSchema: code, description, parent_codes.
    """
    descriptions = descriptions or {}
    codes = sorted({e["code"] for e in events if e.get("code")})
    data = {
        "code": codes,
        "description": [descriptions.get(c) for c in codes],
        "parent_codes": [[] for _ in codes],
    }
    table = pa.table(data, schema=CodeMetadataSchema.schema())
    os.makedirs(os.path.join(output_dir, "metadata"), exist_ok=True)
    pq.write_table(table, os.path.join(output_dir, "metadata", "codes.parquet"))


def write_subject_splits(output_dir, events, split_name="train"):
    """
    Write subject splits to metadata/subject_splits.parquet in the output directory.
    Columns: subject_id, split. All assigned to split_name by default.
    """
    subject_ids = sorted({e["subject_id"] for e in events if e.get("subject_id") is not None})
    data = {
        "subject_id": subject_ids,
        "split": [split_name] * len(subject_ids),
    }
    table = pa.table(data, schema=SubjectSplitSchema.schema())
    os.makedirs(os.path.join(output_dir, "metadata"), exist_ok=True)
    pq.write_table(table, os.path.join(output_dir, "metadata", "subject_splits.parquet"))
