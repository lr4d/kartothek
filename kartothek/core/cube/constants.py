"""
Common constants for klee.
"""
from kartothek.serialization import ParquetSerializer

__all__ = (
    "KLEE_DF_SERIALIZER",
    "KLEE_METADATA_DIMENSION_COLUMNS",
    "KLEE_METADATA_KEY_IS_SEED",
    "KLEE_METADATA_STORAGE_FORMAT",
    "KLEE_METADATA_VERSION",
    "KLEE_UUID_SEPERATOR",
)


#
# !!!! WARNING !!!
#
# If you change any of these constants, this may break backwards compatibility.
# Also, always ensure to also adapt the docs (especially the format specification in the README).
#
# !!!!!!!!!!!!!!!!
#


#: DataFrame serializer that is be used to write data.
KLEE_DF_SERIALIZER = ParquetSerializer(compression="ZSTD")

#: Storage format for kartothek metadata that is be used by default.
KLEE_METADATA_STORAGE_FORMAT = "json"

#: Kartothek metadata version that klee is based on.
KLEE_METADATA_VERSION = 4

#: Metadata key that is used to mark seed datasets
KLEE_METADATA_KEY_IS_SEED = "klee_is_seed"

#: Metadata key to store dimension columns
KLEE_METADATA_DIMENSION_COLUMNS = "klee_dimension_columns"

#: Metadata key to store partition columns
KLEE_METADATA_PARTITION_COLUMNS = "klee_partition_columns"

#: Metadata key to store the timestamp column (can be null)
KLEE_METADATA_TIMESTAMP_COLUMN = "klee_timestamp_column"

#: Character sequence used to seperate cube and dataset UUID
KLEE_UUID_SEPERATOR = "++"
