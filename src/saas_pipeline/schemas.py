"""Explicit source schemas keep ingestion deterministic."""

from pyspark.sql.types import StringType, StructField, StructType

DELIVERIES_RAW_SCHEMA = StructType(
    [
        StructField("pais", StringType(), True),
        StructField("fecha_proceso", StringType(), True),
        StructField("transporte", StringType(), True),
        StructField("ruta", StringType(), True),
        StructField("tipo_entrega", StringType(), True),
        StructField("material", StringType(), True),
        StructField("precio", StringType(), True),
        StructField("cantidad", StringType(), True),
        StructField("unidad", StringType(), True),
    ]
)

MATERIALS_RAW_SCHEMA = StructType(
    [
        StructField("material", StringType(), False),
        StructField("descripcion", StringType(), True),
        StructField("categoria", StringType(), True),
        StructField("precio_base", StringType(), True),
        StructField("valid_from", StringType(), True),
        StructField("valid_to", StringType(), True),
        StructField("is_current", StringType(), True),
    ]
)

DELIVERY_SOURCE_COLUMNS = [field.name for field in DELIVERIES_RAW_SCHEMA.fields]
