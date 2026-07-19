from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType, TimestampType
)

ec_schema = StructType([
    StructField("event_time", TimestampType(), nullable=True),
    StructField("event_type", StringType(), nullable=True),
    StructField("product_id", IntegerType(), nullable=True),
    StructField("category_id", LongType(), nullable=True),
    StructField("category_code", StringType(), nullable=True),
    StructField("brand", StringType(), nullable=True),
    StructField("price", DoubleType(), nullable=True),
    StructField("user_id", IntegerType(), nullable=True),
    StructField("user_session", StringType(), nullable=True),
])