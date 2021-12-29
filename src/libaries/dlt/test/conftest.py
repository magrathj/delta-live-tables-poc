import pytest
import pyspark

@pytest.fixture(scope="session")
def spark():
    builder = (
        pyspark.sql.SparkSession.builder
        .master("local[1]")
        .appName("delta_migrations")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
    )
    spark = spark = builder.getOrCreate()
    yield spark