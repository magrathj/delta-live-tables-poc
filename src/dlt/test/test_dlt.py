from dlt import *
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

def test_dlt_table(spark):
    """test @dlt.table() function"""

    data = [
        ("James","","Smith","36636","M",3000),
        ("Michael","Rose","","40288","M",4000),
        ("Robert","","Williams","42114","M",4000),
        ("Maria","Anne","Jones","39192","F",4000),
        ("Jen","Mary","Brown","","F",-1)
    ]

    schema = StructType([ \
        StructField("firstname",StringType(),True), \
        StructField("middlename",StringType(),True), \
        StructField("lastname",StringType(),True), \
        StructField("id", StringType(), True), \
        StructField("gender", StringType(), True), \
        StructField("salary", IntegerType(), True) \
    ])

    @dlt.table(
        comment="Test"
    )
    def read_table():
        return (
            spark.createDataFrame(data=data, schema=schema)
        )

    assert read_table().count() == 5
    assert spark.read.table("read_table").count() == 5

def test_dlt_view(spark):
    """test @dlt.view() function"""

    data = [
        ("James","","Smith","36636","M",3000),
        ("Michael","Rose","","40288","M",4000),
        ("Robert","","Williams","42114","M",4000),
        ("Maria","Anne","Jones","39192","F",4000),
        ("Jen","Mary","Brown","","F",-1)
    ]

    schema = StructType([ \
        StructField("firstname",StringType(),True), \
        StructField("middlename",StringType(),True), \
        StructField("lastname",StringType(),True), \
        StructField("id", StringType(), True), \
        StructField("gender", StringType(), True), \
        StructField("salary", IntegerType(), True) \
    ])

    @dlt.view(
        comment="Test"
    )
    def read_table():
        return (
            spark.createDataFrame(data=data, schema=schema)
        )

    assert read_table().count() == 5
    assert spark.read.table("read_table").count() == 5


def test_dlt_read(spark):
    """test dlt.read() function"""

    data = [
        ("James","","Smith","36636","M",3000),
        ("Michael","Rose","","40288","M",4000),
        ("Robert","","Williams","42114","M",4000),
        ("Maria","Anne","Jones","39192","F",4000),
        ("Jen","Mary","Brown","","F",-1)
    ]

    schema = StructType([ \
        StructField("firstname",StringType(),True), \
        StructField("middlename",StringType(),True), \
        StructField("lastname",StringType(),True), \
        StructField("id", StringType(), True), \
        StructField("gender", StringType(), True), \
        StructField("salary", IntegerType(), True) \
    ])

    # create temp view
    (
        spark
        .createDataFrame(data=data, schema=schema)
        .createOrReplaceTempView("test_dataframe")
    )

    df = dlt.read(
        "test_dataframe"
    )
    assert df.count() == 5

def test_dlt_expect(spark):
    """test @dlt.expect() function"""

    data = [
        ("James","","Smith","36636","M",3000),
        ("Michael","Rose","","40288","M",4000),
        ("Robert","","Williams","42114","M",4000),
        ("Maria","Anne","Jones","39192","F",4000),
        ("Jen","Mary","Brown","","F",-1)
    ]

    schema = StructType([ \
        StructField("firstname",StringType(),True), \
        StructField("middlename",StringType(),True), \
        StructField("lastname",StringType(),True), \
        StructField("id", StringType(), True), \
        StructField("gender", StringType(), True), \
        StructField("salary", IntegerType(), True) \
    ])

    @dlt.expect(
        "test",
        "test"
    )
    @dlt.table(
        comment="Test"
    )
    def read_table():
        return (
            spark.createDataFrame(data=data, schema=schema)
        )

    assert read_table().count() == 5
    assert spark.read.table("read_table").count() == 5
