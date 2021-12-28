from dlt import dlt
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

def test_dlt_table(spark):
    """test """

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

