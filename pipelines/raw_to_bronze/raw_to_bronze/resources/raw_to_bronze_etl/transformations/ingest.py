from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, ArrayType, LongType
from pyspark import pipelines as dp

books_schema = StructType([
    StructField("book_id", StringType()),
    StructField("title", StringType()),
    StructField("author", StringType()),
    StructField("price", DoubleType()),
    StructField("updated", TimestampType())
])

customers_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("email", StringType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("gender", StringType()),
    StructField("city", StringType()),
    StructField("country_code", StringType()),
    StructField("row_status", StringType()),
    StructField("row_time", TimestampType())
])

orders_schema = StructType([
    StructField("order_id", StringType()),
    StructField("order_timestamp", TimestampType()),
    StructField("customer_id", StringType()),
    StructField("quantity", LongType()),
    StructField("total", LongType()),
    StructField("email", StringType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("gender", StringType()),
    StructField("street", StringType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("row_time", TimestampType()),
    StructField("processed_timestamp", TimestampType()),
    StructField("books", ArrayType(StructType([
        StructField("book_id", StringType()),
        StructField("quantity", LongType()),
        StructField("subtotal", LongType())
        ])))
])

@dp.view()
def raw_feed():
    return (
        spark.readStream
        .format('cloudFiles')
        .option('cloudFiles.format', 'json')
        .load('s3://bookstorerawdata/feed_batch/')
        .withColumn('key',F.decode(F.unbase64(F.col('key')),'utf-8'))
        .withColumn('value',F.decode(F.unbase64(F.col('value')),'utf-8'))
        .select(
            F.col('topic'),
            F.col('key').cast('string'),
            F.col('value').cast('string'),
            (F.col('timestamp') / 1000).cast('timestamp').alias('create_ts'),
            F.col('_metadata.file_path').alias('source_file'),
            F.from_utc_timestamp(F.current_timestamp(), 'Asia/Kolkata').alias('insert_ts')
        )
    )

@dp.table(name='bookstore.bronze.books')
def books():
    return (
        spark.readStream.table('raw_feed')
        .filter(F.col('topic') == 'books')
        .withColumn('v', F.from_json(F.col('value'), books_schema))
        .select('key', 'v.*', 'create_ts', 'source_file', 'insert_ts')
    )

@dp.table(name='bookstore.bronze.customers')
def customers():
    return (
        spark.readStream.table('raw_feed')
        .filter(F.col('topic') == 'customers')
        .withColumn('v', F.from_json(F.col('value'), customers_schema))
        .select('key', 'v.*', 'create_ts', 'source_file', 'insert_ts')
    )

@dp.table(name='bookstore.bronze.orders')
def orders():
    return (
        spark.readStream.table('raw_feed')
        .filter(F.col('topic') == 'orders')
        .withColumn('v', F.from_json(F.col('value'), orders_schema))
        .select('key', 'v.*', 'create_ts', 'source_file', 'insert_ts')
    )