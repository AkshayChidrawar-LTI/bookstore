from pyspark.sql.types import StructType,StructField,StringType,DoubleType,TimestampType, ArrayType,LongType,BinaryType

path_feeds = 's3://bookstorerawdata/feed_batch/'

tbl_bronze_feeds = 'bookstore.bronze.feeds'
tbl_bronze_books = 'bookstore.bronze.books'
tbl_bronze_customers = 'bookstore.bronze.customers'
tbl_bronze_orders = 'bookstore.bronze.orders'

topic_books = 'books'
topic_customers = 'customers'
topic_orders = 'orders'

schema_feeds = StructType([
    StructField("key", BinaryType())
    ,StructField("value", BinaryType())
    ,StructField("topic", StringType())
    ,StructField("partition", LongType())
    ,StructField("offset", LongType())
    ,StructField("timestamp", LongType())
])

schema_books = StructType([
    StructField("book_id", StringType())
    ,StructField("title", StringType())
    ,StructField("author", StringType())
    ,StructField("price", DoubleType())
    ,StructField("updated", TimestampType())
])

schema_customers = StructType([
    StructField("customer_id", StringType())
    ,StructField("email", StringType())
    ,StructField("first_name", StringType())
    ,StructField("last_name", StringType())
    ,StructField("gender", StringType())
    ,StructField("city", StringType())
    ,StructField("country_code", StringType())
    ,StructField("row_status", StringType())
    ,StructField("row_time", TimestampType())
])

schema_orders = StructType([
    StructField("order_id", StringType()),
    StructField("order_timestamp", TimestampType()),
    StructField("customer_id", StringType()),
    StructField("quantity", LongType()),
    StructField("total", LongType()),
    StructField("books", ArrayType(StructType([
        StructField("book_id", StringType()),
        StructField("quantity", LongType()),
        StructField("subtotal", LongType())
        ])))
])

