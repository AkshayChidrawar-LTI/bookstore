from pyspark import pipelines as dp

from raw_to_bronze_input import (
    path_feeds,
    schema_feeds,
    tbl_bronze_feeds
    )

from raw_to_bronze_function import raw_feed, topics_feed

@dp.table(name=tbl_bronze_feeds
          ,table_properties={"delta.enableChangeDataFeed": "true"})
def fn_feed():
    return raw_feed(spark=spark,path_=path_feeds,schema_=schema_feeds)

"""
from raw_to_bronze_input import (
    path_feed,
    schema_feed,
    tbl_bronze_books,
    tbl_bronze_customers,
    tbl_bronze_orders,
    topic_books,
    topic_customers,
    topic_orders,
    schema_books,
    schema_customers,
    schema_orders
    )


@dp.view(name='feed')
def fn_feed():
    return raw_feed(spark=spark,path_=path_feed,schema_=schema_feed)

@dp.table(name=tbl_bronze_books
          ,table_properties={"delta.enableChangeDataFeed": "true"})
def fn_books():
    return topics_feed(feed=spark.readStream.table('feed'),name_=topic_books,schema_=schema_books)

@dp.table(name=tbl_bronze_customers
          ,table_properties={"delta.enableChangeDataFeed": "true"})
def fn_customers():
    return topics_feed(feed=spark.readStream.table('feed'),name_=topic_customers,schema_=schema_customers)

@dp.table(name=tbl_bronze_orders
          ,table_properties={"delta.enableChangeDataFeed": "true"})
def fn_orders():
    return topics_feed(feed=spark.readStream.table('feed'),name_=topic_orders,schema_=schema_orders)

"""