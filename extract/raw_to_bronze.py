from pyspark import pipelines as dp

from utils.functions import raw_feed,topics_feed

from utils.inputs import (
    path_feeds,
    schema_feeds,
    schema_books,
    schema_customers,
    schema_orders,
    tbl_bronze_feeds,
    tbl_bronze_books,
    tbl_bronze_customers,
    tbl_bronze_orders,
    topic_books,
    topic_customers,
    topic_orders
    )

#----------------------------------------------------------------------------------------------
@dp.view(name='vw_feed')
def fn_get_feed():
    return raw_feed(spark=spark
                    ,path_=path_feeds
                    ,schema_=schema_feeds)

@dp.table(name=tbl_bronze_feeds,table_properties={"delta.enableChangeDataFeed": "true"})
def fn_write_feed():
    return spark.readStream.table('vw_feed')
#----------------------------------------------------------------------------------------------
@dp.table(name=tbl_bronze_books
          ,table_properties={"delta.enableChangeDataFeed": "true"})
def fn_write_books():
    return topics_feed(feed=spark.readStream.table('vw_feed')
                       ,name_=topic_books
                       ,schema_=schema_books)

@dp.table(name=tbl_bronze_customers
          ,table_properties={"delta.enableChangeDataFeed": "true"})
def fn_write_customers():
    return topics_feed(feed=spark.readStream.table('vw_feed')
                       ,name_=topic_customers
                       ,schema_=schema_customers)

@dp.table(name=tbl_bronze_orders
          ,table_properties={"delta.enableChangeDataFeed": "true"})
def fn_write_orders():
    return topics_feed(feed=spark.readStream.table('vw_feed')
                       ,name_=topic_orders
                       ,schema_=schema_orders)
#----------------------------------------------------------------------------------------------
# dp.create_streaming_table(name=tbl_bronze_books,table_properties={"delta.enableChangeDataFeed": "true"})

# @dp.view(name='vw_books')
# def fn_read_books():
#     return topics_feed(feed=spark.readStream.table('vw_feed')
#                        ,name_=topic_books
#                        ,schema_=schema_books)

# dp.create_auto_cdc_flow(
#     source='vw_books'
#     ,target=tbl_bronze_books
#     ,keys=['book_id']
#     ,sequence_by='updated'
#     ,stored_as_scd_type=2
#     ,name='scd2_books_query'
# )
# #----------------------------------------------------------------------------------------------
# dp.create_streaming_table(name=tbl_bronze_customers,table_properties={"delta.enableChangeDataFeed": "true"})

# @dp.view(name='vw_customers')
# def fn_get_customers():
#     return topics_feed(feed=spark.readStream.table('vw_feed')
#                        ,name_=topic_customers
#                        ,schema_=schema_customers)

# dp.create_auto_cdc_flow(
#     source='vw_customers'
#     ,target=tbl_bronze_customers
#     ,keys=['customer_id']
#     ,sequence_by='row_time'
#     ,stored_as_scd_type=1
#     ,name='scd1_customers_query'
# )
#----------------------------------------------------------------------------------------------