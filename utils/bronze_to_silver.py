from pyspark import pipelines as dp

from utils.functions import raw_feed,topics_feed

from utils.inputs import (
    tbl_bronze_books,
    tbl_bronze_customers,
    tbl_silver_books,
    tbl_silver_customers,
    tbl_silver_orders
    )

#----------------------------------------------------------------------------------------------
dp.create_streaming_table(name=tbl_silver_books,table_properties={"delta.enableChangeDataFeed": "true"})

@dp.view(name='vw_books')
def fn_read_books():
    return spark.readStream.table(tbl_bronze_books)

dp.create_auto_cdc_flow(
    source='vw_books'
    ,target=tbl_silver_books
    ,keys=['book_id']
    ,sequence_by='updated'
    ,stored_as_scd_type=2
    ,name='scd2_books_query'
)
#----------------------------------------------------------------------------------------------
dp.create_streaming_table(name=tbl_silver_customers,table_properties={"delta.enableChangeDataFeed": "true"})

@dp.view(name='vw_customers')
def fn_get_customers():
    return spark.readStream.table(tbl_bronze_customers)

dp.create_auto_cdc_flow(
    source='vw_customers'
    ,target=tbl_silver_customers
    ,keys=['customer_id']
    ,sequence_by='row_time'
    ,stored_as_scd_type=1
    ,name='scd1_customers_query'
)
#----------------------------------------------------------------------------------------------