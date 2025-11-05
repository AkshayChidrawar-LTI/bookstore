#from pyspark import pipelines as dp

# @dp.view(name='bronze_books')
# def fn_read_bronze_books():
#     return spark.readStream.table('bookstore.bronze.books')

# # Declare the target streaming table (create if not already exists, reference it if already exists)
# dp.create_streaming_table(name='bookstore.silver.books_temp'
#                           ,table_properties={"delta.enableChangeDataFeed": "true"})

# # Define the Auto CDC flow
# dp.create_auto_cdc_flow(
#     source='bronze_books',
#     target='bookstore.silver.books_temp',
#     keys=['book_id'],
#     sequence_by='updated',
#     stored_as_scd_type=2,
#     name='scd2_books_query'
# )