
try:
    spark.sql(f"""CREATE TABLE ct_bookstore.db_silver.tbl_orders
(
order_id string not null,
order_timestamp timestamp ,
customer_id string not null,
quantity bigint not null,
total bigint not null,
books array <struct<
	book_id STRING ,
	quantity bigint ,
	subtotal bigint >>
)
LOCATION 's3://bookstoredatabricks/ct_bookstore/db_silver/tbl_orders';
""")
    print(f"\nSuccess: CREATE TABLE 'ct_bookstore.db_silver.tbl_orders': \n's3://bookstoredatabricks/ct_bookstore/db_silver/tbl_orders'")
except Exception as e:
    print(f"\nFailure: CREATE TABLE 'ct_bookstore.db_silver.tbl_orders': \n{e}")
