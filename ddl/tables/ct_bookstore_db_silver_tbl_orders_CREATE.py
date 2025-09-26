
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
);
""")
    logger.info(f"\nSuccess: CREATE TABLE 'ct_bookstore.db_silver.tbl_orders'")
except Exception as e:
    logger.error(f"\nFailure: CREATE TABLE 'ct_bookstore.db_silver.tbl_orders': \n{e}")
