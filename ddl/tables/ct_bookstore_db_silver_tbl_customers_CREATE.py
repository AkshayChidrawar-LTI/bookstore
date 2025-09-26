
try:
    spark.sql(f"""CREATE TABLE ct_bookstore.db_silver.tbl_customers
(
customer_id string not null,
email string ,
first_name string ,
last_name string ,
gender string ,
city string ,
country string ,
last_updated timestamp 
);
""")
    logger.info(f"\nSuccess: CREATE TABLE 'ct_bookstore.db_silver.tbl_customers'")
except Exception as e:
    logger.error(f"\nFailure: CREATE TABLE 'ct_bookstore.db_silver.tbl_customers': \n{e}")
