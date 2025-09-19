
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
)
LOCATION 's3://bookstoredatabricks/ct_bookstore/db_silver/tbl_customers';
""")
    print(f"\nSuccess: CREATE TABLE 'ct_bookstore.db_silver.tbl_customers': \n's3://bookstoredatabricks/ct_bookstore/db_silver/tbl_customers'")
except Exception as e:
    print(f"\nFailure: CREATE TABLE 'ct_bookstore.db_silver.tbl_customers': \n{e}")
