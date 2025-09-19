
try:
    spark.sql(f"""DROP TABLE ct_bookstore.db_silver.tbl_customers """)
    print(f"\nSuccess: DROP TABLE 'ct_bookstore.db_silver.tbl_customers'")
except Exception as e:
    print(f"\nFailure: DROP TABLE 'ct_bookstore.db_silver.tbl_customers': \n{e}")
