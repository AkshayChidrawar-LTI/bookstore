
try:
    spark.sql(f"""DROP TABLE ct_bookstore.db_silver.tbl_orders """)
    print(f"\nSuccess: DROP TABLE 'ct_bookstore.db_silver.tbl_orders'")
except Exception as e:
    print(f"\nFailure: DROP TABLE 'ct_bookstore.db_silver.tbl_orders': \n{e}")
