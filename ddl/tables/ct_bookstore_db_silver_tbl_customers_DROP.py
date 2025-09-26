
try:
    spark.sql(f"""DROP TABLE ct_bookstore.db_silver.tbl_customers """)
    logger.info(f"\nSuccess: DROP TABLE 'ct_bookstore.db_silver.tbl_customers'")
except Exception as e:
    logger.error(f"\nFailure: DROP TABLE 'ct_bookstore.db_silver.tbl_customers': \n{e}")
