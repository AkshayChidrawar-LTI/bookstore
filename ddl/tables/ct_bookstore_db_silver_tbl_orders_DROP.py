
try:
    spark.sql(f"""DROP TABLE ct_bookstore.db_silver.tbl_orders """)
    logger.info(f"\nSuccess: DROP TABLE 'ct_bookstore.db_silver.tbl_orders'")
except Exception as e:
    logger.error(f"\nFailure: DROP TABLE 'ct_bookstore.db_silver.tbl_orders': \n{e}")
