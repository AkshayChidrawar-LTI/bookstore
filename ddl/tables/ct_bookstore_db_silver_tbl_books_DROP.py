
try:
    spark.sql(f"""DROP TABLE ct_bookstore.db_silver.tbl_books """)
    logger.info(f"\nSuccess: DROP TABLE 'ct_bookstore.db_silver.tbl_books'")
except Exception as e:
    logger.error(f"\nFailure: DROP TABLE 'ct_bookstore.db_silver.tbl_books': \n{e}")
