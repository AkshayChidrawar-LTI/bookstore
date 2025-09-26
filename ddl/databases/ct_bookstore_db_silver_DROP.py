
try:
    spark.sql(f"""DROP DATABASE ct_bookstore.db_silver CASCADE""")
    logger.info(f"\nSuccess: DROP DATABASE 'ct_bookstore.db_silver'")
except Exception as e:
    logger.error(f"\nFailure: DROP DATABASE 'ct_bookstore.db_silver': \n{e}")
