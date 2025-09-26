
try:
    spark.sql(f"""DROP DATABASE ct_bookstore.db_bronze CASCADE""")
    logger.info(f"\nSuccess: DROP DATABASE 'ct_bookstore.db_bronze'")
except Exception as e:
    logger.error(f"\nFailure: DROP DATABASE 'ct_bookstore.db_bronze': \n{e}")
