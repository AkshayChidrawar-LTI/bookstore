
try:
    spark.sql(f"""CREATE DATABASE ct_bookstore.db_bronze""")
    logger.info(f"\nSuccess: CREATE DATABASE 'ct_bookstore.db_bronze'")
except Exception as e:
    logger.error(f"\nFailure: CREATE DATABASE 'ct_bookstore.db_bronze': \n{e}")
