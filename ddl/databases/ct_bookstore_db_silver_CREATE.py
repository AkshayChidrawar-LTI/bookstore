
try:
    spark.sql(f"""CREATE DATABASE ct_bookstore.db_silver""")
    logger.info(f"\nSuccess: CREATE DATABASE 'ct_bookstore.db_silver'")
except Exception as e:
    logger.error(f"\nFailure: CREATE DATABASE 'ct_bookstore.db_silver': \n{e}")
