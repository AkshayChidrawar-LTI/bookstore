
try:
    spark.sql(f"""CREATE DATABASE ct_bookstore.db_gold""")
    logger.info(f"\nSuccess: CREATE DATABASE 'ct_bookstore.db_gold'")
except Exception as e:
    logger.error(f"\nFailure: CREATE DATABASE 'ct_bookstore.db_gold': \n{e}")
