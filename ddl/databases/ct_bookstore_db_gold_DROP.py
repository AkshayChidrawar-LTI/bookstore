
try:
    spark.sql(f"""DROP DATABASE ct_bookstore.db_gold CASCADE""")
    logger.info(f"\nSuccess: DROP DATABASE 'ct_bookstore.db_gold'")
except Exception as e:
    logger.error(f"\nFailure: DROP DATABASE 'ct_bookstore.db_gold': \n{e}")
