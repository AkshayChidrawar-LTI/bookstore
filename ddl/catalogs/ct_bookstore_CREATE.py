
try:
    spark.sql(f"""CREATE CATALOG ct_bookstore""")
    logger.info(f"\nSuccess: CREATE CATALOG 'ct_bookstore'")
except Exception as e:
    logger.error(f"\nFailure: CREATE CATALOG 'ct_bookstore': \n{e}")
