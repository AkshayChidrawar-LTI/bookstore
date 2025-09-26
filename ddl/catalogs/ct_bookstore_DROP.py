
try:
    spark.sql(f"""DROP CATALOG ct_bookstore CASCADE""")
    logger.info(f"\nSuccess: DROP CATALOG 'ct_bookstore'")
except Exception as e:
    logger.error(f"\nFailure: DROP CATALOG 'ct_bookstore': \n{e}")
