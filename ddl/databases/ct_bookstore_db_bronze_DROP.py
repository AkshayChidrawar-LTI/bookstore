
try:
    spark.sql(f"""DROP DATABASE IF EXISTS ct_bookstore.db_bronze CASCADE""")
    print(f"\nSuccess: DROP DATABASE 'ct_bookstore.db_bronze'")
except Exception as e:
    print(f"\nFailure: DROP DATABASE 'ct_bookstore.db_bronze': \n{e}")
