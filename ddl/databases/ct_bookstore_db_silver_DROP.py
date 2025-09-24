
try:
    spark.sql(f"""DROP DATABASE IF EXISTS ct_bookstore.db_silver CASCADE""")
    print(f"\nSuccess: DROP DATABASE 'ct_bookstore.db_silver'")
except Exception as e:
    print(f"\nFailure: DROP DATABASE 'ct_bookstore.db_silver': \n{e}")
