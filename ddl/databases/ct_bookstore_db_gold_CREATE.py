
try:
    spark.sql(f"""CREATE DATABASE ct_bookstore.db_gold""")
    print(f"\nSuccess: CREATE DATABASE 'ct_bookstore.db_gold': \n''")
except Exception as e:
    print(f"\nFailure: CREATE DATABASE 'ct_bookstore.db_gold': \n{e}")
