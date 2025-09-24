
try:
    spark.sql(f"""CREATE DATABASE ct_bookstore.db_silver""")
    print(f"\nSuccess: CREATE DATABASE 'ct_bookstore.db_silver': \n''")
except Exception as e:
    print(f"\nFailure: CREATE DATABASE 'ct_bookstore.db_silver': \n{e}")
