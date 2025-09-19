
try:
    spark.sql(f"""CREATE DATABASE ct_bookstore.db_bronze
MANAGED LOCATION 's3://bookstoredatabricks/ct_bookstore/db_bronze';
""")
    print(f"\nSuccess: CREATE DATABASE 'ct_bookstore.db_bronze': \n's3://bookstoredatabricks/ct_bookstore/db_bronze'")
except Exception as e:
    print(f"\nFailure: CREATE DATABASE 'ct_bookstore.db_bronze': \n{e}")
