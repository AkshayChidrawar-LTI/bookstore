
try:
    spark.sql(f"""CREATE DATABASE ct_bookstore.db_gold
MANAGED LOCATION 's3://bookstoredatabricks/ct_bookstore/db_gold';
""")
    print(f"\nSuccess: CREATE DATABASE 'ct_bookstore.db_gold': \n's3://bookstoredatabricks/ct_bookstore/db_gold'")
except Exception as e:
    print(f"\nFailure: CREATE DATABASE 'ct_bookstore.db_gold': \n{e}")
