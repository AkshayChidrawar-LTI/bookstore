
try:
    spark.sql(f"""CREATE CATALOG ct_bookstore
MANAGED LOCATION 's3://bookstoredatabricks/ct_bookstore';
""")
    print(f"\nSuccess: CREATE CATALOG 'ct_bookstore': \n's3://bookstoredatabricks/ct_bookstore'")
except Exception as e:
    print(f"\nFailure: CREATE CATALOG 'ct_bookstore': \n{e}")
