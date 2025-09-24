
try:
    spark.sql(f"""CREATE CATALOG ct_bookstore""")
    print(f"\nSuccess: CREATE CATALOG 'ct_bookstore': \n''")
except Exception as e:
    print(f"\nFailure: CREATE CATALOG 'ct_bookstore': \n{e}")
