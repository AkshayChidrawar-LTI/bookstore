
try:
    spark.sql(f"""DROP CATALOG IF EXISTS ct_bookstore CASCADE""")
    print(f"\nSuccess: DROP CATALOG 'ct_bookstore'")
except Exception as e:
    print(f"\nFailure: DROP CATALOG 'ct_bookstore': \n{e}")
