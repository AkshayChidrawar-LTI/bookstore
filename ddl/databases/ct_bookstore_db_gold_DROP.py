
try:
    spark.sql(f"""DROP DATABASE ct_bookstore.db_gold CASCADE""")
    print(f"\nSuccess: DROP DATABASE 'ct_bookstore.db_gold'")
except Exception as e:
    print(f"\nFailure: DROP DATABASE 'ct_bookstore.db_gold': \n{e}")
