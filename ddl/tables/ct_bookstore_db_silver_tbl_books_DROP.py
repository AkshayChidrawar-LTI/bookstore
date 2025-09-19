
try:
    spark.sql(f"""DROP TABLE ct_bookstore.db_silver.tbl_books """)
    print(f"\nSuccess: DROP TABLE 'ct_bookstore.db_silver.tbl_books'")
except Exception as e:
    print(f"\nFailure: DROP TABLE 'ct_bookstore.db_silver.tbl_books': \n{e}")
