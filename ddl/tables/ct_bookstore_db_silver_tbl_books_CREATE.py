
try:
    spark.sql(f"""CREATE TABLE ct_bookstore.db_silver.tbl_books
(
book_id string not null,
title string ,
author string ,
price double ,
isactive boolean not null,
start_date timestamp not null,
end_date timestamp 
)
LOCATION 's3://bookstoredatabricks/ct_bookstore/db_silver/tbl_books';
""")
    print(f"\nSuccess: CREATE TABLE 'ct_bookstore.db_silver.tbl_books': \n's3://bookstoredatabricks/ct_bookstore/db_silver/tbl_books'")
except Exception as e:
    print(f"\nFailure: CREATE TABLE 'ct_bookstore.db_silver.tbl_books': \n{e}")
