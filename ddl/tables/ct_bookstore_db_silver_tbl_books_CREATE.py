
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
);
""")
    logger.info(f"\nSuccess: CREATE TABLE 'ct_bookstore.db_silver.tbl_books'")
except Exception as e:
    logger.error(f"\nFailure: CREATE TABLE 'ct_bookstore.db_silver.tbl_books': \n{e}")
