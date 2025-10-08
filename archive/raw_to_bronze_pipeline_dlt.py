import dlt

from raw_to_bronze_input import (
    path_feed,
    schema_feed,
    tbl_bronze_books,
    tbl_bronze_customers,
    tbl_bronze_orders,
    topic_books,
    topic_customers,
    topic_orders,
    schema_books,
    schema_customers,
    schema_orders
    )

from raw_to_bronze_function import raw_feed, topics_feed

@dlt.view
def feed():
    return raw_feed(path_=path_feed,schema_=schema_feed)

@dlt.table(name=tbl_bronze_books)
def books():
    return topics_feed(feed=feed(),name_=topic_books,schema_=schema_books)

@dlt.table(name=tbl_bronze_customers)
def customers():
    return topics_feed(feed=feed(),name_=topic_customers,schema_=schema_customers)

@dlt.table(name=tbl_bronze_orders)
def orders():
    return topics_feed(feed=feed(),name_=topic_orders,schema_=schema_orders)