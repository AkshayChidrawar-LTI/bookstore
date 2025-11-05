
CREATE TABLE IF NOT EXISTS workspace.default.bronze_to_silver_last_processed
(topic STRING,last_processed_ts TIMESTAMP);
INSERT INTO workspace.default.bronze_to_silver_last_processed VALUES 
('books',from_utc_timestamp(current_timestamp(), 'Asia/Kolkata'))
,('customers',from_utc_timestamp(current_timestamp(), 'Asia/Kolkata'))
,('orders',from_utc_timestamp(current_timestamp(), 'Asia/Kolkata'));

def parse_value_to_row(value,schema):
  df = spark.createDataFrame([{"value":json.dumps(value)}])
  parsed_df = df\
  .select(F.from_json(F.col("value"),schema).alias("parsed"))\
  .select("parsed.*")
  return parsed_df.collect()[0]

  topic_map = {
    'books': ('bookstore.silver.books',schema_books)
    ,'customers': ('bookstore.silver.customers',schema_customers)
    ,'orders': ('bookstore.silver.orders',schema_orders)
  }

  for row in feed.collect():
      topic = row['topic']
    
      if topic in topic_map:
        value = row['value']
        table,schema = topic_map[topic]
        parsed = parse_value_to_row(value,schema)
        row_dict = row.asDict()
        row_dict.pop('topic',None)
        row_dict.pop('value',None)
        merged_dict = {**parsed_row.asDict(),row_dict}
        row_updated = Row(**merged_dict)        
        row_updated.write.format('delta').mode('append').saveAsTable(table)

      else:
          quarantine = spark.createDataFrame([row.asDict()])
          quarantine
          .write
          .format('delta')
          .mode('append')
          .saveAsTable('bookstore.bronze.quarantines')

def process_feed():
    feed = spark.sql(f"""
                    select *
                    from bookstore.bronze.feeds
                    where insert_ts > (
                          select max(insert_ts)
                          from bookstore.silver.books
                        )
                    """)
  
    insert_ts = F.from_utc_timestamp(F.current_timestamp(),'Asia/Kolkata')

F.date_format(F.from_utc_timestamp(F.current_timestamp(),'Asia/Kolkata'),
    'yyyy-MM-dd HH:mm:ss')

datetime.now(pytz.timezone('Asia/Kolkata'))

value={'order_id':'000000003996','order_timestamp':'2021-12-29 14:03:00','customer_id':'C00159','quantity':2,'total':73,'books':[{'book_id':'B09','quantity':1,'subtotal':24},{'book_id':'B01','quantity':1,'subtotal':49}]}
value_str = json.dumps(value)
display((value_str))

schema= StructType([
    StructField('order_id', StringType()),
    StructField('order_timestamp', TimestampType()),
    StructField('customer_id', StringType()),
    StructField('quantity', LongType()),
    StructField('total', LongType()),
    StructField('books', ArrayType(StructType([
        StructField('book_id', StringType()),
        StructField('quantity', LongType()),
        StructField('subtotal', LongType())
        ])))
])


df = spark.createDataFrame(
    [{"value": value_str}]
)

parsed = df.select(
    F.from_json(F.col("value"),schema).alias("parsed")
).select("parsed.*")

display(parsed_df)

import sys
sys.path.append('/Workspace/Users/akshay.chidrawar@ltimindtree.com/bookstore/transformations')
from raw_to_bronze_input import schema_feeds,schema_books,schema_customers,schema_orders

def drop_columns_from_schema(schema, columns_to_drop):
    new_fields = [
        field for field in schema.fields
        if field.name not in columns_to_drop
    ]
    return StructType(new_fields)

topic_map = {
        'books': ('bookstore.silver.books',schema_books),
        'customers': ('bookstore.silver.customers',schema_customers),
        'orders': ('bookstore.silver.orders',schema_orders)
    }

if 1==1:
    feed = spark.sql(f"""
                    select *
                    from bookstore.bronze.feeds""").limit(10)
    for row in feed.collect():
        row_dict = row.asDict()
        topic = row_dict['topic']
        if topic in topic_map:
            table,schema = topic_map[topic]
            value = row_dict['value']
            parsed = parse_value_to_row(value,schema)
            row_dict.pop('topic',None)
            row_dict.pop('value',None)
            schema = drop_columns_from_schema(schema,['create_ts','value'])
            merged_dict = {**parsed,**row_dict}
            df = spark.createDataFrame([merged_dict],schema)
            display(df)

def parse_value_to_row(value,schema):
    parsed_df = spark.createDataFrame([{"value":json.dumps(value)}])\
        .withColumn("parsed",F.from_json(F.col("value"),schema))\
        .select("parsed.*")
    return parsed_df.collect()[0].asDict()

def process_feed():
    feed = spark.sql(f"""
                     select *
                     from bookstore.bronze.feeds
                     where insert_ts > (
                         select max(insert_ts)
                         from bookstore.silver.books
                         )
                         """)

    insert_ts = F.from_utc_timestamp(F.current_timestamp(),'Asia/Kolkata')

    topic_map = {
        'books': ('bookstore.silver.books', schema_books),
        'customers': ('bookstore.silver.customers', schema_customers),
        'orders': ('bookstore.silver.orders', schema_orders)
    }

    for row in feed.collect():
        row_dict = row.asDict()
        topic = row_dict['topic']
        if topic in topic_map:
            table,schema = topic_map[topic]
            value = row_dict['value']
            parsed = parse_value_to_row(value,schema)
            row_dict.pop('topic',None)
            row_dict.pop('value',None)
            merged_dict = {**parsed,**row_dict}
            # spark.createDataFrame([merged_dict])\
            #     .write\
            #     .format('delta')\
            #     .mode('append')\
            #     .saveAsTable(table)

        else:
            quarantine = spark.createDataFrame([row_dict])
            quarantine \
                .write \
                .format('delta') \
                .mode('append') \

                .saveAsTable('bookstore.bronze.quarantines')

silver_insert_ts_max = spark.sql(f"""
                                  SELECT COALESCE(MAX(insert_ts),'{silver_start_date}') AS insert_ts_max
                                  FROM bookstore.silver.books
                                  """).collect()[0]['insert_ts_max']

spark.sql(f"""
          SELECT *
          FROM bookstore.bronze.books
          WHERE insert_ts > '{silver_insert_ts_max}'
          """).createOrReplaceTempView('latest_feed')

_,silver_insert_ts_str = get_current_timestamp()

spark.sql(f"""
          MERGE INTO bookstore.silver.books dt
          USING (
            SELECT 'NA' AS merge_key, b.*
            FROM latest_feed b
            UNION ALL
            SELECT b.book_id AS merge_key, b.*
            FROM latest_feed b
            INNER JOIN bookstore.silver.books t
            ON t.book_id = b.book_id
            AND t.isActive = 'Y'
            AND t.start_date <> b.updated
          )st
          ON dt.book_id = st.merge_key

          WHEN MATCHED AND dt.isActive = 'Y' AND dt.start_date <> st.updated THEN
          UPDATE SET
          isActive = 'N',
          end_date = st.updated

          WHEN NOT MATCHED and st.merge_key is not null THEN
          INSERT (
            book_id
            ,title
            ,author
            ,price
            ,isActive
            ,start_date
            ,end_date
            ,source_file
            ,insert_ts
            )
          VALUES (
            st.book_id
            ,st.title
            ,st.author
            ,st.price
            ,'Y'
            ,st.updated
            ,to_timestamp('{silver_end_date}','yyyy-MM-dd HH:mm:ss')
            ,st.source_file
            ,to_timestamp('{silver_insert_ts_str}','yyyy-MM-dd HH:mm:ss')
          )
          """)

silver_insert_ts_max = spark.sql(f"""
                                  SELECT COALESCE(MAX(insert_ts),'{silver_start_date}') AS insert_ts_max
                                  FROM bookstore.silver.books
                                  """).collect()[0]['insert_ts_max']

spark.sql(f"""
          SELECT  * except (updated)
                  ,updated AS start_date
                  ,COALESCE(
                    LEAD(start_date) OVER (PARTITION BY book_id ORDER BY updated)
                    ,to_timestamp('{silver_end_date}', 'yyyy-MM-dd HH:mm:ss')
                    ) AS end_date
                  ,case when end_date = to_timestamp('{silver_end_date}', 'yyyy-MM-dd HH:mm:ss') then 'Y' else 'N' end AS isActive
          FROM    bookstore.bronze.books
          WHERE   insert_ts > '{silver_insert_ts_max}'
          """).createOrReplaceTempView('latest_feed')

insert_ts_sqlcol,insert_ts_str = get_current_timestamp()

booksUpdate_01 = spark.createDataFrame(data=[],schema=schema_books)

from pyspark.sql import Row

# Define new records as dictionaries
new_rows = [
    {
        'book_id': 'B01',
        'title': 'The Soul of a New Machine',
        'author': 'Tracy Kidder',
        'price': 59,
        'updated': '2025-10-20T17:20:33.507+00:00'
    },
    {
        'book_id': 'B01',
        'title': 'The Soul of a New Machine',
        'author': 'Tracy Kidder',
        'price': 69,
        'updated': '2025-10-20T17:30:33.507+00:00'
    }
]

# Create a DataFrame for the new rows
new_df = spark.createDataFrame(data=[Row(**row) for row in new_rows])
booksUpdate_01 = booksUpdate_01.unionByName(new_df)
booksUpdate_01 = booksUpdate_01\
    .withColumn('source_file',F.lit('s3://bookstorerawdata/rawfeed/booksUpdate_01.json'))\
    .withColumn('insert_ts',insert_ts_sqlcol)

display(booksUpdate_01.sort("book_id"))
# booksUpdate_01.write.mode("append").saveAsTable(source)

insert_ts_sqlcol,insert_ts_str = get_current_timestamp()

booksUpdate_02 = spark.createDataFrame(data=[],schema=schema_books)

from pyspark.sql import Row

# Define new records as dictionaries
new_rows = [
    {
        'book_id': 'B02',
        'title': 'Learning JavaScript Design Patterns',
        'author': 'Addy Osmani',
        'price': 35,
        'updated': '2025-10-20T18:15:33.507+00:00'
    },
    {
        'book_id': 'B42',
        'title': 'New book',
        'author': 'New author',
        'price': 11,
        'updated': '2025-10-20T18:20:33.507+00:00'
    }
]

# Create a DataFrame for the new rows
new_df = spark.createDataFrame(data=[Row(**row) for row in new_rows])
booksUpdate_02 = booksUpdate_02.unionByName(new_df)
booksUpdate_02 = booksUpdate_02\
    .withColumn('source_file',F.lit('s3://bookstorerawdata/rawfeed/booksUpdate_02.json'))\
    .withColumn('insert_ts',insert_ts_sqlcol)

display(booksUpdate_02.sort("book_id"))
# booksUpdate_02.write.mode("append").saveAsTable(source)

current_ts_sqlcol,current_ts_str = get_current_timestamp()

df = spark.createDataFrame(schema=['dummy'],data=[('1')])
df2 = df.withColumn('current_ts_spcol',current_ts_sqlcol).createOrReplaceTempView('v_df2')

df3 = (spark.sql(f"""
                 select *
                 ,to_timestamp('{current_ts_str}','yyyy-MM-dd HH:mm:ss') as current_ts_sqlcol
                ,'{current_ts_str}' as current_ts_str
                from v_df2
                """))
display(df3.schema)
display(df3)

def books_scdType2_upsert(spark,source='bookstore.bronze.books',target='bookstore.silver.books'):
    silver_first_ts = '1900-01-01 00:00:00'
    silver_last_ts = '9999-12-31 23:59:59'

    silver_insert_ts_max = spark.sql(f"""
                                    SELECT COALESCE(MAX(insert_ts),'{silver_first_ts}') AS insert_ts_max
                                    FROM {target}
                                    """).collect()[0]['insert_ts_max']

    latest_feed_df = spark.sql(f"""
            SELECT  book_id,title,author,price,start_date,end_date
                    ,CASE
                    WHEN end_date = to_timestamp('{silver_last_ts}', 'yyyy-MM-dd HH:mm:ss')
                    THEN 'Y'
                    ELSE 'N'
                    END AS isActive
                    ,source_file,insert_ts
            FROM    (
                    SELECT  *
                            ,updated AS start_date
                            ,COALESCE(
                                    LEAD(updated) OVER (PARTITION BY book_id ORDER BY updated)
                                    ,to_timestamp('{silver_last_ts}', 'yyyy-MM-dd HH:mm:ss')
                                    ) AS end_date
                    FROM    {source}
                    WHERE   insert_ts > '{silver_insert_ts_max}'
                    )
                    """)
    latest_feed_df.createOrReplaceTempView('latest_feed')

    if latest_feed_df.count() > 0:
        spark.sql(f"""
                with update_feed as(
                    SELECT  book_id
                            ,min(start_date) AS end_date_for_target_record
                    FROM  latest_feed
                    GROUP BY book_id
                )
                MERGE INTO {target} as target
                USING update_feed as source
                ON target.book_id = source.book_id
                WHEN MATCHED AND target.isActive = 'Y' THEN
                    UPDATE SET
                    target.isActive = 'N'
                    ,target.end_date = source.end_date_for_target_record
                ;
                """)
        _,silver_insert_ts_str = get_current_timestamp()
        spark.sql(f"""
                INSERT INTO {target} (book_id,title,author,price,start_date,end_date,isActive,source_file,insert_ts)
                SELECT  source.book_id,source.title,source.author,source.price,source.start_date,source.end_date,source.isActive,source.source_file,to_timestamp('{silver_insert_ts_str}','yyyy-MM-dd HH:mm:ss')
                FROM    latest_feed AS source
            """)