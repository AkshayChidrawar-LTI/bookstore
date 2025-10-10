from pyspark.sql import functions as F

def raw_feed(spark,path_,schema_):
    insert_ts = F.from_utc_timestamp(F.current_timestamp(),'Asia/Kolkata')
    return (
        spark.readStream.format('json')
        .schema(schema_)
        .load(path_)
        .withColumn('insert_ts',insert_ts)
        .select(
            F.col('topic')
            ,F.col('key').cast('string')
            ,F.col('value').cast('string')
            ,(F.col('timestamp') / 1000).cast('timestamp').alias('create_ts')
            ,F.col('_metadata.file_path').alias('source_file')
            ,F.col('insert_ts')
            )
    )

#------------------------------------------------------------

def parse_value_to_row(value,schema):
  df = spark.createDataFrame([{"value":json.dumps(value)}])
  parsed_df = df\
  .select(F.from_json(F.col("value"),schema).alias("parsed"))\
  .select("parsed.*")
  return parsed_df.collect()[0]

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

def topics_feed(feed,name_,schema_):
    return (
        feed
        .filter(F.col('topic') == name_)
        .withColumn('v',F.from_json(F.col('value'),schema_))
        .select('v.*','create_ts','source_file','insert_ts')
    )

def books_scdType2_upsert(spark):
  spark.sql(f"""
            SELECT *
            FROM bookstore.bronze.feeds
            WHERE
            """).createOrReplaceTempView('latest_feed')

  spark.sql(f"""
            MERGE INTO bookstore.silver.books dt
            USING (
              SELECT NULL AS merge_key, b.*
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

            WHEN NOT MATCHED THEN
              INSERT (book_id, title, author, price, isActive, start_date, end_date)
              VALUES (st.book_id, st.title, st.author, st.price, 'Y', st.updated, NULL);
            """)

