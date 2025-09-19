# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql import functions as F
from pyspark.sql.window import Window
spark.conf.set("spark.sql.session.timeZone","Asia/Kolkata")

# COMMAND ----------

# DBTITLE 1,Variables
source_path = 'dbfs:/mnt/demo-datasets/DE-Pro/bookstore/'
kafka_streaming = source_path+'kafka-streaming/'
kafka_raw = source_path+'kafka-raw/'
ItemToRetain = kafka_raw+'01.json'
books_updates_streaming = 'dbfs:/mnt/demo-datasets/DE-Pro/bookstore/books-updates-streaming'
books_updates_raw = kafka_raw+'books-updates/'

storage_path = 'dbfs:/user/hive/warehouse/'
checkpoint_path = 'dbfs:/mnt/demo_pro/checkpoints/'
lookup_country_path = 'dbfs:/mnt/demo-datasets/DE-Pro/bookstore/country_lookup'

dbname = 'bookstore_eng_pro'
tblID11 = 'bronze_books'
tblID12 = 'bronze_customers'
tblID13 = 'bronze_orders'
tblID21 = 'silver_books'
tblID22 = 'silver_customers'
tblID23 = 'silver_orders'
tblID24 = 'silver_orders_books'
tblID25 = 'silver_orders_customers'
tblID26 = 'silver_currentbooks'
tblID31 = 'gold_'
tblList = [tblID11,tblID12,tblID13,tblID21,tblID22,tblID23,tblID24,tblID25,tblID26]
createTableList = [tblID21,tblID22,tblID23,tblID25]

#Below schemas are used to impose on source data feed. Bronze tables need not be created in advance like silver tables, because Bronze tables are not used in MERGE operation, but used to capture stream data for respective topics. This operation only requires schema definitions to be imposed on source.  
SchemaImposedOnSource = 'key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp long'
RecordSchema_books = 'book_id STRING, title STRING, author STRING, price DOUBLE, updated TIMESTAMP'
RecordSchema_customers = 'customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, city STRING, country_code STRING, row_status STRING, row_time timestamp'
RecordSchema_orders = 'order_id STRING, order_timestamp timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>'

#Silver tables are used during MERGE operation for CDC. They must be created before being used in MERGE operation.
tblSchema_silver_books = 'book_id STRING, title STRING, author STRING, price DOUBLE, isActive BOOLEAN, start_date TIMESTAMP, end_date TIMESTAMP'
tblSchema_silver_customers = 'customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, city STRING, country STRING, last_updated TIMESTAMP'
tblSchema_silver_orders_customers = 'order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country STRING, row_time TIMESTAMP, processed_timestamp TIMESTAMP, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>'

#this table is not used in Merge operation. 
tblSchema_silver_orders = 'order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>'

dbStorage = storage_path+dbname+'.db/'

#current_catalog = str(spark.sql(f"SELECT current_catalog()").collect()[0][0])

# COMMAND ----------

# DBTITLE 1,Helper 1
def getValue_ForGlobalVar(fix,var):
  return globals()[f'{fix}{var}']

def setPaths(tblID):
  globals()[f'tbl_{tblID}'] = f'{dbname}.{tblID}'
  globals()[f'tblStorage_{tblID}'] = f'{dbStorage}{tblID}'
  globals()[f'tblCheckpoint_{tblID}'] = f'{checkpoint_path}{tblID}'
  print(
    f'tbl_{tblID}',' = ',getValue_ForGlobalVar('tbl_',tblID)
    ,'\n',f'tblStorage_{tblID}',' = ',getValue_ForGlobalVar('tblStorage_',tblID)
    ,'\n',f'tblCheckpoint_{tblID}',' = ',getValue_ForGlobalVar('tblCheckpoint_',tblID))

def setVars():
  for tblID in tblList:
    setPaths(tblID)
    print('\n')

def path_exists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except:
    return False

def delete_file_if_exists(file_path):
  if path_exists(file_path):
    dbutils.fs.rm(file_path)
    print('File deleted: ',file_path)
  else:
    print('File does not exists: ',file_path)

def delete_folder_if_exists(folder_path):
  if path_exists(folder_path):
    dbutils.fs.rm(folder_path,recurse=True)
    print('Folder deleted: ',folder_path)
  else:
    print('Folder does not exists: ',folder_path)

def drop_Database(dbname,dbStorage):
  delete_folder_if_exists(dbStorage)
  try:
    spark.sql(f"DROP DATABASE {dbname} CASCADE")
    print('Database',f"'{dbname}'",'dropped from Hive metastore')
  except:
    print('Database',f"'{dbname}'",'does not exist in Hive metastore')

def create_Database(dbname):
  spark.sql(f"CREATE DATABASE {dbname}")
  print('Database',f"'{dbname}'",'created in Hive metastore')

def drop_Table(tblID):
  delete_folder_if_exists(getValue_ForGlobalVar('tblStorage_',tblID))
  delete_folder_if_exists(getValue_ForGlobalVar('tblCheckpoint_',tblID))
  try:
    spark.sql(f"DROP TABLE {getValue_ForGlobalVar('tbl_',tblID)}")
    print('Table',f"'{getValue_ForGlobalVar('tbl_',tblID)}'",'dropped from Hive metastore')
  except:
    print('Table',f"'{getValue_ForGlobalVar('tbl_',tblID)}'",'does not exist in Hive metastore')

def create_Table(tblID):
  spark.sql(f"create table {getValue_ForGlobalVar('tbl_',tblID)} ({getValue_ForGlobalVar('tblSchema_',tblID)})")
  print('Table created: ',f"'{getValue_ForGlobalVar('tbl_',tblID)}'"
        ,'\n Schema: ',f"'{getValue_ForGlobalVar('tblSchema_',tblID)}'")

def clearAllButItem(path,ItemToRetain):
  for i in dbutils.fs.ls(path):
    if i.path != ItemToRetain:
        dbutils.fs.rm(i.path,recurse=True)
        print('Item deleted: ',i.path)

def cleanup_and_setup():
  clearAllButItem(kafka_raw,ItemToRetain)
  drop_Database(dbname,dbStorage)
  print('\n')
  create_Database(dbname)
  print('\n')
  for tblID in tblList:
    if (tblID != tblID26):
      drop_Table(tblID)
      print('\n')
  for tblID in createTableList:    
    create_Table(tblID)
    print('\n')

def enableCDFonTable(tblID):
  spark.sql(f"""
          ALTER TABLE {tblID} 
          SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
          """)


# COMMAND ----------

# DBTITLE 1,Helper 2
def get_index(dir):
    try:
        files = dbutils.fs.ls(dir)
        file = max(f.name for f in files if f.name.endswith('.json'))
        index = int(file.rsplit('.', maxsplit=1)[0])
    except:
        index = 0
    return index+1

def load_json_file(current_index,streaming_dir,raw_dir):
    latest_file = f"{str(current_index).zfill(2)}.json"
    source = f"{streaming_dir}/{latest_file}"
    target = f"{raw_dir}/{latest_file}"
    prefix = streaming_dir.split("/")[-1]
    if path_exists(source):
        print(f"Loading {prefix}-{latest_file} file to the bookstore dataset")
        dbutils.fs.cp(source, target)

def load_data(max,streaming_dir,raw_dir,all=False):
    index = get_index(raw_dir)
    if index > max:
        print("No more data to load\n")
    elif all == True:
        while index <= max:
            load_json_file(index, streaming_dir, raw_dir)
            index += 1
    else:
        load_json_file(index, streaming_dir, raw_dir)
        index += 1

def load_books_updates():
    streaming_dir = books_updates_streaming
    raw_dir = books_updates_raw
    load_data(5,streaming_dir, raw_dir)

def load_new_data(num_files = 1):
    streaming_dir = kafka_streaming
    raw_dir = kafka_raw
    for i in range(num_files):
        load_data(10, streaming_dir, raw_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ####BRONZE

# COMMAND ----------

# DBTITLE 1,BRONZE - books, customers, orders
def generateReadStream():
    readQuery = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(SchemaImposedOnSource)
        .load(kafka_raw)
        .select(
            F.col("topic")
            ,F.col("key").cast("string")
            ,(F.col("timestamp")/1000).cast("timestamp").alias("create_ts")
            ,F.input_file_name().alias("Source_file")
            ,F.current_timestamp().alias("insert_ts")
            ,F.col("value").cast("string")
            )
    )
    return readQuery

def writeData_bronze(readQuery,Topic,tblID):
    write_bronze_Topic = (
        readQuery
        .filter(F.col("topic")==Topic)
        .withColumn("v",F.from_json(F.col("value"),getValue_ForGlobalVar('RecordSchema_',Topic)))
        .select("key","create_ts","Source_file","insert_ts","v.*")
        .writeStream
        .option("checkpointLocation",getValue_ForGlobalVar('tblCheckpoint_',tblID))
        .option("mergeSchema", True)
        .trigger(availableNow=True)
        .table(getValue_ForGlobalVar('tbl_',tblID))
    )
    write_bronze_Topic.awaitTermination()
    
    print(
    'Source\n',kafka_raw 
    ,'\nSchema\n',getValue_ForGlobalVar('RecordSchema_',Topic)
    ,'\nDest\n',getValue_ForGlobalVar('tbl_',tblID),'\n'
    )

def process_bronze_books():
    readQuery = generateReadStream()
    writeData_bronze(readQuery,'books','bronze_books')
    
def process_bronze_customers():
    readQuery = generateReadStream()
    writeData_bronze(readQuery,'customers','bronze_customers')

def process_bronze_orders():
    readQuery = generateReadStream()
    writeData_bronze(readQuery,'orders','bronze_orders')
    

# COMMAND ----------

# MAGIC %md
# MAGIC ###SILVER

# COMMAND ----------

# DBTITLE 1,SILVER - books (SCD Type2 Upsert)
def process_silver_books():  
  query = (
    spark.readStream
    .table(tbl_bronze_books)
    .withColumnRenamed("updated","update_date")
    .writeStream
    .foreachBatch(lambda BatchDF,BatchID: scdType2_upsert(BatchDF,BatchID,tbl_silver_books))
    .option("checkpointLocation", tblCheckpoint_silver_books)
    .trigger(availableNow=True)
    .start()
  )
  query.awaitTermination()
  print(
    'Source\n',tbl_bronze_books
    ,'\nDest\n',tbl_silver_books,'\n',tblStorage_silver_books,'\n',tblCheckpoint_silver_books
    )

def scdType2_upsert(BatchDF,BatchID,tbl):
  BatchDF.createOrReplaceTempView("batch")
  query = f"""
  MERGE INTO {tbl} AS dt
  USING
  (
      select null as merge_key,b.*
      from batch b
      union all
      select b.book_id as merge_key,b.*
      from batch b inner join {tbl} t
      on t.book_id = b.book_id
      and t.isActive = 'Y' 
      and t.start_date <> b.update_date
  )AS st 
  ON dt.book_id = st.merge_key

  WHEN MATCHED AND dt.isActive = 'Y' AND dt.start_date <> st.update_date THEN 
  UPDATE SET
  dt.isActive = 'N',
  dt.end_date = st.update_date

  WHEN NOT MATCHED THEN
  INSERT (dt.book_id,dt.title,dt.author,dt.price,dt.isActive,dt.start_date,dt.end_date)
  VALUES (st.book_id,st.title,st.author,st.price,'Y',st.update_date,NULL);
  """
  BatchDF.sparkSession.sql(query)


# COMMAND ----------

# DBTITLE 1,SILVER - customers (SCD Type1 Upsert)
def process_silver_customers():
    query = (
        spark.readStream
        .table(tbl_bronze_customers)
        .join(F.broadcast(spark.read.json(lookup_country_path)),F.col("country_code") == F.col("code"),"inner")        
        .writeStream
        .foreachBatch(lambda BatchDF,BatchID: scdType1_upsert(BatchDF,BatchID,tbl_silver_customers))        
        .option("checkpointLocation", tblCheckpoint_silver_customers)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    print(
        'Source\n',tbl_bronze_customers
        ,'\nDest\n',tbl_silver_customers,'\n',tblStorage_silver_customers,'\n',tblCheckpoint_silver_customers
        )

def scdType1_upsert(BatchDF,BatchID,tbl):
    window = Window.partitionBy("customer_id").orderBy(F.col("row_time").desc())
    ranked_updatesDF = (
        BatchDF
        .filter(F.col("row_status").isin(["insert", "update"]))
        .withColumn("rank",F.rank().over(window))
        .filter("rank == 1")
        .drop("rank")        
       )
    ranked_updatesDF.createOrReplaceTempView("ranked_updates")

    query = f"""
    MERGE INTO {tbl} dt
    USING ranked_updates st
    ON dt.customer_id = st.customer_id
    WHEN MATCHED AND dt.last_updated < st.row_time THEN 
    UPDATE SET 
    dt.customer_id = st.customer_id
    ,dt.email = st.email
    ,dt.first_name = st.first_name
    ,dt.last_name = st.last_name
    ,dt.gender = st.gender
    ,dt.city = st.city
    ,dt.country = st.country
    ,dt.last_updated = st.row_time

    WHEN NOT MATCHED THEN 
    INSERT (dt.customer_id,dt.email,dt.first_name,dt.last_name,dt.gender,dt.city,dt.country,dt.last_updated)
    VALUES (st.customer_id,st.email,st.first_name,st.last_name,st.gender,st.city,st.country,st.row_time)
    """
    BatchDF.sparkSession.sql(query)
#,dt.last_updated = st.row_time
# window operations are only supported for static read DF and not for readStream DF. This problem can be resolved by using the forEachBatch function, here we use batchDF and batch syntax instead of stream syntax. 
# In Merge operation, if source & target tables have different schemas, merging happens in accordance with dest table (target). 

# COMMAND ----------

# DBTITLE 1,SILVER - orders (insert_only)
def process_silver_orders():
    query = (
        spark.readStream.table(tbl_bronze_orders)
        .withWatermark("order_timestamp", "30 seconds")
        .dropDuplicates(["order_id", "order_timestamp"])
        .writeStream
        .foreachBatch(lambda BatchDF,BatchID: insert_only(BatchDF,BatchID,tbl_silver_orders))
        .option("checkpointLocation", tblCheckpoint_silver_orders)
        .trigger(availableNow=True)
        .start()
        )
    query.awaitTermination()
    
    print(
        'Source\n',tbl_bronze_orders
        ,'\nDest\n',tbl_silver_orders,'\n',tblStorage_silver_orders,'\n',tblCheckpoint_silver_orders
        )

def insert_only(BatchDF,batchID,tbl):
  BatchDF.createOrReplaceTempView("orders")    
  query = f"""
  MERGE INTO {tbl} dt
  USING orders st
  ON dt.order_id = st.order_id AND dt.order_timestamp = st.order_timestamp
  WHEN NOT MATCHED THEN 
  INSERT (dt.order_id,dt.order_timestamp,dt.customer_id,dt.quantity,dt.total,dt.books)
  VALUES (st.order_id,st.order_timestamp,st.customer_id,st.quantity,st.total,st.books)
  """
  BatchDF.sparkSession.sql(query)

#INSERT ONLY if not exists. implement duplicate check logic before inserting data into target table.
#dropDuplicates() can be used for both .read and .readstream objects. But with .readstream, it makes more sense to use dropDuplicates() along with withWatermark() function. 
# .foreachBatch does not require to explicitly pass parameters for MicroBatchDF & MicroBatchID. It only requires to pass a function which has that signature. Spark detects and pass those parameters automatically. 


# COMMAND ----------

# MAGIC %md
# MAGIC ###JOINS

# COMMAND ----------

# DBTITLE 1,Silver - Orders_Books (Stream - Static join)
def process_silver_orders_books():
    orders_df = (
        spark.readStream
        .table(tbl_silver_orders)
        .withColumn("book", F.explode("books"))
        )
    books_df = (
        spark.read
        .table(tbl_silver_currentbooks)
        )
    query = (
        orders_df
        .join(books_df,orders_df.book.book_id == books_df.book_id, "inner")
        .drop("books","book","quantity","total")
        .writeStream
        .outputMode("append")
        .option("checkpointLocation", tblCheckpoint_silver_orders_books)
        .option("mergeSchema",True)
        .trigger(availableNow=True)
        .table(tbl_silver_orders_books)
    )
    query.awaitTermination()

def process_silver_currentbooks():
    spark.sql(f"""
                create or replace table {tbl_silver_currentbooks} as
                select * from {tbl_silver_books}
                where isActive='true'
              """)

# COMMAND ----------

# DBTITLE 1,Silver - Orders_Customers (Stream - Stream join)
def process_silver_orders_customers():    
    customers_cdf =(
        spark.readStream
        .option("readChangeFeed", True)
        .option("startingVersion", 2)
        .table(tbl_silver_customers)
    )    
    query = (
        spark.readStream.table(tbl_silver_orders)
        .join(customers_cdf,["customer_id"],"inner")
        .writeStream
        .foreachBatch(lambda BatchDF,BatchID: cdf_scdType1_upsert(BatchDF,BatchID,tbl_silver_orders_customers))
        .option("checkpointLocation", tblCheckpoint_silver_orders_customers)
        .trigger(availableNow=True)
        .start()
        )
    query.awaitTermination()    
    print(
        'Source\n',tbl_silver_orders,'\n',tbl_silver_customers 
        ,'\nDest\n',tbl_silver_orders_customers,'\n',tblCheckpoint_silver_orders_customers
        )

def cdf_scdType1_upsert(BatchDF,batchID,tbl):
    window = F.Window.partitionBy("order_id", "customer_id").orderBy(F.col("_commit_timestamp").desc())
    ranked_updates_df = (
        BatchDF
        .filter(F.col("_change_type").isin(["insert", "update_postimage"]))
        .withColumn("rank", F.rank().over(window))
        .filter("rank = 1")
        .drop("rank", "_change_type", "_commit_version")
        .withColumnRenamed("_commit_timestamp", "processed_timestamp"))
    ranked_updates_df.createOrReplaceTempView("ranked_updates")
    
    query = f"""
    MERGE INTO {tbl} dt
    USING ranked_updates st
    ON dt.order_id = st.order_id AND dt.customer_id = st.customer_id
    WHEN MATCHED AND dt.processed_timestamp < st.processed_timestamp
    THEN UPDATE SET *
    WHEN NOT MATCHED
    THEN INSERT *
    """
    BatchDF.sparkSession.sql(query)



# COMMAND ----------

# MAGIC %md
# MAGIC ###EXIT (LAST CELL)

# COMMAND ----------

# DBTITLE 1,EXIT
dbutils.notebook.exit("Notebook execution completed successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ###GOLD

# COMMAND ----------

# DBTITLE 1,GOLD - View
def process_gold_:
    query = (
        spark.readStream
        .table("books_sales")
        .withWatermark("order_timestamp", "10 minutes")
        .groupBy(
            F.window("order_timestamp", "5 minutes").alias("time")
            ,"author")
        .agg(
            F.count("order_id").alias("orders_count")
            ,F.avg("quantity").alias ("avg_quantity"))
        .writeStream
        .option("checkpointLocation", f"dbfs:/mnt/demo_pro/checkpoints/authors_stats")
        .trigger(availableNow=True)
        .table("authors_stats")
            )

query.awaitTermination()