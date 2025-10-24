from pyspark.sql import functions as F
from datetime import datetime
import pytz
#----------------------------------------------------------------------------------
def get_current_timestamp():
    current_ts_sqlcol = F.from_utc_timestamp(F.current_timestamp(),'Asia/Kolkata')
    current_ts_dtobj = datetime.now(pytz.timezone('Asia/Kolkata')) 
    current_ts_str = current_ts_dtobj.strftime('%Y-%m-%d %H:%M:%S')
    return current_ts_sqlcol,current_ts_str

  # Convert dtobject > string > sqlcol using to_timestamp 
  # (sqlcol can be used in Spark df directly, but not in spark sql; need to reconstruct again using to_timestamp during runtime)
  # current_ts_sqlcolFromStr = F.to_timestamp(current_ts_str,'yyyy-MM-dd HH:mm:ss')
  # print(type(current_ts_dtobj)) --> datetime
  # print(type(current_ts_sqlcolFromStr)) --> sqlcol
  # print(type(current_ts_sqlcol)) --> sqlcol
  # print(type(current_ts_str)) --> string
#-------------------------------------------------------------------------------------
def raw_feed(spark,path_,schema_,lakeflow=1):
    insert_ts_sqlcol,_ = get_current_timestamp()
    if lakeflow:
        feed = (
            spark.readStream
            .format('json')
            .option('recursiveFileLookup','false')
            .schema(schema_)
            .load(path_)
        )
    else:
        feed = (
            spark.read
            .format('json')
            .option('recursiveFileLookup','false')
            .schema(schema_)
            .load(path_)
        )
    return (
        feed
        .select(
            F.col('topic')
            ,F.col('key').alias('key_encoded')
            ,F.col('value').alias('value_encoded')
            ,F.col('partition')
            ,F.col('offset')
            ,F.col('timestamp')
            ,F.col('_metadata.file_path').alias('source_file')
            )
        .withColumn('key',F.col('key_encoded').cast('string'))
        .withColumn('value',F.col('value_encoded').cast('string'))
        .withColumn('create_ts',(F.timestamp_millis(F.col('timestamp'))))
        .withColumn('insert_ts',insert_ts_sqlcol)
        .select('topic','key','value','create_ts','source_file','insert_ts')
    )
#------------------------------------------------------------
def topics_feed(feed,name_,schema_):
    insert_ts_sqlcol,_ = get_current_timestamp()
    return (
        feed
        .filter(F.col('topic') == name_)
        .select('value','source_file')
        .withColumn('v',F.from_json(F.col('value'),schema_))
        .withColumn('insert_ts',insert_ts_sqlcol)
        .select('v.*','source_file','insert_ts')
    )
#----------------------------------------------------------------------
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

