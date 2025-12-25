from pyspark.sql import functions as F
from datetime import datetime
import pytz

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

def ingest_data(dbutils,source_dir,target_dir,prefix):
    files = dbutils.fs.ls(target_dir)
    json_files = [
        f.name for f in files
        if f.name.startswith(prefix) and f.name.endswith('.json')
    ]
    if json_files:
        indices = [
            int(f.rsplit('.', 1)[0].split('_')[-1])
            for f in json_files
        ]
        max_index = max(indices)
    else:
        max_index = 0
    next_index = str(max_index + 1).zfill(2)
    source_file = f"{source_dir}{prefix}_{next_index}.json"
    target_file = f"{target_dir}{prefix}_{next_index}.json"
    dbutils.fs.cp(source_file,target_file)

def clear_contents(dbutils,path):
    items = dbutils.fs.ls(path)
    for i in items:
        dbutils.fs.rm(i.path,recurse=True)