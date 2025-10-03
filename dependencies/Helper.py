# Databricks notebook source
import json
import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
from typing import Callable
import gc

# COMMAND ----------

def generate_dlt_script(dlt_path,feed_path,topics):
    script = f"""
import dlt
from pyspark.sql import functions as F

@dlt.view
def raw_feed():
    return (
        spark.readStream
        .format('cloudFiles')
        .option('cloudFiles.format','json')
        .load('{feed_path}')
        .select(
            F.col('topic')
            ,F.col('key').cast('string')
            ,F.col('value').cast('string')
            ,(F.col('timestamp')/1000).cast('timestamp').alias('create_ts')
            ,F.input_file_name().alias('source_file')
            ,F.current_timestamp().alias('insert_ts')
        )
    )
"""
    for topic in topics:
        topic_name = topic['name']
        topic_schema = topic['schema']
        topic_BronzeTblName = topic['BronzeTblName']
        script += f"""
'\n\n'
@dlt.table(name='{topic_BronzeTblName}')
def dlt_{topic}():
    return (
        dlt.read(raw_feed)
        .filter(F.col('topic') == {topic_name})
        .withColumn('v', F.from_json(F.col('value'),{topic_schema}))
        .select('key','create_ts','source_file','insert_ts','v.*')
    )
"""
    save_script(dlt_path,script)
    logger.info(f"Below script generated to CREATE DLT pipeline: \n'{dlt_path}'")
