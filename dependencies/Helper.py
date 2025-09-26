# Databricks notebook source
import os
import yaml
import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
import logging
import gc
spark.conf.set("spark.sql.session.timeZone","Asia/Kolkata")

# COMMAND ----------

def get_path(*args):
  return os.path.join(*args)

def get_sloc(item):
  return item.replace('.','/')

def get_fname(item):
  return item.replace('.','_')

def get_bucketlink(bucket_name):
  return 's3://'+ bucket_name + '/'

def appendTo_DF(df,new_row)->pd.DataFrame:
    df = pd.concat([df,pd.DataFrame([new_row])],ignore_index=True)
    return df

def setLogger(logFileName):
  logger = logging.getLogger()
  formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
  # Remove existing FileHandlers for this log file
  for handler in logger.handlers[:]:
    if isinstance(handler,logging.FileHandler) and handler.baseFilename == logFileName:
      logger.removeHandler(handler)
  if not logger.handlers: # Avoid adding multiple handlers if already present
    handler = logging.FileHandler(logFileName, mode='w')
  logger.setLevel(logging.INFO)
  handler.setFormatter(formatter)
  logger.addHandler(handler)
  return logger

# COMMAND ----------

def read_yaml(file_path)->dict|list:
    with open(file_path, 'r') as f:
        fc = yaml.safe_load(f)
    return fc

def null_check(cnull):
    if cnull:
        return ''
    else:
        return 'not null'

def yaml_to_schema(columns):
    schema = '('
    for col in columns:
        if col['type'] == 'array' and col['item']['type'] == 'struct':
            schema += f"\n" + col['name'] + ' array <struct<'
            for itemcol in col['item']['columns']:
                schema += f"\n\t{itemcol['name']} {itemcol['type']} {null_check(itemcol['nullable'])},"
            schema = schema.rstrip(',') + '>>'
        else:
            schema += f"\n{col['name']} {col['type']} {null_check(col['nullable'])},"
    schema = schema.rstrip(',') + '\n)'
    return schema

# COMMAND ----------

def save_script(file_name,script):
    with open(file_name, 'w') as file:
        file.write(script)

def remove_script(file_name,logger):
    try:
        dbutils.fs.rm(file_name,recursive=True)
        logger.info(f"Success: Script removed: '{file_name}'")
    except Exception as e:
        logger.warning(f"Failure: Error occured while removing file '{file_name}:'\n{e}")

def execute_script(script_file,logger):
    with open(script_file) as f:
        code = f.read()
    exec(code, {'logger': logger})

def generate_CREATE_scripts(object_type,object_name,object_ddl,logger,**kwargs):
    host = kwargs.get('host','')
    token = kwargs.get('token','')
    bucket_arn = kwargs.get('bucket_arn','')
    el_loc = kwargs.get('el_loc','')
    el_sc = kwargs.get('el_sc','')
    table_loc = kwargs.get('table_loc','')
    table_schema = kwargs.get('table_schema','')

    if object_type == 'storage_credential':
        script = f"""
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import AwsIamRole

ws = WorkspaceClient(host='{host}',token = '{token}')
try:
    ws.storage_credentials.create(
        name='{object_name}',
        aws_iam_role=AwsIamRole(role_arn='{bucket_arn}')
        )
    logger.info(f"\\nSuccess: CREATE {object_type} '{object_name}'")
except Exception as e:
    logger.critical(f"\\nFailure: CREATE {object_type} '{object_name}': \\n{{e}}")
"""
    elif object_type == 'external_location':
        script = f"""
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient(host='{host}',token = '{token}')
try:
    ws.external_locations.create(
        name='{object_name}'
        ,url='{el_loc}'
        ,credential_name='{el_sc}'
        )
    logger.info(f"\\nSuccess: CREATE {object_type} '{object_name}': \\n'{el_loc}'")
except Exception as e:
    logger.critical(f"\\nFailure: CREATE {object_type} '{object_name}': \\n{{e}}")
"""
    elif object_type in ['CATALOG','DATABASE','TABLE']:
        script_ddl_create = f"CREATE {object_type} {object_name}"
        if object_type in ['CATALOG','DATABASE']:
            script_ddl_schema = ''
        elif object_type == 'TABLE':
            script_ddl_schema = f"\n{table_schema};\n"
        script_ddl = script_ddl_create+script_ddl_schema
        script = f"""
try:
    spark.sql(f\"\"\"{script_ddl}\"\"\")
    logger.info(f"\\nSuccess: CREATE {object_type} '{object_name}'")
except Exception as e:
    logger.error(f"\\nFailure: CREATE {object_type} '{object_name}': \\n{{e}}")
"""
    save_script(object_ddl,script)
    logger.info(f"\nBelow script generated to CREATE {object_type} '{object_name}'. \n'{object_ddl}'")

def generate_DROP_scripts(object_type,object_name,object_ddl,logger,**kwargs):
    host = kwargs.get('host')
    token = kwargs.get('token')

    if object_type == 'storage_credential':
        script = f"""
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient(host='{host}',token = '{token}')
try:
    ws.storage_credentials.delete(name='{object_name}')
    logger.info(f"\\nSuccess: DROP {object_type} '{object_name}'")
except Exception as e:
    logger.critical(f"\\nFailure: DROP {object_type} '{object_name}': \\n{{e}}")
"""
    elif object_type == 'external_location':
        script = f"""
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient(host='{host}',token = '{token}')
try:
    ws.external_locations.delete(name='{object_name}')
    logger.info(f"\\nSuccess: DROP {object_type} '{object_name}'")
except Exception as e:
    logger.critical(f"\\nFailure: DROP {object_type} '{object_name}': \\n{{e}}")
"""
    elif object_type in ['CATALOG','DATABASE','TABLE']:
        if object_type in ['CATALOG','DATABASE']:
            cascade = 'CASCADE'
        elif object_type in ['TABLE']:
            cascade = ''
        script_ddl = f"DROP {object_type} {object_name} {cascade}"
        script = f"""
try:
    spark.sql(f\"\"\"{script_ddl}\"\"\")
    logger.info(f"\\nSuccess: DROP {object_type} '{object_name}'")
except Exception as e:
    logger.error(f"\\nFailure: DROP {object_type} '{object_name}': \\n{{e}}")
"""
    save_script(object_ddl,script)
    logger.info(f"\nBelow script generated to DROP {object_type} '{object_name}'. \n'{object_ddl}'")

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
    logger.info(f"\nBelow script generated to CREATE DLT pipeline: \n'{dlt_path}'")
