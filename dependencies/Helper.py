# Databricks notebook source
import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import AwsIamRole
import yaml
import json
import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
from typing import Callable
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

def create_Workspace(host,token,logger):
  workspace = WorkspaceClient(host=host,token=token)
  logger.info(f"Success: WorkspaceClient object created:\n {workspace}")
  return workspace

# COMMAND ----------

import logging
import pytz
from datetime import datetime

class ISTFormatter(logging.Formatter):
    def converter(self,timestamp):
        dt = datetime.fromtimestamp(timestamp, pytz.timezone('Asia/Kolkata'))
        return dt
    def formatTime(self, record, datefmt=None):
        dt = self.converter(record.created)
        if datefmt is None:
            datefmt = '%Y-%m-%d %H:%M:%S'
        return dt.strftime(datefmt)

def create_Logger(logger_name,log_file,log_mode):
  logger = logging.getLogger(logger_name)
  formatter = ISTFormatter('%(asctime)s - %(levelname)s - %(message)s')
  # Remove existing FileHandlers for this log file
  for handler in logger.handlers[:]:
    if isinstance(handler,logging.FileHandler) and handler.baseFilename == log_file:
      logger.removeHandler(handler)
  if not logger.handlers: # Avoid adding multiple handlers if already present
    handler = logging.FileHandler(log_file, mode=log_mode)
  logger.setLevel(logging.INFO)
  handler.setFormatter(formatter)
  logger.addHandler(handler)
  return logger

def get_Logger(logger_name):
  return logging.getLogger(logger_name)

def get_log_file(logger):
  return logger.handlers[0].baseFilename

def remove_log_files(path_log,logger):
  for log_file in os.listdir(path_log):
    if log_file != 'purge.log':
      remove_script(object_ddl=get_path(path_log,log_file),logger=logger)

def remove_loggers(logger_purge):
  for logger_name in [
    'logger_for_initializer',
    'logger_for_scripting',
    'logger_for_setup',
    'logger_for_cleanup']:
    logger = logging.getLogger(logger_name)
    handlers = logger.handlers[:]
    for handler in handlers:
      logger.removeHandler(handler)
      handler.close()
      logger_purge.info(f"Logger Handler removed: {logger} - {handler}")

# COMMAND ----------

logger_for_initializer = create_Logger('logger_for_initializer',get_path('/Workspace/Users/akshay.chidrawar@ltimindtree.com/bookstore/log','initializer.log'),'w')
logger_for_scripting = create_Logger('logger_for_scripting',get_path('/Workspace/Users/akshay.chidrawar@ltimindtree.com/bookstore/log','scripting.log'),'w')
logger_for_setup = create_Logger('logger_for_setup',get_path('/Workspace/Users/akshay.chidrawar@ltimindtree.com/bookstore/log','setup.log'),'w')
logger_for_purge = create_Logger('logger_for_purge',get_path('/Workspace/Users/akshay.chidrawar@ltimindtree.com/bookstore/log','purge.log'),'a')
for logger_name in [
    'logger_for_initializer',
    'logger_for_scripting',
    'logger_for_setup',
    'logger_for_cleanup']:
    logger = logging.getLogger(logger_name)
    handlers = logger.handlers[:]
    for handler in handlers:
      logger_for_purge.info(f"Logger Handler removed: {logger.name} - {handler}")
      logger.removeHandler(handler)
      handler.close()

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

################################ CREATE ######################################

def script_for_create_sc(object_name,aws_iam_role):
    script = f"""
workspace.storage_credentials.create(
    name='{object_name}'
    ,aws_iam_role={aws_iam_role}
    )
"""
    return script
#----------------------------------------------------------------------
def script_for_create_el(object_name,el_loc,el_sc):
    script = f"""
workspace.external_locations.create(
    name='{object_name}'
    ,url='{el_loc}'
    ,credential_name='{el_sc}'
    )
"""
    return script
#----------------------------------------------------------------------
def script_for_create_cdt(object_type,object_name,table_schema=''):
    script = f"""
spark.sql(f\"\"\" CREATE {object_type} {object_name} \\n{table_schema};\\n \"\"\")
"""
    return script

################################## DROP ####################################

def script_for_drop_sc(object_name):
    script = f"""
workspace.storage_credentials.delete(name='{object_name}')
"""
    return script
#----------------------------------------------------------------------
def script_for_drop_el(object_name):
    script = f"""
workspace.external_locations.delete(name='{object_name}')
"""
    return script
#----------------------------------------------------------------------
def script_for_drop_cdt(object_type,object_name):
    cascade = '' if object_type == 'TABLE' else 'CASCADE'
    script = f"spark.sql(\"DROP {object_type} {object_name} {cascade};\")\n"
    return script

# COMMAND ----------

from functools import wraps
import inspect

def try_except(success_template, failure_template):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            bound_args = inspect.signature(func).bind(*args, **kwargs)
            bound_args.apply_defaults()
            logger = kwargs.get('logger', None)
            try:
                result = func(*args, **kwargs)
                if logger:
                    logger.info(success_template.format(
                        **bound_args.arguments,
                        result=str(result)
                    ))
                return result
            except Exception as e:
                if logger:
                    # Add 'e' and 'result' to arguments for formatting
                    format_args = dict(bound_args.arguments)
                    format_args['e'] = e
                    # Remove 'result' if already present to avoid duplicate key error
                    format_args.pop('result', None)
                    logger.error(failure_template.format(**format_args))
                raise
        return wrapper
    return decorator
#---------------------------------------------------------------------------
@try_except("\nSuccess: Scripts generated for CREATE and DROP {object_type} {object_name}. Proceeding to save script..."
            ,"\nFailure: Error occurred while generating scripts CREATE/ DROP for {object_type} {object_name} :\n{e}")
def generate_script(object_type,object_name,logger,**kwargs):
    workspace   = kwargs.get('workspace','')
    aws_iam_role= kwargs.get('aws_iam_role','')
    el_loc      = kwargs.get('el_loc','')
    el_sc       = kwargs.get('el_sc','')
    table_schema= kwargs.get('table_schema','')

    func_map = {
        'storage_credential': (
            lambda:script_for_create_sc(object_name,aws_iam_role)
            ,lambda:script_for_drop_sc(object_name)
        )
        ,'external_location': (
            lambda:script_for_create_el(object_name,el_loc,el_sc)
            ,lambda:script_for_drop_el(object_name)
            )
        ,'CATALOG':(
            lambda:script_for_create_cdt(object_type,object_name)
            ,lambda:script_for_drop_cdt(object_type,object_name)
            )
        ,'DATABASE':(
            lambda: script_for_create_cdt(object_type,object_name)
            ,lambda: script_for_drop_cdt(object_type,object_name)
            )
        ,'TABLE':(
            lambda: script_for_create_cdt(object_type,object_name,table_schema)
            ,lambda: script_for_drop_cdt(object_type,object_name)
            )
        }
    if object_type in func_map:
        create_func,drop_func = func_map[object_type]
        create_script = create_func()
        drop_script   = drop_func()
    else:
        raise ValueError(f"Unsupported object_type: {object_type}")
    return create_script,drop_script

@try_except("\nSuccess: Script saved here: '{object_ddl}'"
            ,"\nFailure: Error occurred while saving script '{object_ddl}':\n{e}")
def save_script(object_ddl,script,logger):
    with open(object_ddl,'w') as file:
        file.write(script)

def GenerateSave_script(object_type,object_name,object_ddl_CREATE,object_ddl_DROP,logger,**kwargs):
    create_script,drop_script = generate_script(object_type=object_type,object_name=object_name,logger=logger,**kwargs)
    save_script(object_ddl=object_ddl_CREATE,script=create_script,logger=logger)
    save_script(object_ddl=object_ddl_DROP,script=drop_script,logger=logger)

@try_except("\nSuccess: Script executed: '{object_ddl}'"
            ,"\nFailure: Error occurred while executing script '{object_ddl}':\n{e}")
def execute_script(object_ddl,logger,context={}):
    with open(object_ddl) as f:
        code = f.read()
    exec(code,context)

@try_except("\nSuccess: Script removed: '{object_ddl}'"
            ,"\nFailure: Error occurred while removing script '{object_ddl}':\n{e}")
def remove_script(object_ddl,logger):
    os.remove(object_ddl)

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
