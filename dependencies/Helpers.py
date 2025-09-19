# Databricks notebook source
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import AwsIamRole
import pandas as pd
import re
import pyspark.sql.functions as F
import pyspark.sql.types as T
import os
import gc

# COMMAND ----------

def get_path(*args):
  return os.path.join(*args)

def get_sloc(item):
  return item.replace('.','/')

def get_fname(item):
  return item.replace('.','_')

def get_bucketlink(bucket_name):
  return 's3://'+ bucket_name + '/'

# COMMAND ----------

def addto_Tracker(
    self
    ,object_type: str
    ,object_name: str
    ,object_loc: str
    ,object_ddl_CREATE: str
    ,object_ddl_DROP: str
    ,**kwargs
    )->pd.DataFrame:
    object_properties = kwargs.get('object_properties')
    self.objects_tracker = pd.concat([
        self.objects_tracker
        ,pd.DataFrame([{
            "object_type": object_type
            ,"object_name": object_name
            ,"object_loc": object_loc
            ,"object_ddl_CREATE": object_ddl_CREATE
            ,"object_ddl_DROP": object_ddl_DROP
            ,"object_properties": object_properties
            }])
    ], ignore_index=True)
    return self.objects_tracker

def get_sortedTracker(
    objects_tracker
    ,list_of_objects
    ,AscFlag: bool = True
    )->pd.DataFrame:
    sortedTracker = objects_tracker.copy()
    sortedTracker["object_type"] = pd.Categorical(
        sortedTracker["object_type"]
        ,categories=list_of_objects
        ,ordered=True
        )
    sortedTracker.sort_values(["object_type","object_name"],ascending=AscFlag,inplace=True)
    return sortedTracker

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

def yaml_to_schema(yaml):
    schema = '('
    for col in yaml['columns']:
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

def remove_script(file_name):
    os.remove(file_name)
    print(f"Script removed: '{file_name}'")

def generate_CREATE_scripts(object_type,object_name,object_loc,object_ddl,**kwargs):
    host = kwargs.get('host')
    token = kwargs.get('token')
    bucket_arn = kwargs.get('bucket_arn')
    el_sc = kwargs.get('el_sc')
    table_schema = kwargs.get('table_schema')

    if object_type == 'storage_credential':
        script = f"""
ws = WorkspaceClient(host='{host}',token = '{token}')
try:
    ws.storage_credentials.create(
        name='{object_name}',
        aws_iam_role=AwsIamRole(role_arn='{bucket_arn}')
        )
    print(f"\\nSuccess: CREATE {object_type} '{object_name}': \\n'{object_loc}'")
except Exception as e:
    print(f"\\nFailure: CREATE {object_type} '{object_name}': \\n{{e}}")
"""
    elif object_type == 'external_location':
        script = f"""
ws = WorkspaceClient(host='{host}',token = '{token}')
try:
    ws.external_locations.create(
        name='{object_name}'
        ,url='{object_loc}'
        ,credential_name='{el_sc}'
        )
    print(f"\\nSuccess: CREATE {object_type} '{object_name}': \\n'{object_loc}'")
except Exception as e:
    print(f"\\nFailure: CREATE {object_type} '{object_name}': \\n{{e}}")
"""
    elif object_type in ['CATALOG','DATABASE','TABLE']:
        script_ddl_create = f"CREATE {object_type} {object_name}"
        if object_type =='TABLE'and table_schema:
            script_ddl_schema = f"\n{table_schema}"
            script_ddl_location = f"\nLOCATION '{object_loc}';\n"
        if object_type in ['CATALOG','DATABASE']:
            script_ddl_schema = ''
            script_ddl_location = f"\nMANAGED LOCATION '{object_loc}';\n"
        script_ddl = script_ddl_create+script_ddl_schema+script_ddl_location
        script = f"""
try:
    spark.sql(f\"\"\"{script_ddl}\"\"\")
    print(f"\\nSuccess: CREATE {object_type} '{object_name}': \\n'{object_loc}'")
except Exception as e:
    print(f"\\nFailure: CREATE {object_type} '{object_name}': \\n{{e}}")
"""
    save_script(object_ddl,script)
    print(f"\nBelow script generated to CREATE {object_type} '{object_name}'. \n'{object_ddl}'")

def generate_DROP_scripts(object_type,object_name,object_ddl,**kwargs):
    host = kwargs.get('host')
    token = kwargs.get('token')

    if object_type == 'storage_credential':
        script = f"""
ws = WorkspaceClient(host='{host}',token = '{token}')
try:
    ws.storage_credentials.delete(name='{object_name}')
    print(f"\\nSuccess: DROP {object_type} '{object_name}'")
except Exception as e:
    print(f"\\nFailure: DROP {object_type} '{object_name}': \\n{{e}}")
"""
    elif object_type == 'external_location':
        script = f"""
ws = WorkspaceClient(host='{host}',token = '{token}')
try:
    ws.external_locations.delete(name='{object_name}')
    print(f"\\nSuccess: DROP {object_type} '{object_name}'")
except Exception as e:
    print(f"\\nFailure: DROP {object_type} '{object_name}': \\n{{e}}")
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
    print(f"\\nSuccess: DROP {object_type} '{object_name}'")
except Exception as e:
    print(f"\\nFailure: DROP {object_type} '{object_name}': \\n{{e}}")
"""
    save_script(object_ddl,script)
    print(f"\nBelow script generated to DROP {object_type} '{object_name}'. \n'{object_ddl}'")

# COMMAND ----------

#print
