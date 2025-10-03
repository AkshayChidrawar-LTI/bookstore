import os
from databricks.sdk import WorkspaceClient

def get_path(*args):
  return os.path.join(*args)

def get_sloc(item):
  return item.replace('.','/')

def get_fname(item):
  return item.replace('.','_')

def get_bucketlink(bucket_name):
  return 's3://'+ bucket_name + '/'

def create_Workspace(host,token):
  workspace = WorkspaceClient(host=host,token=token)
  return workspace