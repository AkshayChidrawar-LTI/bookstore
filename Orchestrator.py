# Databricks notebook source
# MAGIC %run /Workspace/Users/akshay.chidrawar@ltimindtree.com/bookstore/dependencies/Designer

# COMMAND ----------

bookstore = ProjectSetup(
    ProjectName='bookstore'
    ,repository_file='/Workspace/Users/akshay.chidrawar@ltimindtree.com/bookstore/config/repository.yml'
    ,metadata_file = '/Workspace/Users/akshay.chidrawar@ltimindtree.com/bookstore/config/metadata.yml'
    ,logFileName = '/Workspace/Users/akshay.chidrawar@ltimindtree.com/bookstore/logs/bookstore.log'
)

# COMMAND ----------

bookstore.GenerateScripts()
display(bookstore.objects_tracker)

# COMMAND ----------

bookstore.CreateObjects()

# COMMAND ----------

bookstore.DropObjectsAndScripts()
del bookstore
gc.collect()
