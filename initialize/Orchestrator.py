# Databricks notebook source
# MAGIC %run /Workspace/Users/akshay.chidrawar@ltimindtree.com/bookstore/dependencies/Designer

# COMMAND ----------

bookstore = ProjectSetup(
    'bookstore'
    ,repository_file='/Workspace/Users/akshay.chidrawar@ltimindtree.com/bookstore/config/repository.yml'
    ,metadata_file = '/Workspace/Users/akshay.chidrawar@ltimindtree.com/bookstore/config/metadata.yml'
)

# COMMAND ----------

bookstore.GenerateScripts()
display(bookstore.get_ObjectsTracker(AscFlag=False))

# COMMAND ----------

bookstore.CreateObjects()

# COMMAND ----------

bookstore.DropObjects()
del bookstore
gc.collect()