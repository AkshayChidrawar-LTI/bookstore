# Databricks notebook source
# MAGIC %run /Workspace/Users/akshay.chidrawar@ltimindtree.com/bookstore/dependencies/Designer

# COMMAND ----------

bookstore = ProjectSetup(
    ProjectName='bookstore'
    ,repository_file='/Workspace/Users/akshay.chidrawar@ltimindtree.com/bookstore/config/repository.yml'
    ,metadata_file = '/Workspace/Users/akshay.chidrawar@ltimindtree.com/bookstore/config/metadata.yml'
)

# COMMAND ----------

bookstore.ScriptGenerator().GenerateScripts()
display(spark.sql(f"select * from {bookstore.ProjectManager.tbl_ObjectsTracker}"))

# COMMAND ----------

bookstore.ScriptExecutor().Setup()
display(spark.sql(f"select * from workspace.default.objects_tracker"))

# COMMAND ----------

bookstore.ProjectM().Cleanup()
display(spark.sql(f"select * from workspace.default.objects_tracker"))

# COMMAND ----------

bookstore.ProjectManager.Purge()
del bookstore
gc.collect()
