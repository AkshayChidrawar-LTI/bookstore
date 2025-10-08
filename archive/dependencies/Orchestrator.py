# Databricks notebook source
# MAGIC %run /Workspace/Users/akshay.chidrawar@ltimindtree.com/bookstore/dependencies/Designer

# COMMAND ----------

bookstore = ProjectSetup(
    ProjectName='bookstore'
    ,repository_file='/Workspace/Users/akshay.chidrawar@ltimindtree.com/bookstore/config/repository.yml'
    ,metadata_file = '/Workspace/Users/akshay.chidrawar@ltimindtree.com/bookstore/config/metadata.yml'
)

# COMMAND ----------

bookstore.ScriptGenerator.GenerateScripts()
display(bookstore.ProjectManager.ObjectsTracker.getData_objects_tracker())
bookstore.ProjectManager.Setup()
display(bookstore.ProjectManager.ObjectsTracker.getData_objects_tracker())

display(bookstore.ProjectManager.ObjectsTracker.getData_objects_tracker())

# COMMAND ----------

bookstore.ProjectManager.Cleanup()
bookstore.ProjectManager.Purge()
