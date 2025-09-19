# Databricks notebook source
# MAGIC %run ./001_Functions

# COMMAND ----------

FileName_DWH = '/Workspace/Users/akshay.chidrawar@ltimindtree.com/PrashantPandey/DWH_details.xlsx'

Setup(FileName_DWH)


# COMMAND ----------

class Config():
    def __init__(self):
        self.dir_raw = ''
        self.dir_checkpoint = ''
        self.db_name = ''