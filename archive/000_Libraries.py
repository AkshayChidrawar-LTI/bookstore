# Databricks notebook source
from databricks.sdk.service.catalog import AwsIamRole
from databricks.sdk import WorkspaceClient
import pandas as pd
import pyspark.sql.functions as F
from urllib.parse import urlparse
import json
import re
# !pip install openpyxl