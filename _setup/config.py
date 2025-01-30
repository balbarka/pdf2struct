# Databricks notebook source
# DBTITLE 1,Install Dependencies
# MAGIC %pip install pytesseract==0.3.10 PyMuPDF==1.23.25
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Note these resources need to be set up manually, creation is not included in ref code

UC_CATALOG = "main"
UC_SCHEMA = "default"
UC_VOLUME = "pdf_source"
CP_VOLUME = "pdf_checkpoint"
UC_TABLE = "pdf_content"
UC_TABLE_STREAMING = "pdf_content_streaming"
PDF_VOLUME_PATH = f"/Volumes/{UC_CATALOG}/{UC_SCHEMA}/{UC_VOLUME}"
CP_VOLUME_PATH = f"/Volumes/{UC_CATALOG}/{UC_SCHEMA}/{CP_VOLUME}"
PDF_TABLE_NAME = f"{UC_CATALOG}.{UC_SCHEMA}.{UC_TABLE}"
PDF_STREAMING_TABLE_NAME = f"{UC_CATALOG}.{UC_SCHEMA}.{UC_TABLE_STREAMING}"
