# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # pytesseract_batch_simple
# MAGIC
# MAGIC This notebook will read all files in our source volumes directory and create a delta table with all of the text as a field. The notebook is organized as follows:
# MAGIC  - Install Dependencies
# MAGIC  - Set Configs
# MAGIC  - Define python functions
# MAGIC  - Define spark dataframes from functions
# MAGIC  - Save pdfs as string to delta from spark dataframes
# MAGIC
# MAGIC  **NOTE**: This is the simple implementation because it only has the extraction of text. However, this pattern can be extended further with additional transforms that can run genai functions on the extracted text.

# COMMAND ----------

# DBTITLE 1,Install Dependencies
# MAGIC %pip install pytesseract==0.3.10 PyMuPDF==1.23.25
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Set Configs
# MAGIC %run ./_setup/config

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Define python functions
# MAGIC
# MAGIC
# MAGIC We'll define the following functions to be used for the file ingestion. Many of these could be combined together, consider changes that improve readbility or will better align with intended unit testing:
# MAGIC  - `pdf_to_png` - uses [PyMuPDF](https://pypi.org/project/PyMuPDF/) to convert PDF file into list of bytes 
# MAGIC  - `image_to_b64string` - converts bytes into string, a intermediate form that can be used in a spark dataframe
# MAGIC  - `b64string_to_image` - a convenience method for evaluating images, not used in the batch transform
# MAGIC  - `split_pages` - **pandas_udf** creates a list of pages as jpeg which is an accepted format for [pytesseract](https://pypi.org/project/pytesseract/)
# MAGIC  - `extract_text` - **pandas_udf** will leverage the pytesseract function `image_to_string` to produce an unmodified output as string from Tesseract OCR processing

# COMMAND ----------

# DBTITLE 1,Image Handling Functions
import fitz
import io
import base64
from PIL import Image

import base64
from io import BytesIO

def pdf_to_png(pdf_name):
  return_pngs = []
  with fitz.open(pdf_name) as doc:
        for i, page in enumerate(doc):
          page = doc.load_page(i)
          pixmap = page.get_pixmap(dpi=300)
          img = Image.open(io.BytesIO(pixmap.tobytes()))
          return_pngs.append(img)
  return return_pngs

def image_to_b64string(image: Image, format:str = "JPEG", encoding:str = "utf-8") -> str:
  buffered = BytesIO()
  image.save(buffered, format=format)
  b64bytes = base64.b64encode(buffered.getvalue())
  return b64bytes.decode(encoding)

def b64string_to_image(b64_img_str:str, encoding:str = "utf-8") -> Image:
  return Image.open(BytesIO(base64.b64decode(bytes(b64_img_str, encoding))))

# COMMAND ----------

# DBTITLE 1,pandas_udf functions
#create a pandas_udf to get pages
from pyspark.sql.functions import pandas_udf, explode, col
from typing import Iterator
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructType, StructField
import pandas as pd

return_schema = ArrayType(
    StructType([
        StructField("page_number", IntegerType()),
        StructField("page_encoded_img", StringType())
    ])
)

@pandas_udf(returnType=return_schema)
def split_pages( batches : Iterator[pd.Series]) -> Iterator[pd.Series]:
    for batch in batches:
        #batch -> list of filenames
        batch_pages = []
        for file_name in batch.tolist():    
            pages = pdf_to_png(file_name)
            file_pages = []
            for i in range(len(pages)):
                img_str = image_to_b64string(pages[i])
                page_dict = {
                    "page_number":i+1,
                    "page_encoded_img": img_str
                }
                file_pages.append(page_dict)

            batch_pages.append(file_pages)

        yield pd.Series(batch_pages)

import pytesseract

@pandas_udf(returnType=StringType())
def extract_text(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
    for batch in batches:
        #batch -> list of page img str
        batch_result = []
        for page_img_str in batch.tolist():     
            page_img = b64string_to_image(page_img_str)
            result = pytesseract.image_to_string(page_img)        
            batch_result.append(result)
        yield pd.Series(batch_result)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Define spark dataframes
# MAGIC
# MAGIC While a single spark dataframe could be defined for the whole process, we will include intermediate definitions for ease of inspection and debugging. Those dataframes are defined in the following order:
# MAGIC
# MAGIC | name | description |
# MAGIC | ---- | ----------- |
# MAGIC | `paths_df` | A lisiting of all the pdf files that will be read into text. For batch, we'll use [dbutils](https://docs.databricks.com/en/dev-tools/databricks-utils.html). |
# MAGIC | `pages_df` | A dataframe of all pdfs read into jpeg as string and exploded by page (meaning one record per page) |
# MAGIC | 'df' | desc |

# COMMAND ----------

# DBTITLE 1,paths_df
paths_lst = [file.path.replace("dbfs:","") for file in dbutils.fs.ls(PDF_VOLUME_PATH)]
paths_df = spark.createDataFrame(pd.DataFrame({ "path" : paths_lst}))

display(paths_df)

# COMMAND ----------

# DBTITLE 1,pages_df
from pyspark.sql.functions import col

pages_df = paths_df.withColumn("pages", split_pages("path")) \
                   .withColumn("page", explode("pages")) \
                   .drop("pages")

display(pages_df)

# COMMAND ----------

text_df = pages_df.withColumn("content",extract_text("page.page_encoded_img")) \
                  .select("path",
                          col("page.page_number").alias("page_number"),
                          "content")

display(text_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Save Dataframe to Delta
# MAGIC
# MAGIC Now we can save the entire Dataframe to delta to have it persisted. It is not necessary to stop here, we could keep adding additional transforms.

# COMMAND ----------

text_df.write.mode("overwrite").saveAsTable(PDF_TABLE_NAME)

display(spark.sql(f"SELECT * FROM {PDF_TABLE_NAME}"))
