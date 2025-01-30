# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # pytesseract_incremental_batch
# MAGIC
# MAGIC This notebook will read all files in our source volumes directory and create a delta table with all of the text as a field. The notebook is organized as follows:
# MAGIC  * **Install Dependencies & Set Configs**: Called using **%run**
# MAGIC  * **Define python functions**: Left inline in the notebook during dev, but often imported from another source for reusability
# MAGIC  * **Define spark dataframes**: All dataframes including intermediates are defined. There is no runtime cost to defining intermediates if there isn't an action on them.
# MAGIC  * **Save pdfs as string to delta**: In a job, this would be the only action on a dataframe and is how we persist the transform.
# MAGIC
# MAGIC  **NOTE**: This is the simple implementation because it only has the extraction of text. However, this pattern can be extended further with additional transforms that can run genai functions on the extracted text.

# COMMAND ----------

# DBTITLE 1,Install Dependencies & Set Configs
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

import pytesseract

# This this is different than how implemented in batch because it will read the pdf from content
# instead of open the file

def pdf_to_png(pdf_binary):
  return_pngs = []
  doc = fitz.open("pdf", pdf_binary)
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
        for pdf_binary in batch.tolist():    
            pages = pdf_to_png(pdf_binary)
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

# DBTITLE 1,ai_query function
# We are going to write our AI call taking advantage of ai_query - the intent here is that databricks is making an investment in optimizing ai_query batch performance
# This will greately simplify application tuning, but to take advantage of this, we'll want to make our LLM calls at the top column level and from within a pandas UDF
# This appraoch is being taken to greatly reduce the complexity and maintainability effort of a workflow
# Since there isn't currently a pyspark col function for ai_query, we'll write our own

from pyspark.sql.functions import expr, from_json
from pyspark.sql.types import StructType, StructField, StringType

# Define the schema of the JSON string
json_extract_schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("lastname", StringType(), True)
])

def extract_names_expr(context_col: str="content",
                       endpoint: str = "databricks-meta-llama-3-3-70b-instruct") -> str:
    prompt = "In the following CONTEXT there is a primary character. Identify the primary character. Return only the primary character's name using only json in the following format : {'firstname': [First Name], 'lastname': [Last Name]}. \n\nCONTEXT: "
    request = f"CONCAT(\"{prompt}\", {context_col})"
    ai_query_expresssion =  f"ai_query('{endpoint}', \n{request})"
    return ai_query_expresssion

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Set Incremental Batch Configurations
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

print("PDF_VOLUME_PATH: " + PDF_VOLUME_PATH)
print("CP_VOLUME_PATH: " + CP_VOLUME_PATH)
print("PDF_STREAMING_TABLE_NAME: " + PDF_STREAMING_TABLE_NAME)

# COMMAND ----------

from pyspark.sql.functions import input_file_name

PDF_VOLUME_PATH = "/Volumes/main/default/pdf_source"
CP_VOLUME_PATH = "/Volumes/main/default/pdf_checkpoint"
PDF_STREAMING_TABLE_NAME = "main.default.pdf_content_streaming"

pytesseract.pytesseract.tesseract_cmd = "/usr/bin/tesseract" 

# Read PDF files as a stream from UC Volume
df = (
    spark.readStream
    .format("cloudFiles")  # Auto Loader
    .option("cloudFiles.format", "binaryFile")  # Read as binary files
    .load(PDF_VOLUME_PATH)
    .withColumn("file_path", input_file_name())  # Capture file path
    .withColumn("pages", split_pages("content"))
    .withColumn("page", explode("pages"))
    .withColumn("page_number", col("page.page_number"))
    .withColumn("content",extract_text("page.page_encoded_img"))
    .withColumn("extract", from_json(expr(extract_names_expr()), json_extract_schema))
    .drop("pages").drop("page")
)

query = (
    df.writeStream
    .format("delta")
    .outputMode("append")  # Append-only mode
    .option("checkpointLocation", CP_VOLUME_PATH)  # Required for fault tolerance
    .trigger(once=True)  # Process all available data and then stop
    .toTable(PDF_STREAMING_TABLE_NAME)  # Write to Delta table
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM main.default.pdf_content_streaming;
