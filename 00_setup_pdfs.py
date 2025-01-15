# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # DEMO PDFs Creation
# MAGIC
# MAGIC This is some simple reference code to generate from pdf files that we can test with. PDFs will not have the complexity of typical use cases, but are needed to show how reference code for ingest works. In this notebook we will write out a couple pdfs and store in Unity Catalog. This repo assumes that source documents will be using [Databricks Unity Catalog](https://docs.databricks.com/en/volumes/index.html#what-are-unity-catalog-volumes).
# MAGIC
# MAGIC To make variables consistent across notebooks, we'll use [%run](https://docs.databricks.com/en/notebooks/notebook-workflows.html#use-run-to-import-a-notebook) with configs save inline in the <a href="$./_setup/config" target="_blank">_setup/config</a> notebook.

# COMMAND ----------

# DBTITLE 1,Install Dependencies
# MAGIC %pip install fpdf

# COMMAND ----------

# DBTITLE 1,Set Configs
# MAGIC %run ./_setup/config

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **create_pdf_from_list** is a simple local function that will allow use to generate some sample pdf files and save them directly to Unity Catalog.
# MAGIC
# MAGIC We'll then create two examples and save them to the path `PDF_VOLUME_PATH` defined in <a href="$./_setup/config" target="_blank">_setup/config</a>.

# COMMAND ----------

# DBTITLE 1,Define create_pdf_from_list
from fpdf import FPDF

# Function to convert a list of strings into a PDF
def create_pdf_from_list(string_list, output_file):
    """
    Converts a list of strings into a PDF file.
    
    Args:
        string_list (list): List of strings to include in the PDF.
        output_file (str): Path to save the generated PDF.
    """
    # Initialize the PDF object
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", size=12)
    
    # Add each string to the PDF
    for line in string_list:
        pdf.multi_cell(0, 10, line)  # 0 width for automatic wrapping, 10 for line height
    
    # Save the PDF
    pdf.output(output_file)
    print(f"PDF successfully created: {output_file}")

# COMMAND ----------

# DBTITLE 1,Example File #1
# Example usage
example_strings = [
    "This is the first line of the PDF.",
    "Here is another line, showcasing the capabilities of this script.",
    "You can add as many lines as you want!",
    "Each line will be added to the PDF in the order provided."
]

output_pdf_path = f"{PDF_VOLUME_PATH}/file1.pdf"
create_pdf_from_list(example_strings, output_pdf_path)

# COMMAND ----------

# DBTITLE 1,Example File #2
# Example usage
example_strings = [
"Some are born great, some achieve greatness, and some have greatness thrust upon them.",
"All the world's a atage, and all the men and women merely players; They have their exits and their entrances; And one man in his time plays many parts.",
"Love looks not with the eyes, but with the mind, And therefore is winged Cupid painted blind.",
"Cowards die many times before their deaths; The valiant never taste of death but once."]

output_pdf_path = f"{PDF_VOLUME_PATH}/file2.pdf"
create_pdf_from_list(example_strings, output_pdf_path)
