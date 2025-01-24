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
    "A long time ago there was an emprire in the midst of conflict.",
    "I man named Luke Skywalker showed up to fight on the rebellion side of this conflict.",
    "He carried a light saber and fired a blaster."
]

output_pdf_path = f"{PDF_VOLUME_PATH}/file1.pdf"
create_pdf_from_list(example_strings, output_pdf_path)

# COMMAND ----------

# DBTITLE 1,Example File #2
# Example usage
example_strings = [
"Kittens are cute and cuddly.",
"There was a young woman named Molly Kittens who love all kinds of kittens.",
"She started a kitten hospital that everyone liked."]

output_pdf_path = f"{PDF_VOLUME_PATH}/file2.pdf"
create_pdf_from_list(example_strings, output_pdf_path)

# COMMAND ----------

from fpdf import FPDF

# Function to convert a list of lists of strings into a multi-page PDF
def create_pdf_from_nested_list(nested_string_list, output_file):
    """
    Converts a list of lists of strings into a multi-page PDF file.

    Args:
        nested_string_list (list): List of lists of strings, where each sublist corresponds to a page.
        output_file (str): Path to save the generated PDF.
    """
    # Initialize the PDF object
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.set_font("Arial", size=12)

    # Loop through each sublist to create separate pages
    for page_content in nested_string_list:
        pdf.add_page()

        # Add each line from the current page's content
        for line in page_content:
            pdf.multi_cell(0, 10, line)  # 0 width for automatic wrapping, 10 for line height

    # Save the PDF
    pdf.output(output_file)
    print(f"PDF successfully created: {output_file}")

# Example usage
nested_content = [
    ["Peter Pan was a boy who wouldn't grow up.", "He spent some time in a place called never never land."],
    ["Peter Pan could fly.", "He had a friend who could also fly."],
    ["Tiny Tim was a poor boy in england.", "He may have been a chimney sweep, but I'm not sure."],
    ["Tiny Tim had crutches.", "He liked to play poker."],
]

create_pdf_from_nested_list(nested_content, f"{PDF_VOLUME_PATH}/file3.pdf")

# COMMAND ----------


