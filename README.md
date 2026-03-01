# ETL Process task

## Introduction
Hello!
My name is Kristiina Marie Palu, and I am submitting my task for the SEB's FX and Derivatives team internship.
I hope my submission matches your expectations, and I am eager to hear feedback from you. 

## Overview of the task
I created an ETL pipeline that extracts the Euro foreign exchange reference rates from the 
ECB, transforms the data, and loads the cleaned data into a Markdown file. 

## Why I used my approach

### `def extract_csv_from_zip`
* I used pandas built in capability to handle the decompression automatically
* Error handling is in the form of try-except to handle any network issues.

### `def transform`
* For currency filtering, I used pandas column selection with the `CURRENCY_CODES` list
* The daily rates uses `iloc[0]` to transform the 2D DataFrame to a 1D Series for the Markdown table.
* The mean calculation uses `mean(skipna=True)` to handle any missing values

### `def load`
* I chose a Markdown file for the exchange rates table

## How to run
* Open the terminal and type `pip install -r requirements.txt`
* Then `python etl_pipeline.py`

# AI usage
I refrained from using AI for most of the task. Instead, I used Google, StackOverFlow, Medium and W3Schools.
I did use ChatGPT when I finished my assignment to check if I had understood the task correctly and to see if there
were any big flaws I hadn't noticed. Here is the link to the question I asked.
https://chatgpt.com/share/69a43927-fe50-8010-a1aa-b10ed7ee8f70