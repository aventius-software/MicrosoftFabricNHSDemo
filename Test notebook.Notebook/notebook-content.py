# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2c074247-8225-4557-9291-126b8442b5f4",
# META       "default_lakehouse_name": "LandingLakehouse",
# META       "default_lakehouse_workspace_id": "7d297a63-edac-48fc-9875-65dbf95464db"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col

def clean_column_names(data_frame, replace_characters = True):
    #new_column_name_list= list(map(lambda x: x.replace(" ", "_"), df.columns))
    #df = df.toDF(*new_column_name_list)

    # Are we replacing characters with alternatives?    
    if replace_characters:
        # Replace invalid characters with valid ones
        df = (
            data_frame.select([col(column_name)
                .alias(column_name
                    .replace(" ", "_")            
                    .replace("-", "_")
                    .replace("&", "and")
                ) for column_name in data_frame.columns]
            )
        )

    # No, we're just removing the characters only!
    else:
        # Only remove the characters, don't replace them with anything
        df = (
            data_frame.select([col(column_name)
                .alias(column_name
                    .replace(" ", "")            
                    .replace("-", "")
                    .replace("&", "")
                ) for column_name in data_frame.columns]
            )
        )

    return df

from pyspark.sql import functions as F

def read_csv(path, schema = None, header = True, sep = ",", quote = '"', escape = "\\", infer_schema = True):
    df = (
        spark.read.csv(
            path=path,
            schema=schema,
            header=header,
            sep=sep,
            quote=quote,
            escape=escape,
            inferSchema=infer_schema,
        )
        .withColumn("DateTimeLoaded", F.current_timestamp())
        .withColumn("InputFileName", F.input_file_name())
    )

    return clean_column_names(df)

df = read_csv("Files/etr.csv", schema = None, header = False)
df.show(10)

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
#df = spark.read.csv("/lake".option("headers", True)

df = spark.read.csv("/lakehouse/default/Files/")

# CELL ********************

#df = spark.read.format("csv").option("header","true").load("Files/referral_to_treatment/unzipped_files/Full-CSV-data-file-Jan24-ZIP-3702K-55749-1.zip/20240131-RTT-January-2024-full-extract.csv")
df = spark.read.format("csv").option("header","true").load("/lakehouse/default/Files/referral_to_treatment/unzipped_files/Full-CSV-data-file-Jan24-ZIP-3702K-55749-1.zip/20240131-RTT-January-2024-full-extract.csv")
display(df.limit(10))

# CELL ********************

import os.path
from os import path

#directory = "Files/referral_to_treatment/zipped_files/Full-CSV-data-file-Jan24-ZIP-3702K-55749-1.zip" # Doesn't work
#directory = "/lakehouse/default/Files/referral_to_treatment/zipped_files/Full-CSV-data-file-Jan24-ZIP-3702K-55749-1.zip" # Works
directory = "/LandingLakehouse/Files/referral_to_treatment/zipped_files/Full-CSV-data-file-Jan24-ZIP-3702K-55749-1.zip" # Works
#directory = "abfss://7d297a63-edac-48fc-9875-65dbf95464db@onelake.dfs.fabric.microsoft.com/2c074247-8225-4557-9291-126b8442b5f4/Files/referral_to_treatment/zipped_files/Full-CSV-data-file-Jan24-ZIP-3702K-55749-1.zip"

if path.exists(directory):
    print("exists")
else:
    print("doesn't exist")

    #Files/referral_to_treatment/zipped_files/Full-CSV-data-file-Jan24-ZIP-3702K-55749-1.zip
    #/lakehouse/default/Files/referral_to_treatment/zipped_files/Full-CSV-data-file-Jan24-ZIP-3702K-55749-1.zip

# CELL ********************

import os.path

def path_exists(path_to_check):
    # If its a relative path from the files use method 1 where 
    # we need to use the ugly 'hack' involving 'ls' since os.path.exists 
    # doesn't work with relative paths for spark, only the 'default' lakehouse paths
    if path_to_check.startswith("Files/"):
        try:
            # If we can list the path, obviously it exists. If it doesn't then this
            # throws an exception which we can catch below ;-)
            mssparkutils.fs.ls(path_to_check)
            return True
        except:
            # If the path doesn't exist, we just catch the exception thrown above
            # and now we know that the path doesn't exist
            return False
    
    # Otherwise we can just use the os.path method ;-)
    else:
        return os.path.exists(path_to_check)

# Test relative paths
directory = "Files/some_file.csv" # Relative path for spark

if path_exists(directory):
    print("exists")
else:
    print("doesn't exist")

# Test the other way
directory = "/lakehouse/default/Files/some_file.csv" # Default lakehouse path

if path_exists(directory):
    print("exists")
else:
    print("doesn't exist")

# CELL ********************

Relative path for Spark = Files/referral_to_treatment/zipped_files/Full-CSV-data-file-Dec23-ZIP-3569K-58522.zip
File API path = /lakehouse/default/Files/referral_to_treatment/zipped_files/Full-CSV-data-file-Dec23-ZIP-3569K-58522.zip

# CELL ********************

# Adds the default metadata fields to the start of a data frame
#
# Example test code:
#
# from datetime import datetime, date
# from pyspark.sql import Row
#
# df = spark.createDataFrame([
#    Row(a=1, b=4., c='GFG1', d=date(2000, 8, 1), e=datetime(2000, 8, 1, 12, 0)),   
#    Row(a=2, b=8., c='GFG2', d=date(2000, 6, 2), e=datetime(2000, 6, 2, 12, 0)),   
#    Row(a=4, b=5., c='GFG3', d=date(2000, 5, 3), e=datetime(2000, 5, 3, 12, 0))
# ])
#
# new_df = add_metadata_fields_to_data_frame(df)
# new_df.show()

def add_metadata_fields_to_data_frame(df):
    from pyspark.sql.types import TimestampType
    from pyspark.sql.functions import lit, current_timestamp

    return df.select(
        # Add a 'DateTimeLoaded' column at the start, with the value
        # of current timestamp value (i.e. the current date and time)
        lit(current_timestamp()).cast(TimestampType()).alias("DateTimeLoaded"),

        # Now add another column called 'DateTimeUpdated', but we only 
        # need it be NULL for the moment
        lit(None).cast(TimestampType()).alias("DateTimeUpdated"),

        # Now, all the other existing columns come after ;-)
        "*")


from datetime import datetime, date
from pyspark.sql import Row

df = spark.createDataFrame([
    Row(a=1, b=4., c='GFG1', d=date(2000, 8, 1), e=datetime(2000, 8, 1, 12, 0)),   
    Row(a=2, b=8., c='GFG2', d=date(2000, 6, 2), e=datetime(2000, 6, 2, 12, 0)),   
    Row(a=4, b=5., c='GFG3', d=date(2000, 5, 3), e=datetime(2000, 5, 3, 12, 0))
])

new_df = add_metadata_fields_to_data_frame(df)
new_df.show()

# CELL ********************

from pyspark.sql import functions as F

df = (
    spark.read.csv("some file.csv")
        .withColumn("DateTimeLoaded", F.current_timestamp())
        .withColumn("InputFileName", F.input_file_name())
)

# CELL ********************

#from pyspark.sql import SparkSession
import requests
import os

# Create a SparkSession
#spark = SparkSession.builder.appName("File Downloader").getOrCreate()

# Define the URL of the website you want to scrape
url = "https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/rtt-data-2023-24/"

# Use the `requests` library to get the HTML content of the webpage
response = requests.get(url)

# Parse the HTML content using BeautifulSoup
from bs4 import BeautifulSoup
soup = BeautifulSoup(response.text, 'html.parser')

# Find all links on the page that point to files (e.g. PDFs, CSVs, etc.)
file_links = []
for link in soup.find_all('a'):
    href = link.get('href')
    #if href and href.endswith(('.pdf', '.csv', '.zip', '.txt')):
    if href and href.endswith('.zip'):
        obj = dict(link_url = href, link_text = link.get_text())
        #obj["text"] = link.get_text()
        #file_links.append(href)
        file_links.append(obj)

print(file_links)
# Create a Spark DataFrame to store the links
#links_df = spark.createDataFrame(file_links, ['link'])

#links_df.show().limit(10)
# Download each file linked on the page
#for row in links_df.collect():
#    link = row['link']
#    filename = os.path.basename(link)
#    response = requests.get(link, stream=True)
#    with open(filename, 'wb') as f:
#        for chunk in response.iter_content(1024):
#            f.write(chunk)

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from bs4 import BeautifulSoup
import requests

# Function to scrape data from a single URL
def scrape_url(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            # Extract data from the soup object based on HTML structure
            # (Replace this with your specific scraping logic)
            link = soup.find_all("a")#.text
            href = link.get('href')
            return data
        else:
            print(f"Failed to retrieve data from {url}. Status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error scraping data from {url}: {str(e)}")
        return None

# UDF (User Defined Function) for parallelized scraping
#scrape_udf = udf(scrape_url, StringType())

#if __name__ == '__main__':
    # Initialize Spark session
    #spark = SparkSession.builder.appName("WebScrapingWithPySpark").getOrCreate()

    # List of URLs to scrape (replace with your own URLs)
#url_list = ['https://example.com/page1', 'https://example.com/page2', 'https://example.com/page3']

# Create a DataFrame from the list of URLs
#url_df = spark.createDataFrame([(url,) for url in url_list], ["url"])

# Apply the scraping UDF to each URL in parallel
#results_df = url_df.withColumn("scraped_data", scrape_udf("url"))

# Display the results
#results_df.show(truncate=False)

# Stop Spark session
#spark.stop()

# CELL ********************

def scape_url_for_file_links(url_to_scrape, file_extension_filter = (".csv", ".zip", ".pdf", ".txt")):    
    # Use the requests library to get the HTML content of the webpage
    import requests
    response = requests.get(url)

    # Parse the HTML content using BeautifulSoup
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(response.text, "html.parser")

    # Find all links on the page that point to files
    file_links = []

    for link in soup.find_all("a"):
        # Find the href attribute in the link
        href = link.get("href")

        # Filter for only the extensions we want
        if href and href.endswith(file_extension_filter):        
            obj = dict(link_url = href, link_text = link.get_text())            
            file_links.append(obj)

    return file_links


# Define the URL of the website you want to scrape
url = "https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/rtt-data-2022-23/"
links = scape_url_for_file_links(url, (".zip"))

print(links)
