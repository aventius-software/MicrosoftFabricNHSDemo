# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2c074247-8225-4557-9291-126b8442b5f4",
# META       "default_lakehouse_name": "LandingLakehouse",
# META       "default_lakehouse_workspace_id": "7d297a63-edac-48fc-9875-65dbf95464db",
# META       "known_lakehouses": [
# META         {
# META           "id": "2c074247-8225-4557-9291-126b8442b5f4"
# META         },
# META         {
# META           "id": "c3283c02-4fae-433e-b45f-a900e8be8237"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # This is a collection of shared functions which can be used by other notebooks

# CELL ********************

%run "Shared parameters"

# MARKDOWN ********************

# ## File and folder manipulation functions

# CELL ********************

# Returns boolean to indicate if path exists, note that you
# can use either 'Files/*' or '/lakehouse/default/Files/*'
# for this particular function!

def path_exists(path_to_check):
    import os.path

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

# CELL ********************

# Creates a folder in the lake if it doesn't already exist

def create_folder_if_not_exists(path):
    if not path_exists(path):
        mssparkutils.fs.mkdirs(path)

# CELL ********************

# Creates the relevant dataset folders in the current lakehouse if they're not already there

def create_dataset_folders(dataset_name):
    # Create the root dataset folder
    create_folder_if_not_exists(f"Files/{dataset_name}")

    # Create zipped/unzipped folders (variables are global from 'Shared Parameters' notebook!)
    create_folder_if_not_exists(f"Files/{dataset_name}/{zipped_folder_name}")
    create_folder_if_not_exists(f"Files/{dataset_name}/{unzipped_folder_name}")

# CELL ********************

# Get the file name from the specified path, but without the file extension

def get_file_name_without_extension(file_name):    
    parts = file_name.split(".")
    return parts[0]

# CELL ********************

# Some functions to help unzip files or streams

def unzip_file(zipped_file_path, path_to_unzip_files_to):
    import requests
    import zipfile

    # Get the zipped file
    zipped_file = zipfile.ZipFile(zipped_file_path)
    
    # Example zipped_file.extractall(f"/lakehouse/default/{path_to_unzip_files_to}/{file_name}")
    zipped_file.extractall(path_to_unzip_files_to)

def unzip_http_response_file_to_lake(http_response_stream, path_to_unzip_files_to):
    import requests
    import zipfile
    import io

    # Get the zipped file from the http response content stream
    zipped_file = zipfile.ZipFile(io.BytesIO(http_response_stream.content))
    
    # Example: zipped_file.extractall(f"/lakehouse/default/{unzipped_lake_path}/{file_name}")
    zipped_file.extractall(path_to_unzip_files_to)

# CELL ********************

# Some helpers to get zipped/unzipped/non-zipped file folder locations

def get_zipped_download_location(dataset_name, file_name):
    return f"Files/{dataset_name}/{zipped_folder_name}/{file_name}"

def get_unzipped_download_location(dataset_name, file_name):
    return f"Files/{dataset_name}/{unzipped_folder_name}/{file_name}"

def get_normal_download_location(dataset_name, file_name):
    return f"Files/{dataset_name}/{downloaded_files_folder_name}/{file_name}"

# CELL ********************

# Returns last part of a url if its a file name

def get_file_name_from_url(url):
    parts = url.split("/")    
    return parts[len(parts) - 1]

# MARKDOWN ********************

# ## Web and http functions

# CELL ********************

# Scrapes the specified url for file download links

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

#url = "https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/rtt-data-2022-23/"
#links = scape_url_for_file_links(url, (".zip"))
#print(links)

# CELL ********************

# Downloads a file from the specified url to the location specified in the 
# lake, also returns a copy of the http response for further action
#
# Test example:
#
# url = "https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2024/02/Full-CSV-data-file-Dec23-ZIP-3569K-58522.zip"
# location_to_write_file_to_in_lake = "/lakehouse/default/Files/referral_to_treatment/zipped_files/Full-CSV-data-file-Dec23-ZIP-3569K-58522.zip"
# download_file(url, location_to_write_file_to_in_lake)

def download_file(url, location_to_write_file_to_in_lake):
    import requests
    import pathlib 
    import io

    # Create the web request and try and get a response
    http_response = requests.get(url)

    # For debugging, print the location the file gets written to
    print(f"Starting download from '{url}'")
    print(f"Intended destination: '{location_to_write_file_to_in_lake}'")

    # Write a copy of the file to the lake    
    pathlib.Path(location_to_write_file_to_in_lake) \
        .write_bytes(io.BytesIO(http_response.content) \
            .getbuffer() \
            .tobytes())

    # For debugging, print the location the file gets written to
    print(f"Completed downloaded from '{url}'")
    print(f"Downloaded to: '{location_to_write_file_to_in_lake}'")

# MARKDOWN ********************

# ## List and array manipulation functions

# CELL ********************

# Some helper functions for lists

def remove_items_from_list(items, items_to_remove):
    # Does what it says, removes specific items from the specified list and returns new one
    return [value for value in items if value not in items_to_remove]

def get_differences_between_lists(values, values_to_compare, values_to_ignore):
    # Remove any values we want to ignore
    values = [x for x in values if x not in values_to_ignore]    
    values_to_compare = [x for x in values_to_compare if x not in values_to_ignore]    

    # First check for values that are NOT in the 'compare' list
    values_not_in_values_to_compare = [x for x in values if x not in values_to_compare]
    
    # Now check for values from the 'compare' list that are NOT in the original list
    values_to_compare_not_in_values = [x for x in values_to_compare if x not in values]
    
    # Combine the 2 lists
    values_not_in_values_to_compare.extend(values_to_compare_not_in_values)

    # Finally, return the extended list ;-)
    return values_not_in_values_to_compare

def are_different_lists(values, values_to_compare, values_to_ignore):
    return len(get_differences_between_lists(values, values_to_compare, values_to_ignore)) > 0

# MARKDOWN ********************

# ## Auditing and logging functions

# CELL ********************

def log_error(error_message):
    import logging
    
    # Customize the logging format for all loggers
    FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(fmt=FORMAT)
    
    for handler in logging.getLogger().handlers:
        handler.setFormatter(formatter)
    
    # Customize log level for all loggers
    #logging.getLogger().setLevel(logging.INFO)
    
    # Customize the log level for a specific logger
    #customizedLogger = logging.getLogger('customized')
    #customizedLogger.setLevel(logging.WARNING)
    
    # logger that use the default global log level
    defaultLogger = logging.getLogger('default')
    
    #defaultLogger.debug("default debug message")
    #defaultLogger.info("default info message")
    #defaultLogger.warning("default warning message")
    defaultLogger.error(error_message)
    #defaultLogger.critical("default critical message")
    
    # logger that use the customized log level
    #customizedLogger.debug("customized debug message")
    #customizedLogger.info("customized info message")
    #customizedLogger.warning("customized warning message")
    #customizedLogger.error("customized error message")
    #customizedLogger.critical("customized critical message")

# CELL ********************

# Logging functions

def log_download_to_audit_table(url, download_location):    
    (spark.sql("INSERT INTO audit_log (Url, DateTimeLoaded, DownloadLocation, DateTimeFilesProcessed) VALUES ({url}, CURRENT_TIMESTAMP, {download_location}, NULL)",
        url = url,
        download_location = download_location))

def log_download_has_been_processed(url):
    spark.sql("UPDATE audit_log SET DateTimeFilesProcessed = CURRENT_TIMESTAMP WHERE Url = {url}", url = url)

# CELL ********************

def build_control_tables():    
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
    
    # Check if the table already exists
    if not spark.catalog.tableExists(audit_table_name):
        # Specify the table fields
        table_schema = StructType([                                
            StructField("Url", StringType(), False),
            StructField("DateTimeLoaded", TimestampType(), False),
            StructField("DownloadLocation", StringType(), False),
            StructField("DateTimeFilesProcessed", TimestampType(), True)
        ])

        # Now create the table
        df = spark.createDataFrame([], table_schema)
        df.write.format("delta").saveAsTable(audit_table_name)

# MARKDOWN ********************

# ## Data and file import functions

# CELL ********************

# See https://medium.com/yipitdata-engineering/the-most-useful-pyspark-function-46febee5c6e3

def read_csv(path, schema = None, header = True, sep = ",", quote = '"', escape = "\\", infer_schema = True):
    from pyspark.sql import functions as F
    
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
        #.withColumn("DateTimeLoaded", F.current_timestamp())
        #.withColumn("InputFileName", F.input_file_name())
    )

    return clean_column_names(df)

# CELL ********************

# See https://medium.com/yipitdata-engineering/the-most-useful-pyspark-function-46febee5c6e3

def read_parquet(path):
    from pyspark.sql import functions as F
    
    df = (
        spark.read.parquet(path)
            #.withColumn("DateTimeLoaded", F.current_timestamp())
            #.withColumn("InputFileName", F.input_file_name())
    )

    return clean_column_names(df)

# CELL ********************

def load_csv_data_file_into_table(path_to_data_file, dataset_name, has_header_row = True, clean_column_names = True):
    # Read the data in
    #df = spark.read.format("csv").option("header", "true").load(path_to_data_file)
    df = read_csv(path_to_data_file, None, has_header_row)

    # Since the tables only allow alphanumeric characters and underscores for column names, we might
    # potentially need to rename some columns. So here we look for some common characters and just
    # replace. Note that you might need to add more characters to this code if your CSV headers
    # contain other 'invalid' or unsupported characters
    #final_df = clean_column_names(df, False)

    # Add the standard metadata fields to the data frame
    #final_df = add_metadata_fields_to_data_frame(cleaned_column_names_df)

    # Lastly create the table from the data frame - note that it will overwrite any existing table!
    df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(dataset_name)

# MARKDOWN ********************

# ## Table and data processing functions

# CELL ********************

# Removes or replaces 'invalid' characters in column names in a data 
# frame ready for insert into a delta table

def clean_column_names(data_frame, replace_characters = True):
    from pyspark.sql.functions import col

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
        lit(current_timestamp()).cast(TimestampType()).alias(datetime_loaded_field_name),
        lit(None).cast(TimestampType()).alias(datetime_updated_field_name),
        lit(None).cast(TimestampType()).alias(datetime_deleted_field_name),
        "*")

# CELL ********************

# Returns a Boolean to indicate if the specified table exists or not

def table_exists(lakehouse_name, table_name):
    # Get list of tables objects for the lakehouse could
    # use the alternative 'spark.catalog.tableExists'?
    tables = spark.catalog.listTables(lakehouse_name)
    
    # Is the table we want in the list?
    if len([item for item in tables if item.name == table_name]) > 0:
        return True
    else:
        return False

# CELL ********************

# Some table comparison helpers

def table_schemas_are_different(table1, table2, column_names_to_exclude):
    # Get the schema for the first table
    t1_df = spark.read.table(table1)
    t1_column_names = t1_df.schema.names
    
    # Get the schema for the other table
    t2_df = spark.read.table(table2)
    t2_column_names = t2_df.schema.names
    
    # Check for differences, returns True/False depending on outcome
    return are_different_lists(t1_column_names, t2_column_names, column_names_to_exclude)

def get_differences_between_table_schemas(table1, table2, column_names_to_exclude):
    # Get the schema for the first table
    t1_df = spark.read.table(table1)
    t1_column_names = t1_df.schema.names
    
    # Get the schema for the other table
    t2_df = spark.read.table(table2)
    t2_column_names = t2_df.schema.names
    
    # Return list of the differences
    return get_differences_between_lists(t1_column_names, t2_column_names, column_names_to_exclude)

# CELL ********************

# Merge table helpers

def build_join_condition(source_alias, target_alias, join_columns):
    return " AND ".join([f"{target_alias}.{col} = {source_alias}.{col}" for col in join_columns])
    
def build_update_condition(source_alias, target_alias, join_columns, columns):
    # First remove the join columns, as we don't update join columns!
    columns_to_update = remove_items_from_list(columns, join_columns)

    # Now build the update differences check condition
    return " OR ".join([f"{target_alias}.{col} <> {source_alias}.{col}" for col in columns_to_update])

def build_update_set(source_alias: str, target_alias: str, columns: list) -> dict:
    return dict([(f"{target_alias}.{key}", f"{source_alias}.{val}") for key, val in zip(columns[::1], columns[::1])])

def build_insert_values(source_alias: str, target_alias: str, columns: list) -> dict:
    return dict([(f"{target_alias}.{key}", f"{source_alias}.{val}") for key, val in zip(columns[::1], columns[::1])])

# CELL ********************

# Basic merge for data from source into target
# See https://learn.microsoft.com/en-us/azure/databricks/delta/merge

from delta.tables import *

def merge_source_into_target(source_table_name, target_table_name, join_columns):        
    # Set some standard stuff
    source_alias = "source"
    target_alias = "target"

    # Get the source data
    source_df = spark.read.table(source_table_name)
    
    # Sort out lists of columns
    column_names = source_df.schema.names
    columns_to_update = remove_items_from_list(column_names, join_columns)

    # Build the join condition
    join_condition = build_join_condition(source_alias, target_alias, join_columns)
    print(f"MERGE - join condition = '{join_condition}'")
    
    # Build the update condition
    update_condition = build_update_condition(source_alias, target_alias, join_columns, column_names)
    print(f"MERGE - update condition = '{update_condition}'")

    # Build the update fields    
    update_set = build_update_set(source_alias, target_alias, columns_to_update)
    print(f"MERGE - update set = '{update_set}'")

    # Get the target
    target_table = DeltaTable.forName(spark, target_table_name)

    # Run the merge
    (target_table.alias(target_alias)\
        .merge(source = source_df.alias(source_alias), condition = join_condition)\
        .whenNotMatchedInsertAll()\
        .whenMatchedUpdate(condition = update_condition, set = update_set)\
        .execute())

# CELL ********************

# Merges data from source into target, but sets target timestamp columns for DateTimeLoaded, DateTimeUpdated
# See https://learn.microsoft.com/en-us/azure/databricks/delta/merge

from delta.tables import *

def merge_source_into_target_with_timestamps(source_table_name, target_table_name, join_columns, loaded_ts_column_name, updated_ts_column_name):
    from pyspark.sql.functions import current_timestamp

    # Set some standard stuff
    source_alias = "source"
    target_alias = "target"

    # Get the source data
    source_df = spark.read.table(source_table_name)
    
    # Sort out lists of columns
    column_names = source_df.schema.names
    
    # Build the join condition
    join_condition = build_join_condition(source_alias, target_alias, join_columns)
    print(f"MERGE - join condition = '{join_condition}'")
    
    # Build the update condition
    update_condition = build_update_condition(source_alias, target_alias, join_columns, column_names)
    print(f"MERGE - update condition = '{update_condition}'")

    # Build the update fields    
    columns_to_update = remove_items_from_list(column_names, join_columns)
    update_set = build_update_set(source_alias, target_alias, columns_to_update)
    update_set[updated_ts_column_name] = current_timestamp()
    print(f"MERGE - update set = '{update_set}'")

    # Build the insert fields    
    insert_values = build_insert_values(source_alias, target_alias, column_names)
    insert_values[loaded_ts_column_name] = current_timestamp()
    print(f"MERGE - insert values = '{insert_values}'")

    # Get the target
    target_table = DeltaTable.forName(spark, target_table_name)

    # Run the merge
    target_table.alias(target_alias)\
        .merge(source = source_df.alias(source_alias), condition = join_condition)\
        .whenNotMatchedInsert(values = insert_values)\
        .whenMatchedUpdate(condition = update_condition, set = update_set)\
        .execute()
