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

# MARKDOWN ********************

# # Download and unpack data to the landing lakehouse
# 
# - First build any control and/or destination tables if they don't already exist
# - Download the data file(s) from the web and unzip if required, log the activity in the audit tables
# - Load the data from the downloaded/unzipped file(s) into the relevant destination table

# PARAMETERS CELL ********************

# Destination table name to use in the lake house
dataset_name = "referral_to_treatment"
subset_name = None

# The URL to download the file from
download_url = "https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2024/02/Full-CSV-data-file-Dec23-ZIP-3569K-58522.zip"

# Specify the file format (not implemented yet, still todo)
file_format = "csv"

# Flag to indicate if the file has a header row
has_header_row = False

# CELL ********************

%run "Shared parameters"

# CELL ********************

%run "Shared functions"

# CELL ********************

def get_lakehouse_path(path):
    return f"/lakehouse/default/{path}"

# Downloads file to lake
def download_file_to_lake(url, dataset_name):
    # Create the relevant download folder if not already there
    create_dataset_folders(dataset_name)
        
    # Although, first we need to work out where we need to download this file 
    # to in the lake, start by getting the file name from the url
    file_name = get_file_name_from_url(url)
    print(f"Extracted file name from url = '{file_name}'")
    
    # Check if this file is a zipped file or not
    is_zipped_file = file_name.endswith(".zip")
    print(f"Is file zipped? {is_zipped_file}")
    
    # Set the download location depending on whether the file is zipped or not
    if is_zipped_file:
        # File IS zipped, so set the download destination folder appropriately
        print("File is zipped")
        destination_path_and_filename = get_zipped_download_location(dataset_name, file_name)
    else:
        # File IS NOT zipped, so set the download destination folder appropriately
        print("File is not zipped")
        destination_path_and_filename = destination_folder_path_normal
    
    # Translate path to start "/lakehouse/default/Files/"
    lakehouse_destination_path_and_filename = get_lakehouse_path(destination_path_and_filename)

    # Now we've got the destination download folder, we can try and download the file
    print(f"Downloading from '{url}' to destination path '{lakehouse_destination_path_and_filename}'")
    download_file(url, lakehouse_destination_path_and_filename)
    
    # Set the path to the final data file
    path_to_data_file = destination_path_and_filename

    # If its a zipped file, we need to unzip it AND change the 'path_to_data_file' value!
    if is_zipped_file:
        # Get folder location for where we unzip to
        raw_unzipped_path = get_unzipped_download_location(dataset_name, file_name)
        path_to_unzip_to = get_lakehouse_path(raw_unzipped_path)
        print(f"Path to unzip to = '{path_to_unzip_to}'")
        
        # Get the path to the zipped file
        file_to_unzip = get_lakehouse_path(destination_path_and_filename)
        print(f"File to unzip = '{file_to_unzip}'")

        # Finally, unzip...
        unzip_file(file_to_unzip, path_to_unzip_to)
        
        # Now lets find the name of the file that was inside the ZIP to read into a 
        # table, first get a list of files from the download location and 
        # we'll (for now) read the first file that is in the unzipped folder
        print(f"Getting list of unzipped files in folder '{raw_unzipped_path}'")
        files_in_unzipped_folder = mssparkutils.fs.ls(raw_unzipped_path)

        # Just the first file...    
        path_to_data_file = files_in_unzipped_folder[0].path

    # For debugging
    print(f"Raw 'path_to_data_file' = '{path_to_data_file}'")

    # Remove absolute path if there is one...
    if path_to_data_file.startswith("abfss://"):
        # Notes
        print("Removing absolute path and translating to relative path")

        # Define the substring to search for
        start_of_relative_path = "/Files/"

        # Calculate the position of the substring in the main string
        position = path_to_data_file.find(start_of_relative_path)

        # Adjust the position to match Python's 0-based indexing
        adjusted_position = position if position != -1 else None

        # Just get the relative path from 'Files' instead of the whole absolute path
        path_to_data_file = path_to_data_file[adjusted_position + 1:len(path_to_data_file)]
        
    # Hopefully now we should have a path to the data file, whether its zipped or not!
    print(f"Path to final data file ready for import '{path_to_data_file}'")

    return path_to_data_file

# CELL ********************

# Loads the CSV file into a data frame

def load_data_file_into_table(file_format, path_to_data_file, table_name, header):
    # Read the data into a data frame
    df = spark.read.format("csv").option("header", header).load(path_to_data_file)

    # Since the tables only allow alphanumeric characters and underscores for column names, we might
    # potentially need to rename some columns. So here we look for some common characters and just
    # replace. Note that you might need to add more characters to this code if your CSV headers
    # contain other 'invalid' or unsupported characters
    final_df = clean_column_names(df, False)
    
    # Lastly create the table from the data frame - note that we want to overwrite any existing table!
    final_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(table_name)

# CELL ********************

# Lets run the import process
table_name = dataset_name

# If there's a subset name we append this to the table name
if subset_name is not None and len(subset_name) > 0:
    table_name = table_name + "_" + subset_name

# Try and download the file to the lake
path_to_data_file = download_file_to_lake(download_url, dataset_name)

# Finally load the file into a table
load_data_file_into_table(file_format, path_to_data_file, table_name, has_header_row)
