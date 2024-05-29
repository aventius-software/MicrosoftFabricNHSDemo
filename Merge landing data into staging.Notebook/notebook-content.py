# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c3283c02-4fae-433e-b45f-a900e8be8237",
# META       "default_lakehouse_name": "StagingLakehouse",
# META       "default_lakehouse_workspace_id": "7d297a63-edac-48fc-9875-65dbf95464db",
# META       "known_lakehouses": [
# META         {
# META           "id": "c3283c02-4fae-433e-b45f-a900e8be8237"
# META         },
# META         {
# META           "id": "2c074247-8225-4557-9291-126b8442b5f4"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Merges landing data into staging table

# PARAMETERS CELL ********************

# Name of the dataset and optional subset
dataset_name = "referral_to_treatment"
subset_name = None

# Comma separated string of columns used to join on
join_columns = "Period,ProviderOrgCode,CommissionerOrgCode,RTTPartType,TreatmentFunctionCode"

# Optional comma separated string of columns to exclude in the merge
excluded_columns = ""

# CELL ********************

%run "Shared parameters"

# CELL ********************

%run "Shared functions"

# CELL ********************

# Lets run the import process
table_name = dataset_name

# If there's a subset name we append this to the table name
if subset_name is not None and len(subset_name) > 0:
    table_name = table_name + "_" + subset_name

print(f"Table name: '{table_name}'")

# Set the names for lakehouse source/target tables
source_table_name = f"LandingLakehouse.{table_name}"
target_table_name = f"StagingLakehouse.{table_name}"
print(f"Table names for merge: '{source_table_name}' into '{target_table_name}'")

# Check if target table exists, if not then load straight into a new table
if not table_exists("StagingLakehouse", table_name):
    print("Staging table doesn't exist, copying data from landing and creating new staging table")

    # No target table yet, so load the source table into a dataframe
    source_df = spark.table(source_table_name)
    
    # Add the standard metadata fields to the data frame
    final_df = add_metadata_fields_to_data_frame(source_df)

    # Now just create a new table in staging lakehouse with the data from landing ;-)
    final_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table_name)

    print("Staging table created from landing table data")
else:
    print("Staging table exists, comparing landing and staging table schemas for any differences")

    # The staging table exists, so first we need to check for schema differences 
    # between the landing table and the staging table in case something has 
    # changed in the new landing table (e.g. column missing or renamed etc...). Just
    # make sure we exclude staging only columns when we compare the schemas!
    column_names_to_exclude = [datetime_loaded_field_name, datetime_updated_field_name, datetime_deleted_field_name]
    
    # Are there any differences?
    if table_schemas_are_different(source_table_name, target_table_name, column_names_to_exclude):        
        # Yes, there are differences, so lets get the columns involved
        differences = get_differences_between_table_schemas(source_table_name, target_table_name, column_names_to_exclude) 
        error_message = f"Schema differences detected between landing and staging tables: {differences}"
        print(error_message)

        # Log the error showing the different columns
        log_error(error_message)

        # Trigger an error!
        raise ValueError(error_message)    

    # If we're here, then all is ok so far. So lets transform the list of join
    # columns into a list for the merge function...
    join_columns_list = join_columns.split(",")
    
    print("Staging merge starting")

    # Merge the landing data into staging lakehouse table
    merge_source_into_target_with_timestamps(source_table_name, target_table_name, join_columns_list, datetime_loaded_field_name, datetime_updated_field_name)

    print("Staging merge completed")
