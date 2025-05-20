import psycopg2
from psycopg2 import sql
import csv
import os
import glob
import pandas as pd
import numpy as np
import datetime
from typing import Dict, List, Any, Union, Optional # Import types for clarity
import uuid

# --- Database Configuration ---
DB_CONFIG = {
    "host": "localhost",
    "port": "5433",  # Make sure this matches your PostgreSQL port
    "dbname": "postgres", # Replace with your actual database name
    "user": "data_eng",      # Replace with your PostgreSQL user
    "password": "12345pP" # Replace with your password
}

# --- CSV Files Directory ---
CSV_DIR = "/home/blueberry/Desktop/advanced_sql_tutorial/data"  # Create a folder named 'csv_data' and put your CSVs there

# --- Table Schema Mapping ---
# Maps the base filename (without .csv) to the fully qualified schema.table
# This is CRUCIAL and needs to be accurate based on your DDL.
TABLE_SCHEMA_MAPPING = {
    "Address": "Person.Address",
    "AddressType": "Person.AddressType",
    "AWBuildVersion": "dbo.AWBuildVersion",
    "BillOfMaterials": "Production.BillOfMaterials",
    "BusinessEntity": "Person.BusinessEntity",
    "BusinessEntityAddress": "Person.BusinessEntityAddress",
    "BusinessEntityContact": "Person.BusinessEntityContact",
    "ContactType": "Person.ContactType",
    "CountryRegion": "Person.CountryRegion",
    "CreditCard": "Sales.CreditCard",
    "Culture": "Production.Culture",
    "Currency": "Sales.Currency",
    "CurrencyRate": "Sales.CurrencyRate",
    "Customer": "Sales.Customer",
    "Department": "HumanResources.Department",
    "Document": "Production.Document", # Note: DocumentNode PK might be tricky with auto-gen
    "EmailAddress": "Person.EmailAddress",
    "Employee": "HumanResources.Employee",
    "EmployeeDepartmentHistory": "HumanResources.EmployeeDepartmentHistory",
    "EmployeePayHistory": "HumanResources.EmployeePayHistory",
    "Illustration": "Production.Illustration",
    "JobCandidate": "HumanResources.JobCandidate",
    "Location": "Production.Location",
    "Password": "Person.Password",
    "Person": "Person.Person",
    "PersonCreditCard": "Sales.PersonCreditCard",
    "PersonPhone": "Person.PersonPhone",
    "PhoneNumberType": "Person.PhoneNumberType",
    "Product": "Production.Product",
    "ProductCategory": "Production.ProductCategory",
    "ProductCostHistory": "Production.ProductCostHistory",
    "ProductDescription": "Production.ProductDescription",
    "ProductDocument": "Production.ProductDocument",
    "ProductInventory": "Production.ProductInventory",
    "ProductListPriceHistory": "Production.ProductListPriceHistory",
    "ProductModel": "Production.ProductModel",
    "ProductModelIllustration": "Production.ProductModelIllustration",
    "ProductModelProductDescriptionCulture": "Production.ProductModelProductDescriptionCulture",
    "ProductPhoto": "Production.ProductPhoto",
    "ProductProductPhoto": "Production.ProductProductPhoto",
    "ProductReview": "Production.ProductReview",
    "ProductSubcategory": "Production.ProductSubcategory",
    "PurchaseOrderDetail": "Purchasing.PurchaseOrderDetail",
    "PurchaseOrderHeader": "Purchasing.PurchaseOrderHeader",
    "SalesOrderDetail": "Sales.SalesOrderDetail",
    "SalesOrderHeader": "Sales.SalesOrderHeader",
    "SalesOrderHeaderSalesReason": "Sales.SalesOrderHeaderSalesReason",
    "SalesPerson": "Sales.SalesPerson",
    "SalesPersonQuotaHistory": "Sales.SalesPersonQuotaHistory",
    "SalesReason": "Sales.SalesReason",
    "SalesTaxRate": "Sales.SalesTaxRate",
    "SalesTerritory": "Sales.SalesTerritory",
    "SalesTerritoryHistory": "Sales.SalesTerritoryHistory",
    "ScrapReason": "Production.ScrapReason",
    "Shift": "HumanResources.Shift",
    "ShipMethod": "Purchasing.ShipMethod",
    "ShoppingCartItem": "Sales.ShoppingCartItem",
    "SpecialOffer": "Sales.SpecialOffer",
    "SpecialOfferProduct": "Sales.SpecialOfferProduct",
    "StateProvince": "Person.StateProvince",
    "Store": "Sales.Store",
    "TransactionHistory": "Production.TransactionHistory",
    "TransactionHistoryArchive": "Production.TransactionHistoryArchive",
    "UnitMeasure": "Production.UnitMeasure",
    "Vendor": "Purchasing.Vendor",
    "WorkOrder": "Production.WorkOrder",
    "WorkOrderRouting": "Production.WorkOrderRouting",
    # This table was in your CSV list but not the original ERD/DDL.
    # You'll need to create it. Assuming a simple structure.
    "CountryRegionCurrency": "Sales.CountryRegionCurrency"
}

# --- Define the order of loading tables to respect foreign key constraints ---
# This is the MOST IMPORTANT part for successful loading.
# List base filenames (without .csv) in the order they should be loaded.
# Parent tables first, then child tables.
# This is a best-effort order based on common dependencies. You MAY need to adjust it.
TABLE_LOAD_ORDER = [
    # Core/Lookup Tables (Fewest Dependencies)
    "AWBuildVersion",
    "CountryRegion",
    "AddressType",
    "ContactType",
    "PhoneNumberType",
    "UnitMeasure",
    "ProductCategory",
    "Culture",
    "Currency",
    "Department",
    "Shift",
    "Illustration",
    "Location",
    "ProductDescription",
    "ProductPhoto",
    "ScrapReason",
    "ShipMethod",
    "SpecialOffer", # Depends on nothing major for its own columns
    "SalesReason",

    # Business Entity and Person related (Core)
    "BusinessEntity", # Parent for Person, Store, Vendor
    "Person", # Depends on BusinessEntity
    "Password", # Depends on Person
    "EmailAddress", # Depends on Person
    "PersonPhone", # Depends on Person, PhoneNumberType
    "StateProvince", # Depends on CountryRegion, SalesTerritory (FK added later in DDL)
    "Address", # Depends on StateProvince
    "BusinessEntityAddress", # Depends on BusinessEntity, Address, AddressType

    # Human Resources
    "Employee", # Depends on Person
    "EmployeeDepartmentHistory", # Depends on Employee, Department, Shift
    "EmployeePayHistory", # Depends on Employee
    "JobCandidate", # Can depend on Employee (nullable FK)
    "Document", # Depends on Employee (Owner) - HIERARCHYID (DocumentNode) needs careful handling if not text

    # Production (Products and related components)
    "ProductSubcategory", # Depends on ProductCategory
    "ProductModel", # No direct FKs in its main columns, but linked from Product
    "Product", # Depends on ProductModel, ProductSubcategory, UnitMeasure
    "BillOfMaterials", # Depends on Product (ProductAssemblyID, ComponentID), UnitMeasure
    "ProductCostHistory", # Depends on Product
    "ProductListPriceHistory", # Depends on Product
    "ProductReview", # Depends on Product
    "ProductInventory", # Depends on Product, Location
    "ProductProductPhoto", # Depends on Product, ProductPhoto
    "ProductModelIllustration", # Depends on ProductModel, Illustration
    "ProductModelProductDescriptionCulture", # Depends on ProductModel, ProductDescription, Culture
    "ProductDocument", # Depends on Product, Document
    "WorkOrder", # Depends on Product, ScrapReason
    "WorkOrderRouting", # Depends on WorkOrder, Product, Location
    "TransactionHistory", # Depends on Product
    "TransactionHistoryArchive", # Data comes from TransactionHistory, not direct FK usually

    # Purchasing
    "Vendor", # Depends on BusinessEntity
    "PurchaseOrderHeader", # Depends on Employee, Vendor, ShipMethod
    "PurchaseOrderDetail", # Depends on PurchaseOrderHeader, Product

    # Sales (Core sales structures)
    "CountryRegionCurrency", # Assuming FKs to CountryRegion, Currency. Create this table.
    "CurrencyRate", # Depends on Currency (From and To)
    "SalesTerritory", # Depends on CountryRegion
    "SalesPerson", # Depends on Employee, SalesTerritory (nullable)
    "Store", # Depends on BusinessEntity, SalesPerson (nullable)
    "Customer", # Depends on Person (nullable), Store (nullable), SalesTerritory
    "CreditCard",
    "PersonCreditCard", # Depends on Person, CreditCard
    "SpecialOfferProduct", # Depends on SpecialOffer, Product
    "SalesOrderHeader", # Depends on Customer, SalesPerson (nullable), Territory, Address, ShipMethod, CreditCard (nullable), CurrencyRate (nullable)
    "SalesOrderDetail", # Depends on SalesOrderHeader, Product, SpecialOffer (via SpecialOfferProduct or direct)
    "SalesOrderHeaderSalesReason", # Depends on SalesOrderHeader, SalesReason
    "SalesPersonQuotaHistory", # Depends on SalesPerson
    "SalesTaxRate", # Depends on StateProvince
    "SalesTerritoryHistory", # Depends on SalesPerson, SalesTerritory
    "ShoppingCartItem" # Depends on Product
]

DB_TO_PANDAS_TYPE_MAP: Dict[str, Union[str, type, callable]] = {
    # Integer types
    'integer': 'Int64', # Nullable integer
    'smallint': 'Int64',
    'bigint': 'Int64',

    # Floating point types
    'decimal': 'Float64', # Nullable float for numeric/decimal
    'numeric': 'Float64',
    'real': 'Float64',
    'double precision': 'Float64',

    # Boolean type
    'boolean': 'boolean', # Nullable boolean

    # String/Text types
    'character varying': 'string', # Nullable string (requires pandas >= 1.0)
    'varchar': 'string',
    'text': 'string',
    'character': 'string', # char

    # Date and Time types
    'date': 'datetime64[ns]', # pandas datetime
    'timestamp without time zone': 'datetime64[ns]',
    'timestamp with time zone': 'datetime64[ns]',
    'time without time zone': 'object', # Time might need custom handling or kept as object/string
    'time with time zone': 'object',

    # UUID type (often stored as string or binary in DB, but we want uuid.UUID objects in Python)
    'uuid': 'string', # Will handle conversion to uuid.UUID objects separately

    # Other types (add as needed)
    # 'json': 'object',
    # 'jsonb': 'object',
    # 'bytea': 'object', # Or 'bytes' dtype if applicable
    # 'inet': 'object', # IP address types
    # 'cidr': 'object',
    # 'macaddr': 'object',
}


def parse_csv_file(csv_filepath):
    """
    Parse a CSV file and return its contents.
    This function is a placeholder. You can use pandas or csv module to read the file.
    """
    # Using pandas for better handling of CSVs
    try:
        df = pd.read_csv(csv_filepath, sep='\t', header=None, encoding='cp1252', low_memory=False)
        return df
    except Exception as e:
        print(f"Error reading CSV file {csv_filepath}: {e}")
        return None

def get_table_schema_with_schema(schema_name: str, table_name: str, conn: Any) -> Dict[str, str]:
    column_info: Dict[str, str] = {}
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s;
                """,
                (schema_name, table_name) # Pass schema and table name as separate parameters
            )
            results = cur.fetchall()
            for row in results:
                column_info[row[0]] = row[1]
        return column_info
    except Exception as e:
        print(f"Error fetching schema for {schema_name}.{table_name}: {e}")
        return {}


def rename_dataframe_columns_from_schema(df: pd.DataFrame, db_schema: Dict[str, str]) -> pd.DataFrame:
    """
    Renames DataFrame columns (expected to be 0, 1, 2, ...) using keys
    from a database schema dictionary.

    Args:
        df (pd.DataFrame): The DataFrame with default integer column names.
        db_schema (Dict[str, str]): A dictionary where keys are the desired
                                     column names in the correct order, and
                                     values are database data types.

    Returns:
        pd.DataFrame: The DataFrame with columns renamed according to the schema keys.

    Raises:
        ValueError: If the number of columns in the DataFrame does not match
                    the number of keys in the schema dictionary.
    """
    # Get the column names from the dictionary keys.
    # Dictionaries maintain insertion order in Python 3.7+
    new_column_names = list(db_schema.keys())

    # Validate that the number of columns matches the number of keys
    if len(df.columns) != len(new_column_names):
        raise ValueError(
            f"Column count mismatch: DataFrame has {len(df.columns)} columns (0 to {len(df.columns)-1}), "
            f"but schema dictionary has {len(new_column_names)} keys. "
            f"Cannot rename columns if counts don't match."
        )

    # Assign the new column names to the DataFrame's columns attribute
    df.columns = new_column_names

    print(f"Successfully renamed {len(df.columns)} columns.")
    return df

def convert_to_uuid_or_none(value: Any) -> Union[uuid.UUID, None]:
    """Helper to convert a value to UUID or None, handling errors and empty strings."""
    # Check for pandas NaN or None, or empty string after stripping
    if pd.isna(value) or str(value).strip() == '':
         return None
    try:
        # Attempt conversion, stripping potential braces {}
        return uuid.UUID(str(value).strip('{}'))
    except (ValueError, TypeError):
        # If conversion fails (invalid UUID string), return None
        # print(f"Warning: Could not convert '{value}' to UUID.") # Optional warning
        return None

def convert_dataframe_columns_to_db_types(df: pd.DataFrame, db_schema: Dict[str, str]) -> pd.DataFrame:
    """
    Converts DataFrame column data types based on a database schema dictionary.

    Assumes DataFrame columns have already been renamed to match db_schema keys.

    Args:
        df (pd.DataFrame): The DataFrame with columns named as in db_schema.
        db_schema (Dict[str, str]): A dictionary {column_name: db_data_type_string}.
                                     Ensure this dictionary does NOT contain the
                                     incorrectly named column.

    Returns:
        pd.DataFrame: The DataFrame with converted column dtypes.
                      Returns a copy, does not modify the original DataFrame.
    """
    # Work on a copy to avoid modifying the original DataFrame
    df_converted = df.copy()

    print("\nStarting data type conversion based on DB schema...")

    # Iterate through the schema dictionary items (column_name, db_type_str)
    for col_name, db_type_str in db_schema.items():
        # IMPORTANT: Ensure the malformed column name is NOT in your db_schema dictionary
        # when you call this function.

        # Check if the column exists in the DataFrame (it should if renaming worked)
        if col_name not in df_converted.columns:
            print(f"  Warning: Column '{col_name}' from schema not found in DataFrame. Skipping type conversion for this column.")
            continue

        # Get the target pandas type/conversion hint from our mapping
        # Use lower() for case-insensitivity when matching DB type names
        pandas_target_hint = DB_TO_PANDAS_TYPE_MAP.get(db_type_str.lower())

        if pandas_target_hint is None:
            print(f"  Warning: Database type '{db_type_str}' for column '{col_name}' is not mapped. Leaving column as '{df_converted[col_name].dtype}'.")
            continue # Skip conversion if the database type is not in our map

        current_dtype = df_converted[col_name].dtype
        # print(f"  Processing column '{col_name}' (DB type: '{db_type_str}', Current pandas dtype: '{current_dtype}')...")

        try:
            # --- Apply conversion based on the target hint ---

            if pandas_target_hint == 'datetime64[ns]':
                # Use pd.to_datetime for date/timestamp types
                # errors='coerce' will turn values that cannot be parsed into NaT (Not a Time)
                # format=... could be added here if you know the exact format for performance/reliability
                df_converted[col_name] = pd.to_datetime(df_converted[col_name], errors='coerce')

            elif pandas_target_hint == 'object' and db_type_str.lower() == 'uuid':
                # Custom conversion for UUIDs using .apply
                # .apply is good for element-wise transformations
                df_converted[col_name] = df_converted[col_name].apply(convert_to_uuid_or_none)
                 # Check if the resulting dtype is still 'object' as expected for UUID objects
                if df_converted[col_name].dtype != 'object':
                     print(f"  Warning: UUID conversion for column '{col_name}' did not result in 'object' dtype. Resulting dtype: '{df_converted[col_name].dtype}'")


            elif isinstance(pandas_target_hint, str):
                 # Use astype for standard pandas dtype strings ('Int64', 'string', 'boolean', 'Float64')
                 # Check if the current dtype is already the target dtype (case-insensitive check for nullable types)
                 if str(current_dtype).lower() == pandas_target_hint.lower():
                     # print(f"  Column '{col_name}' is already {current_dtype}, skipping conversion.")
                     pass # Already the correct type
                 else:
                     # For nullable types, astype handles conversion from object/string and None/NaN
                     # For safety, sometimes converting to object first can help if source is complex
                     # if current_dtype != object and not pd.api.types.is_string_dtype(current_dtype):
                     #     df_converted[col_name] = df_converted[col_name].astype(object)

                     df_converted[col_name] = df_converted[col_name].astype(pandas_target_hint)
            elif isinstance(pandas_target_hint, int):
                # If the target hint is an int, we can use pd.to_numeric
                # This is useful for integer types, but be careful with NaNs
                df_converted[col_name] = pd.to_numeric(df_converted[col_name], errors='coerce').astype(pandas_target_hint)
            elif isinstance(pandas_target_hint, float): 
                # If the target hint is a float, we can use pd.to_numeric
                # This is useful for float types, but be careful with NaNs
                df_converted[col_name] = pd.to_numeric(df_converted[col_name], errors='coerce').astype(pandas_target_hint)
            elif isinstance(pandas_target_hint, bool):      
                # If the target hint is a boolean, we can use astype(bool)
                # This is useful for boolean types, but be careful with NaNs
                df_converted[col_name] = df_converted[col_name].astype(bool)

                
            # Note: We are not explicitly handling Python type objects like int, float, bool here
            # because we are primarily mapping to pandas dtypes (strings like 'Int64')
            # If you mapped to `int`, `float`, etc., you'd add elif isinstance(pandas_target_hint, type):
            # and use df_converted[col_name] = pd.to_numeric(df_converted[col_name], errors='coerce').astype(pandas_target_hint) for numbers
            # or df_converted[col_name] = df_converted[col_name].astype(pandas_target_hint) for others, being careful about NaNs.
            # Mapping to the pandas nullable dtype strings ('Int64', 'string' etc.) is generally preferred.


            # print(f"  Converted column '{col_name}' to dtype: {df_converted[col_name].dtype}")

        except Exception as e:
            # Catch any errors during the conversion process for this column
            print(f"  Error converting column '{col_name}' (DB type '{db_type_str}') to target type. Error: {e}. Leaving column as '{df_converted[col_name].dtype}'.")
            # If an error occurs during conversion of a column, leave it as is and continue with the next column.
            continue

    print("Finished data type conversion.")
    return df_converted


def insert_dataframe_in_chunks(
    df: pd.DataFrame,
    conn: Any, # Type hint for connection object (e.g., psycopg2.extensions.connection)
    table_name: str, # Table name, can include schema (e.g., 'Sales.Address')
    chunk_size: int = 1000, # Number of rows per batch insert
    disable_fk_checks: bool = True # New flag to control FK checks
) -> None:
    """
    Inserts a pandas DataFrame into a PostgreSQL table in specified chunks.

    Args:
        df (pd.DataFrame): The DataFrame to insert.
        conn: The active psycopg2 database connection object.
        table_name (str): The name of the target table (e.g., 'my_table' or 'schema.my_table').
        chunk_size (int): The number of rows to insert in each batch.

    Raises:
        Exception: If a database error occurs during insertion.
        ValueError: If the DataFrame is empty.
    """
    if df.empty:
        print(f"DataFrame is empty. Skipping insertion for table {table_name}.")
        return

    # Get column names from the DataFrame
    df_columns = df.columns.tolist()
    num_columns = len(df_columns)

    if num_columns == 0:
        print(f"DataFrame has no columns. Skipping insertion for table {table_name}.")
        return

        # --- IMPROVED REPLACEMENT STEP: Replace ALL pandas NaT/NaN with None ---
    # This method is more generic and replaces any pandas missing value indicator
    # (NaN for numbers, NaT for datetimes, NA for nullable types) with None.
    print("Starting replacement of ALL pandas missing values (NaN/NaT/NA) with None...")
    df_cleaned = df.copy() # Work on a copy

    # Iterate through all columns
    for col in df_cleaned.columns:
        # Check if NaT/NaN/NA exists in this column before replacement
        has_missing_before = df_cleaned[col].isna().any()
        # print(f"  Processing column '{col}' (dtype: {df_cleaned[col].dtype})... has missing: {has_missing_before}") # Verbose debug

        if has_missing_before:
             # Replace all pandas missing values in this column with None
             df_cleaned[col] = df_cleaned[col].replace({pd.NA: None, pd.NaT: None})
             # Also handle NumPy NaN if present in object columns etc.
             # Using .loc[isna()] is often most reliable for *all* missing types
             df_cleaned.loc[df_cleaned[col].isna(), col] = None

        # Verify replacement for NaT specifically if it's a datetime column
        if df_cleaned[col].dtype == 'datetime64[ns]':
            has_pd_nat_after = (df_cleaned[col] == pd.NaT).any()
            # print(f"    Column '{col}' still contains pd.NaT after replacement: {has_pd_nat_after}") # Verbose debug
            if has_pd_nat_after:
                 print(f"  CRITICAL DEBUG: Column '{col}' (datetime) *still* contains pd.NaT after replacement.")
            else:
                 print(f"  Debug: Column '{col}' (datetime) successfully replaced pd.NaT with None.") # Confirm success


    print("Replacement complete. Final dtypes of cleaned DataFrame:")
    print(df_cleaned.dtypes)
    print("-" * 30)
    # --- END IMPROVED REPLACEMENT STEP ---
    # --- CORRECTED: Construct the INSERT SQL statement using psycopg2.sql ---
    # Correctly handle schema.table notation using sql.Identifier with multiple arguments
    if '.' in table_name:
        # Split into schema and table name
        schema_name, simple_table_name = table_name.split('.', 1) # Split only on the first dot
        # Create ONE identifier object for the qualified name
        table_ident = sql.Identifier(schema_name, simple_table_name)
    else:
        # No schema specified, just use the table name
        table_ident = sql.Identifier(table_name)

    # Column identifiers (use original df_columns names)
    column_idents = [sql.Identifier(col) for col in df_columns]
    column_list = sql.SQL(', ').join(column_idents)

    # Placeholders
    placeholders = sql.SQL(', ').join(sql.Placeholder() * num_columns)

    # --- CORRECTED: Use ONE placeholder for the table identifier in the template ---
    insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        table_ident,   # <--- This is ONE Composable object representing "schema"."table"
        column_list,   # <--- This is a Composable object
        placeholders   # <--- This is a Composable object
    )

        # --- SQL commands for disabling/enabling triggers ---
    disable_triggers_sql = sql.SQL("ALTER TABLE {} DISABLE TRIGGER ALL;").format(table_ident)
    enable_triggers_sql = sql.SQL("ALTER TABLE {} ENABLE TRIGGER ALL;").format(table_ident)


    print(f"\nStarting bulk insert into {table_name} in chunks of {chunk_size}...")
    print(f"Total rows to insert: {len(df_cleaned)}")

    num_rows = len(df_cleaned)
    num_chunks = (num_rows + chunk_size - 1) // chunk_size

    # --- Main try...finally block to ensure triggers are re-enabled ---
    try:
        # --- Disable FK checks if requested ---
        if disable_fk_checks:
            print(f"  Disabling triggers (including FK checks) for {table_name}...")
            with conn.cursor() as cur:
                 cur.execute(disable_triggers_sql)
                 conn.commit() # Commit the ALTER TABLE command
            print(f"  Triggers disabled.")

        # --- Insertion loop ---
        with conn.cursor() as cur:
            for i in range(num_chunks):
                 start_index = i * chunk_size
                 end_index = min((i + 1) * chunk_size, num_rows)

                 chunk_df = df_cleaned.iloc[start_index:end_index]
                 data_for_batch = list(chunk_df.itertuples(index=False, name=None))

                 if not data_for_batch:
                     print(f"  Warning: Batch {i+1}/{num_chunks} is empty. Skipping.")
                     continue

                 try:
                     cur.executemany(insert_sql, data_for_batch)
                     print(f"  Inserted chunk {i+1}/{num_chunks} ({len(data_for_batch)} rows)...")
                     conn.commit() # Commit the transaction for this batch

                 except Exception as e:
                     conn.rollback() # Rollback the failed batch
                     print(f"\n  Error inserting chunk {i+1}/{num_chunks} starting at row {start_index}. Rolling back batch.")
                     print(f"  Error details: {e}")
                     # Re-raise the exception so the outer block knows loading failed
                     raise e

        print(f"Successfully inserted all {len(df_cleaned)} rows into {table_name}.")

    except Exception as e:
        # Catch any errors during insertion (either from batch or other issues)
        print(f"\nAn error occurred during the insertion process for table {table_name}.")
        # The batch error handler already rolled back the failed batch
        # Decide if you need a final rollback here based on your overall transaction strategy
        raise e # Re-raise the exception to signal failure

    finally:
        # --- Ensure FK checks are re-enabled ---
        if disable_fk_checks:
            print(f"  Re-enabling triggers for {table_name}...")
            try:
                with conn.cursor() as cur:
                    cur.execute(enable_triggers_sql)
                    conn.commit() # Commit the ALTER TABLE command
                print(f"  Triggers re-enabled.")
            except Exception as e:
                 print(f"  ERROR: Failed to re-enable triggers for {table_name}: {e}")
                 print("  Manual intervention required to ENABLE TRIGGER ALL for this table!")
                 # Note: You might want to log this critical error properly



def main():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Successfully connected to PostgreSQL database.")

        # Process files in the defined order
        for base_filename in TABLE_LOAD_ORDER:
            csv_file_path = os.path.join(CSV_DIR, f"{base_filename}.csv")
            if base_filename in TABLE_SCHEMA_MAPPING:
                full_table_name = TABLE_SCHEMA_MAPPING[base_filename].lower()
                if os.path.exists(csv_file_path):
                    print(f"Processing file: {base_filename}")
                    schema_name = full_table_name.split('.')[0].lower()
                    table_name = full_table_name.split('.')[1].lower()
                    # Check if the table is already created
                    column_from_db = get_table_schema_with_schema(schema_name, table_name, conn)
                    print(f"  Address table schema: {column_from_db}")
                    try:
                        contents = parse_csv_file(csv_file_path)
                        if contents is None:
                            print(f"  Error parsing file {os.path.basename(csv_file_path)}. Skipping.")
                            return
                        # Check if the DataFrame is empty
                        if contents.empty:
                            print(f"  File {os.path.basename(csv_file_path)} is empty. Skipping.")
                            return
                        # Check if the DataFrame has any rows
                        if contents.shape[0] == 0:
                            print(f"  File {os.path.basename(csv_file_path)} has no rows. Skipping.")
                            return
                        
                    except Exception as e:
                        print(f"Error processing file {csv_file_path}: {e}")
                        return
                
                    print(f"  DataFrame shape: {contents.head()}")
                    print(f"  DataFrame columns: {contents.columns.tolist()}")
                    # Get number of columns from database
                    columns_from_db = len(list(column_from_db.keys()))
                    print(f"  Number of columns in database: {columns_from_db}")
                    new_df = rename_dataframe_columns_from_schema(contents.copy(), column_from_db)
                    print(f"  DataFrame shape after renaming: {new_df.head()}")
                    converted_new_df = convert_dataframe_columns_to_db_types(new_df.copy(), column_from_db)
                    print(f"  DataFrame shape after conversion: {converted_new_df.head()}")
                    # Load the DataFrame into the database
                    insert_dataframe_in_chunks(converted_new_df, conn, full_table_name)
                        
                else:
                    print(f"CSV file {csv_file_path} not found, but was in load order. Skipping.")
            else:
                print(f"Warning: Filename '{base_filename}' is in load order but not in TABLE_SCHEMA_MAPPING. Skipping.")
        

    except psycopg2.Error as e:
        print(f"Database connection error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    if not os.path.isdir(CSV_DIR):
        print(f"Error: CSV directory '{CSV_DIR}' not found. Please create it and add your CSV files.")
    else:
        main()
