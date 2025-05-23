# import psycopg2
# from psycopg2 import sql
# import csv
# import os
# import glob
# import pandas as pd
# import numpy as np
# import datetime
# from typing import Dict, List, Any, Union, Optional # Import types for clarity
# import uuid
# import io
# # --- Database Configuration ---
# DB_CONFIG = {
#     "host": "localhost",
#     "port": "5433",  # Make sure this matches your PostgreSQL port
#     "dbname": "postgres", # Replace with your actual database name
#     "user": "data_eng",      # Replace with your PostgreSQL user
#     "password": "12345pP" # Replace with your password
# }

# # --- CSV Files Directory ---
# CSV_DIR = "/home/blueberry/Desktop/advanced_sql_tutorial/data"  # Create a folder named 'csv_data' and put your CSVs there

# # --- Table Schema Mapping ---
# # Maps the base filename (without .csv) to the fully qualified schema.table
# # This is CRUCIAL and needs to be accurate based on your DDL.
# TABLE_SCHEMA_MAPPING = {
#     "Address": "Person.Address",
#     "AddressType": "Person.AddressType",
#     "AWBuildVersion": "dbo.AWBuildVersion",
#     "BillOfMaterials": "Production.BillOfMaterials",
#     "BusinessEntity": "Person.BusinessEntity",
#     "BusinessEntityAddress": "Person.BusinessEntityAddress",
#     "BusinessEntityContact": "Person.BusinessEntityContact",
#     "ContactType": "Person.ContactType",
#     "CountryRegion": "Person.CountryRegion",
#     "CreditCard": "Sales.CreditCard",
#     "Culture": "Production.Culture",
#     "Currency": "Sales.Currency",
#     "CurrencyRate": "Sales.CurrencyRate",
#     "Customer": "Sales.Customer",
#     "Department": "HumanResources.Department",
#     "Document": "Production.Document", # Note: DocumentNode PK might be tricky with auto-gen
#     "EmailAddress": "Person.EmailAddress",
#     "Employee": "HumanResources.Employee",
#     "EmployeeDepartmentHistory": "HumanResources.EmployeeDepartmentHistory",
#     "EmployeePayHistory": "HumanResources.EmployeePayHistory",
#     "Illustration": "Production.Illustration",
#     # "JobCandidate": "HumanResources.JobCandidate",
#     "Location": "Production.Location",
#     "Password": "Person.Password",
#     "Person": "Person.Person",
#     "PersonCreditCard": "Sales.PersonCreditCard",
#     "PersonPhone": "Person.PersonPhone",
#     "PhoneNumberType": "Person.PhoneNumberType",
#     "Product": "Production.Product",
#     "ProductCategory": "Production.ProductCategory",
#     "ProductCostHistory": "Production.ProductCostHistory",
#     "ProductDescription": "Production.ProductDescription",
#     "ProductDocument": "Production.ProductDocument",
#     "ProductInventory": "Production.ProductInventory",
#     "ProductListPriceHistory": "Production.ProductListPriceHistory",
#     "ProductModel": "Production.ProductModel",
#     "ProductModelIllustration": "Production.ProductModelIllustration",
#     "ProductModelProductDescriptionCulture": "Production.ProductModelProductDescriptionCulture",
#     "ProductPhoto": "Production.ProductPhoto",
#     "ProductProductPhoto": "Production.ProductProductPhoto",
#     # "ProductReview": "Production.ProductReview",
#     "ProductSubcategory": "Production.ProductSubcategory",
#     "PurchaseOrderDetail": "Purchasing.PurchaseOrderDetail",
#     "PurchaseOrderHeader": "Purchasing.PurchaseOrderHeader",
#     "SalesOrderDetail": "Sales.SalesOrderDetail",
#     "SalesOrderHeader": "Sales.SalesOrderHeader",
#     "SalesOrderHeaderSalesReason": "Sales.SalesOrderHeaderSalesReason",
#     "SalesPerson": "Sales.SalesPerson",
#     "SalesPersonQuotaHistory": "Sales.SalesPersonQuotaHistory",
#     "SalesReason": "Sales.SalesReason",
#     "SalesTaxRate": "Sales.SalesTaxRate",
#     "SalesTerritory": "Sales.SalesTerritory",
#     "SalesTerritoryHistory": "Sales.SalesTerritoryHistory",
#     "ScrapReason": "Production.ScrapReason",
#     "Shift": "HumanResources.Shift",
#     "ShipMethod": "Purchasing.ShipMethod",
#     "ShoppingCartItem": "Sales.ShoppingCartItem",
#     "SpecialOffer": "Sales.SpecialOffer",
#     "SpecialOfferProduct": "Sales.SpecialOfferProduct",
#     "StateProvince": "Person.StateProvince",
#     "Store": "Sales.Store",
#     "TransactionHistory": "Production.TransactionHistory",
#     "TransactionHistoryArchive": "Production.TransactionHistoryArchive",
#     "UnitMeasure": "Production.UnitMeasure",
#     "Vendor": "Purchasing.Vendor",
#     "WorkOrder": "Production.WorkOrder",
#     "WorkOrderRouting": "Production.WorkOrderRouting",
#     "CountryRegionCurrency": "Sales.CountryRegionCurrency"
# }

# # --- Define the order of loading tables to respect foreign key constraints ---
# # This is the MOST IMPORTANT part for successful loading.
# # List base filenames (without .csv) in the order they should be loaded.
# # Parent tables first, then child tables.
# # This is a best-effort order based on common dependencies. You MAY need to adjust it.
# TABLE_LOAD_ORDER = [
#     # Core/Lookup Tables (Fewest Dependencies)
#     "AWBuildVersion",
#     "CountryRegion",
#     "AddressType",
#     "ContactType",
#     "PhoneNumberType",
#     "UnitMeasure",
#     "ProductCategory",
#     "Culture",
#     "Currency",
#     "Department",
#     "Shift",
#     "Illustration",
#     "Location",
#     "ProductDescription",
#     "ProductPhoto",
#     "ScrapReason",
#     "ShipMethod",
#     "SpecialOffer", # Depends on nothing major for its own columns
#     "SalesReason",

#     # Business Entity and Person related (Core)
#     "BusinessEntity", # Parent for Person, Store, Vendor
#     "Person", # Depends on BusinessEntity
#     "Password", # Depends on Person
#     "EmailAddress", # Depends on Person
#     "PersonPhone", # Depends on Person, PhoneNumberType
#     "StateProvince", # Depends on CountryRegion, SalesTerritory (FK added later in DDL)
#     "Address", # Depends on StateProvince
#     "BusinessEntityAddress", # Depends on BusinessEntity, Address, AddressType

#     # Human Resources
#     "Employee", # Depends on Person
#     "EmployeeDepartmentHistory", # Depends on Employee, Department, Shift
#     "EmployeePayHistory", # Depends on Employee
#     # "JobCandidate", # Can depend on Employee (nullable FK)
#     "Document", # Depends on Employee (Owner) - HIERARCHYID (DocumentNode) needs careful handling if not text

#     # Production (Products and related components)
#     "ProductSubcategory", # Depends on ProductCategory
#     "ProductModel", # No direct FKs in its main columns, but linked from Product
#     "Product", # Depends on ProductModel, ProductSubcategory, UnitMeasure
#     "BillOfMaterials", # Depends on Product (ProductAssemblyID, ComponentID), UnitMeasure
#     "ProductCostHistory", # Depends on Product
#     "ProductListPriceHistory", # Depends on Product
#     # "ProductReview", # Depends on Product
#     "ProductInventory", # Depends on Product, Location
#     "ProductProductPhoto", # Depends on Product, ProductPhoto
#     "ProductModelIllustration", # Depends on ProductModel, Illustration
#     "ProductModelProductDescriptionCulture", # Depends on ProductModel, ProductDescription, Culture
#     "ProductDocument", # Depends on Product, Document
#     "WorkOrder", # Depends on Product, ScrapReason
#     "WorkOrderRouting", # Depends on WorkOrder, Product, Location
#     "TransactionHistory", # Depends on Product
#     "TransactionHistoryArchive", # Data comes from TransactionHistory, not direct FK usually

#     # Purchasing
#     "Vendor", # Depends on BusinessEntity
#     "PurchaseOrderHeader", # Depends on Employee, Vendor, ShipMethod
#     "PurchaseOrderDetail", # Depends on PurchaseOrderHeader, Product

#     # Sales (Core sales structures)
#     "CountryRegionCurrency", # Assuming FKs to CountryRegion, Currency. Create this table.
#     "CurrencyRate", # Depends on Currency (From and To)
#     "SalesTerritory", # Depends on CountryRegion
#     "SalesPerson", # Depends on Employee, SalesTerritory (nullable)
#     "Store", # Depends on BusinessEntity, SalesPerson (nullable)
#     "Customer", # Depends on Person (nullable), Store (nullable), SalesTerritory
#     "CreditCard",
#     "PersonCreditCard", # Depends on Person, CreditCard
#     "SpecialOfferProduct", # Depends on SpecialOffer, Product
#     "SalesOrderHeader", # Depends on Customer, SalesPerson (nullable), Territory, Address, ShipMethod, CreditCard (nullable), CurrencyRate (nullable)
#     "SalesOrderDetail", # Depends on SalesOrderHeader, Product, SpecialOffer (via SpecialOfferProduct or direct)
#     "SalesOrderHeaderSalesReason", # Depends on SalesOrderHeader, SalesReason
#     "SalesPersonQuotaHistory", # Depends on SalesPerson
#     "SalesTaxRate", # Depends on StateProvince
#     "SalesTerritoryHistory", # Depends on SalesPerson, SalesTerritory
#     "ShoppingCartItem" # Depends on Product
# ]

# DB_TO_PANDAS_TYPE_MAP: Dict[str, Union[str, type, callable]] = {
#     # Integer types
#     'integer': 'Int64', # Nullable integer
#     'smallint': 'Int64',
#     'bigint': 'Int64',

#     # Floating point types
#     'decimal': 'Float64', # Nullable float for numeric/decimal
#     'numeric': 'Float64',
#     'real': 'Float64',
#     'double precision': 'Float64',

#     # Boolean type
#     'boolean': 'boolean', # Nullable boolean

#     # String/Text types
#     'character varying': 'string', # Nullable string (requires pandas >= 1.0)
#     'varchar': 'string',
#     'text': 'string',
#     'character': 'string', # char

#     # Date and Time types
#     'date': 'datetime64[ns]', # pandas datetime
#     'timestamp without time zone': 'datetime64[ns]',
#     'timestamp with time zone': 'datetime64[ns]',
#     'time without time zone': 'object', # Time might need custom handling or kept as object/string
#     'time with time zone': 'object',

#     # UUID type (often stored as string or binary in DB, but we want uuid.UUID objects in Python)
#     'uuid': 'string', # Will handle conversion to uuid.UUID objects separately

#     # Other types (add as needed)
#     # 'json': 'object',
#     # 'jsonb': 'object',
#     # 'bytea': 'object', # Or 'bytes' dtype if applicable
#     # 'inet': 'object', # IP address types
#     # 'cidr': 'object',
#     # 'macaddr': 'object',
# }


# def parse_csv_file(csv_filepath):
#     """
#     Parse a CSV file and return its contents as a pandas DataFrame.
#     Tries multiple common encodings if the default/specified one fails.
#     """
#     # List of encodings to try, in order of likelihood based on common issues
#     # Put UTF-16 LE and UTF-8 SIG high up as they are common for non-ASCII datae
#     encodings_to_try = ['cp1252', 'utf-8-sig', 'utf-16','latin-1'] 
#     utf_16_encodings_file = ('BusinessEntityAddress', 'Employee', 'Person', 'EmailAddress', 'Password', \
#                             'PersonPhone', 'PhoneNumberType', 'ProductPhoto','BusinessEntity', 'ProductModel', \
#                             'CountryRegionCurrency', 'Store', 'Illustration', 'JobCandidate', 'Document', 'ProductDescription')
#     utf_8_encodings_file = ('ProductReview', 'Product', 'Location')


#     # Keep sep='\t' and header=None as they seem correct for your file format
#     read_csv_params = {'sep': '\t', 'header': None, 'low_memory': False} # Keep low_memory=False
#     filename = os.path.basename(csv_filepath).split('.')[0] # Get the base filename without extension
#     if not filename in utf_16_encodings_file and not filename in utf_8_encodings_file:
#         for encoding in encodings_to_try:
#             print(f"  Attempting to read {os.path.basename(csv_filepath)} with encoding='{encoding}'...")
#             try:
#                 # Use pandas to read the file with the current encoding
#                 df = pd.read_csv(csv_filepath, encoding=encoding, **read_csv_params)
#                 print(f"  Successfully read {os.path.basename(csv_filepath)} with encoding='{encoding}'.")
#                 print('columns list ',df.columns)
#                 return df # Success! Return the DataFrame

#             except UnicodeDecodeError as e:
#                 print(f"  Decode error with encoding='{encoding}': {e}")
#                 # Continue to the next encoding in the loop
#                 continue
#             except FileNotFoundError:
#                 print(f"  Error: File not found at {csv_filepath}")
#                 # No need to try other encodings if the file itself isn't found
#                 return None
#             except Exception as e:
#                 # Catch other potential errors during parsing (e.g., CSV format issues, permission errors)
#                 print(f"  An unexpected error occurred reading {os.path.basename(csv_filepath)} with encoding='{encoding}': {e}")
#                 # If it's not a decode error, the encoding might be correct but something else failed.
#                 # You might decide to stop trying encodings here or continue depending on how robust you need it.
#                 return None
#     elif filename in utf_8_encodings_file:
#         # If the filename is in the utf_8_encodings_file list, try UTF-8 SIG first
#         t_tab_files = []
#         file_encoding = 'utf-8'
#         params = {'sep': r'\t', 'header': None}  # Multiple separators r'\+\||\t'
#         # If the filename is in the utf_16_encodings_file list, try UTF-16 LE first
#         print(f"  Attempting to read {os.path.basename(csv_filepath)} with encoding='utf-8-sig'...")
#         cleaned_data = None
#         try:
#             # 1. Read the file in binary mode
#             with open(csv_filepath, 'rb') as f:
#                 raw_bytes = f.read()

#             try:
#                 decoded_string = raw_bytes.decode(file_encoding)
#                 print("File decoded successfully.")
#             except UnicodeDecodeError as e:
#                 print(f"UnicodeDecodeError during decoding: {e}")
#                 print("The file might not be strictly '{file_encoding}'. Trying with errors='ignore'.")
#                 decoded_string = raw_bytes.decode(file_encoding, errors='ignore')

#             cleaned_string = decoded_string.replace('\x00', '') # Remove null bytes
#             print("Invisible characters removed.")

#             # Use io.StringIO to make the cleaned string look like a file to pandas
#             data_io = io.StringIO(cleaned_string)

#             # Let pandas read from the StringIO object
#             # Don't pass encoding or low_memory when reading from StringIO
#             if os.path.basename(csv_filepath).split('.')[0] == 'ProductReview':
#                 params = {'sep': r'\t', 'header': None}
#                 df = pd.read_csv(data_io, sep=params['sep'], header=params['header'])
#             else:
#                 df = pd.read_csv(data_io, sep=params['sep'], header=params['header'])

#             print("File read successfully with pandas from cleaned data!")
#             return df # Success! Return the DataFrame

#         except FileNotFoundError:
#             print(f"Error: File not found at {csv_filepath}")
#             return None
#         except Exception as e:
#             print(f"\nAn unexpected error occurred: {e}")
#             print("Please double-check the file content and encoding.")
#             return None
 
#     else:
#         t_tab_files = ['Employee','CountryRegionCurrency','ProductDescription']
#         file_encoding = 'utf-16-le'
#         params = {'sep': r'\+\|', 'header': None}
#         # If the filename is in the utf_16_encodings_file list, try UTF-16 LE first
#         print(f"  Attempting to read {os.path.basename(csv_filepath)} with encoding='utf-16-le'...")
#         cleaned_data = None
#         try:
#             # Read the file in binary mode
#             with open(csv_filepath, 'rb') as f:
#                 raw_bytes = f.read()

#             # Decode the bytes using the specified encoding
#             try:
#                 decoded_string = raw_bytes.decode(file_encoding)
#                 print("File decoded successfully.")
#             except UnicodeDecodeError as e:
#                 print(f"UnicodeDecodeError during decoding: {e}")
#                 print("The file might not be strictly '{file_encoding}'. Trying with errors='ignore'.")
#                 decoded_string = raw_bytes.decode(file_encoding, errors='ignore')

#             # Clean the string - remove common invisible characters like null bytes
#             # The VS Code warning strongly suggests this is needed.
#             cleaned_string = decoded_string.replace('\x00', '') # Remove null bytes
#             print("Invisible characters removed.")

#             # Use io.StringIO to make the cleaned string look like a file to pandas
#             data_io = io.StringIO(cleaned_string)

#             # Let pandas read from the StringIO object
#             print(f"  debug 1 Attempting to read {os.path.basename(csv_filepath)} with encoding='utf-16-le'...")
#             if os.path.basename(csv_filepath).split('.')[0] in t_tab_files:
#                 print(f"  debug 2 Attempting to read 2 {os.path.basename(csv_filepath)} with encoding='utf-16-le'...")
#                 params = {'sep': r'\t', 'header': None}
#                 df = pd.read_csv(data_io, sep=params['sep'], header=params['header'])
#             else:
#                 df = pd.read_csv(data_io, sep=params['sep'], header=params['header'])


#             print("File read successfully with pandas from cleaned data!")
#             return df # Success! Return the DataFrame

#         except FileNotFoundError:
#             print(f"Error: File not found at {os.path.basename(csv_filepath)}")
#             return None
#         except Exception as e:
#             print(f"\nAn unexpected error occurred: {e}")
#             print("Please double-check the file content and encoding.")
#             return None
    

#     # If the loop finishes without returning, it means all attempted encodings failed to decode
#     print(f"  Failed to decode {os.path.basename(csv_filepath)} with all attempted encodings.")
#     return None
    
# def get_table_schema_with_schema(schema_name: str, table_name: str, conn: Any) -> Dict[str, str]:
#     column_info: Dict[str, str] = {}
#     try:
#         with conn.cursor() as cur:
#             cur.execute(
#                 """
#                 SELECT column_name, data_type
#                 FROM information_schema.columns
#                 WHERE table_schema = %s AND table_name = %s;
#                 """,
#                 (schema_name, table_name) # Pass schema and table name as separate parameters
#             )
#             results = cur.fetchall()
#             for row in results:
#                 column_info[row[0]] = row[1]
#         return column_info
#     except Exception as e:
#         print(f"Error fetching schema for {schema_name}.{table_name}: {e}")
#         return {}


# def rename_dataframe_columns_from_schema(df: pd.DataFrame, db_schema: Dict[str, str]) -> pd.DataFrame:
#     """
#     Renames DataFrame columns (expected to be 0, 1, 2, ...) using keys
#     from a database schema dictionary.

#     Args:
#         df (pd.DataFrame): The DataFrame with default integer column names.
#         db_schema (Dict[str, str]): A dictionary where keys are the desired
#                                      column names in the correct order, and
#                                      values are database data types.

#     Returns:
#         pd.DataFrame: The DataFrame with columns renamed according to the schema keys.

#     Raises:
#         ValueError: If the number of columns in the DataFrame does not match
#                     the number of keys in the schema dictionary.
#     """
#     print('few data',df.columns)

#     # Get the column names from the dictionary keys.
#     # Dictionaries maintain insertion order in Python 3.7+
#     new_column_names = list(db_schema.keys())

#     # Validate that the number of columns matches the number of keys
#     if len(df.columns) != len(new_column_names):
#         raise ValueError(
#             f"Column count mismatch: DataFrame has {len(df.columns)} columns (0 to {len(df.columns)-1}), "
#             f"but schema dictionary has {len(new_column_names)} keys. "
#             f"Cannot rename columns if counts don't match."
#         )

#     # Assign the new column names to the DataFrame's columns attribute
#     df.columns = new_column_names

#     print(f"Successfully renamed {len(df.columns)} columns.")
#     return df

# def convert_to_uuid_or_none(value: Any) -> Union[uuid.UUID, None]:
#     """Helper to convert a value to UUID or None, handling errors and empty strings."""
#     # Check for pandas NaN or None, or empty string after stripping
#     if pd.isna(value) or str(value).strip() == '':
#          return None
#     try:
#         # Attempt conversion, stripping potential braces {}
#         return uuid.UUID(str(value).strip('{}'))
#     except (ValueError, TypeError):
#         # If conversion fails (invalid UUID string), return None
#         # print(f"Warning: Could not convert '{value}' to UUID.") # Optional warning
#         return None

# def convert_dataframe_columns_to_db_types(df: pd.DataFrame, db_schema: Dict[str, str]) -> pd.DataFrame:
#     """
#     Converts DataFrame column data types based on a database schema dictionary.

#     Assumes DataFrame columns have already been renamed to match db_schema keys.

#     Args:
#         df (pd.DataFrame): The DataFrame with columns named as in db_schema.
#         db_schema (Dict[str, str]): A dictionary {column_name: db_data_type_string}.
#                                      Ensure this dictionary does NOT contain the
#                                      incorrectly named column.

#     Returns:
#         pd.DataFrame: The DataFrame with converted column dtypes.
#                       Returns a copy, does not modify the original DataFrame.
#     """
#     # Work on a copy to avoid modifying the original DataFrame
#     df_converted = df.copy()

#     print("\nStarting data type conversion based on DB schema...")

#     # Iterate through the schema dictionary items (column_name, db_type_str)
#     for col_name, db_type_str in db_schema.items():
#         # IMPORTANT: Ensure the malformed column name is NOT in your db_schema dictionary
#         # when you call this function.

#         # Check if the column exists in the DataFrame (it should if renaming worked)
#         if col_name not in df_converted.columns:
#             print(f"  Warning: Column '{col_name}' from schema not found in DataFrame. Skipping type conversion for this column.")
#             continue

#         # Get the target pandas type/conversion hint from our mapping
#         # Use lower() for case-insensitivity when matching DB type names
#         pandas_target_hint = DB_TO_PANDAS_TYPE_MAP.get(db_type_str.lower())

#         if pandas_target_hint is None:
#             print(f"  Warning: Database type '{db_type_str}' for column '{col_name}' is not mapped. Leaving column as '{df_converted[col_name].dtype}'.")
#             continue # Skip conversion if the database type is not in our map

#         current_dtype = df_converted[col_name].dtype
#         # print(f"  Processing column '{col_name}' (DB type: '{db_type_str}', Current pandas dtype: '{current_dtype}')...")

#         try:
#             # --- Apply conversion based on the target hint ---

#             if pandas_target_hint == 'datetime64[ns]':
#                 # Use pd.to_datetime for date/timestamp types
#                 # errors='coerce' will turn values that cannot be parsed into NaT (Not a Time)
#                 # format=... could be added here if you know the exact format for performance/reliability
#                 df_converted[col_name] = pd.to_datetime(df_converted[col_name], errors='coerce')

#             elif pandas_target_hint == 'object' and db_type_str.lower() == 'uuid':
#                 # Custom conversion for UUIDs using .apply
#                 # .apply is good for element-wise transformations
#                 df_converted[col_name] = df_converted[col_name].apply(convert_to_uuid_or_none)
#                  # Check if the resulting dtype is still 'object' as expected for UUID objects
#                 if df_converted[col_name].dtype != 'object':
#                      print(f"  Warning: UUID conversion for column '{col_name}' did not result in 'object' dtype. Resulting dtype: '{df_converted[col_name].dtype}'")

#             elif isinstance(pandas_target_hint, str):
#                  # Use astype for standard pandas dtype strings ('Int64', 'string', 'boolean', 'Float64')
#                  # Check if the current dtype is already the target dtype (case-insensitive check for nullable types)
#                  if str(current_dtype).lower() == pandas_target_hint.lower():
#                      # print(f"  Column '{col_name}' is already {current_dtype}, skipping conversion.")
#                      pass # Already the correct type
#                  else:
#                      # For nullable types, astype handles conversion from object/string and None/NaN
#                      # For safety, sometimes converting to object first can help if source is complex
#                      # if current_dtype != object and not pd.api.types.is_string_dtype(current_dtype):
#                      #     df_converted[col_name] = df_converted[col_name].astype(object)

#                      df_converted[col_name] = df_converted[col_name].astype(pandas_target_hint)
#             elif isinstance(pandas_target_hint, int):
#                 # If the target hint is an int, we can use pd.to_numeric
#                 # This is useful for integer types, but be careful with NaNs
#                 df_converted[col_name] = pd.to_numeric(df_converted[col_name], errors='coerce').astype(pandas_target_hint)
#             elif isinstance(pandas_target_hint, float): 
#                 # If the target hint is a float, we can use pd.to_numeric
#                 # This is useful for float types, but be careful with NaNs
#                 df_converted[col_name] = pd.to_numeric(df_converted[col_name], errors='coerce').astype(pandas_target_hint)
#             elif isinstance(pandas_target_hint, bool):      
#                 # If the target hint is a boolean, we can use astype(bool)
#                 # This is useful for boolean types, but be careful with NaNs
#                 df_converted[col_name] = df_converted[col_name].astype(bool)
#         except Exception as e:
#             # Catch any errors during the conversion process for this column
#             print(f"  Error converting column '{col_name}' (DB type '{db_type_str}') to target type. Error: {e}. Leaving column as '{df_converted[col_name].dtype}'.")
#             # If an error occurs during conversion of a column, leave it as is and continue with the next column.
#             continue

#     return df_converted

# def get_primary_key_columns(cursor, schema_name, table_name):
#     """
#     Retrieves the column names that make up the primary key for a given table.

#     Args:
#         cursor: A psycopg2 cursor object.
#         schema_name: The name of the database schema (e.g., 'public').
#         table_name: The name of the table.

#     Returns:
#         A list of column names if a primary key exists, ordered correctly
#         for composite keys. Returns None if no primary key is found.
#     """
#     query = """
#         SELECT kcu.column_name
#         FROM information_schema.table_constraints tc
#         JOIN information_schema.key_column_usage kcu
#           ON tc.constraint_name = kcu.constraint_name
#          AND tc.table_schema = kcu.table_schema
#          AND tc.table_name = kcu.table_name
#         WHERE tc.constraint_type = 'PRIMARY KEY'
#           AND tc.table_schema = %s
#           AND tc.table_name = %s
#         ORDER BY kcu.ordinal_position;
#     """
#     try:
#         cursor.execute(query, (schema_name, table_name))
#         pk_columns = [row[0] for row in cursor.fetchall()]
#         if pk_columns:
#             return pk_columns
#         else:
#             return None # No primary key found
#     except Exception as e:
#         print(f"Error retrieving primary key for {schema_name}.{table_name}: {e}")
#         # Depending on your needs, you might want to raise the exception
#         # or return None to indicate failure.
#         return None



# def insert_dataframe_in_chunks(
#     df: pd.DataFrame,
#     conn: Any, # Type hint for connection object (e.g., psycopg2.extensions.connection)
#     table_name: str, # Table name, can include schema (e.g., 'Sales.Address')
#     db_schema: Dict[str, str], # <--- NEW: Pass the DB schema dictionary
#     chunk_size: int = 1000, # Number of rows per batch insert
#     disable_fk_checks: bool = True,
#     use_on_conflict_do_nothing: bool = False

# ) -> None:
#     """
#     Inserts a pandas DataFrame into a PostgreSQL table in specified chunks.

#     Args:
#         df (pd.DataFrame): The DataFrame to insert.
#         conn: The active psycopg2 database connection object.
#         table_name (str): The name of the target table (e.g., 'my_table' or 'schema.my_table').
#         chunk_size (int): The number of rows to insert in each batch.

#     Raises:
#         Exception: If a database error occurs during insertion.
#         ValueError: If the DataFrame is empty.
#     """
#     if df.empty:
#         print(f"DataFrame is empty. Skipping insertion for table {table_name}.")
#         return

#     # Get column names from the DataFrame
#     df_columns = df.columns.tolist()
#     num_columns = len(df_columns)

#     if num_columns == 0:
#         print(f"DataFrame has no columns. Skipping insertion for table {table_name}.")
#         return

#         # --- IMPROVED REPLACEMENT STEP: Replace ALL pandas NaT/NaN with None ---
#     # This method is more generic and replaces any pandas missing value indicator
#     # (NaN for numbers, NaT for datetimes, NA for nullable types) with None.
#     print("Starting replacement of ALL pandas missing values (NaN/NaT/NA) with None...")
#     df_cleaned = df.copy() # Work on a copy

#     # Iterate through all columns
#     for col in df_cleaned.columns:
#         target_db_type_str = db_schema.get(col, '').lower()
#         pandas_target_hint = DB_TO_PANDAS_TYPE_MAP.get(target_db_type_str)


#         # # Check if NaT/NaN/NA exists in this column before replacement
#         # has_missing_before = df_cleaned[col].isna().any()

#         # if has_missing_before:
#         #      # Replace all pandas missing values in this column with None
#         #      df_cleaned[col] = df_cleaned[col].replace({pd.NA: None, pd.NaT: None})
#         #      df_cleaned.loc[df_cleaned[col].isna(), col] = None
#         # --- Replace pandas/numpy missing values (NaN, NaT, NA) with None ---
#         # Do this first, so subsequent conversions don't fail on missing values
#         if df_cleaned[col].isna().any():
#             print(f"  Replacing missing values with None in column '{col}'...")
#             df_cleaned.loc[df_cleaned[col].isna(), col] = None


#         # --- **NEW/IMPROVED: Handle Timestamp/Date Columns Explicitly** ---
#         if pandas_target_hint == 'datetime64[ns]':
#             print(f"  Standardizing datetime column '{col}'...")
#             # Use pd.to_datetime with errors='coerce'. This turns invalid parsing
#             # (including the string 'NaT' or other garbage) into pd.NaT.
#             # We then replace pd.NaT with None below.
#             # Add common formats if known for speed/reliability: format=['%Y-%m-%d %H:%M:%S', '%Y-%m-%d']
#             df_cleaned[col] = pd.to_datetime(df_cleaned[col], errors='coerce')

#             # Now replace the resulting pd.NaT values (from original NaT or coerced errors) with None
#             # This covers original missing values AND values that failed pd.to_datetime
#             if df_cleaned[col].isna().any(): # Check again for NaT after conversion
#                  # print(f"  Replacing pd.NaT (including coerced errors) with None in '{col}'...") # Verbose
#                  df_cleaned.loc[df_cleaned[col].isna(), col] = None


# # --- Specific handling for boolean columns (Keep this) ---
#         # Ensure values are standard Python bool or None
#         if pd.api.types.is_bool_dtype(df_cleaned[col].dtype):
#              print(f"  Standardizing boolean column '{col}'...")
#              df_cleaned[col] = df_cleaned[col].apply(
#                  lambda x: True if x is True or (isinstance(x, np.bool_) and x == True) else (False if x is False or (isinstance(x, np.bool_) and x == False) else None)
#              )

#         # Verify replacement for NaT specifically if it's a datetime column
#         if df_cleaned[col].dtype == 'datetime64[ns]':
#             has_pd_nat_after = (df_cleaned[col] == pd.NaT).any()
#             if has_pd_nat_after:
#                  print(f"  CRITICAL DEBUG: Column '{col}' (datetime) *still* contains pd.NaT after replacement.")
#             else:
#                  print(f"  Debug: Column '{col}' (datetime) successfully replaced pd.NaT with None.") # Confirm success
        
#         # --- **NEW: Standardize Numeric Columns** ---
#         # Convert numpy numeric types to standard Python int/float or None
#         # Check for *any* numeric dtype (int, float, both numpy and pandas nullable)
#         if pd.api.types.is_numeric_dtype(df_cleaned[col].dtype):
#             # Exclude boolean dtype which is also numeric according to pandas
#             if not pd.api.types.is_bool_dtype(df_cleaned[col].dtype):
#                 print(f"  Standardizing numeric column '{col}' (dtype: {df_cleaned[col].dtype})...")
#                 # Use apply to convert each non-None value to float (safer for numeric/decimal)
#                 # or int if you are certain it should be integer
#                 # The isna() check above ensures we don't try float(None)
#                 df_cleaned[col] = df_cleaned[col].apply(lambda x: float(x) if x is not None else None)
#                 # If you know it MUST be an integer and want Python int:
#                 # df_cleaned[col] = df_cleaned[col].apply(lambda x: int(x) if x is not None else None)

#     print(f"modifieddate of DataFrame: {df_cleaned['modifieddate']}")

#     print("Replacement complete. Final dtypes of cleaned DataFrame:")
#     # --- END IMPROVED REPLACEMENT STEP ---
#     # --- CORRECTED: Construct the INSERT SQL statement using psycopg2.sql ---
#     # Correctly handle schema.table notation using sql.Identifier with multiple arguments
#     if '.' in table_name:
#         # Split into schema and table name
#         schema_name, simple_table_name = table_name.split('.', 1) # Split only on the first dot
        
#         # Create ONE identifier object for the qualified name
#         table_ident = sql.Identifier(schema_name, simple_table_name)
#     else:
#         # No schema specified, just use the table name
#         table_ident = sql.Identifier(table_name)
#     # strip schema from table name
#     table_name = table_name.split('.')[1] if '.' in table_name else table_name
#     # Column identifiers (use original df_columns names)
#     column_idents = [sql.Identifier(col) for col in df_columns]
#     column_list = sql.SQL(', ').join(column_idents)

#     # Placeholders
#     placeholders = sql.SQL(', ').join(sql.Placeholder() * num_columns)

#     # --- CORRECTED: Use ONE placeholder for the table identifier in the template ---
#     insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
#         table_ident,   # <--- This is ONE Composable object representing "schema"."table"
#         column_list,   # <--- This is a Composable object
#         placeholders   # <--- This is a Composable object
#     )



#         # --- SQL commands for disabling/enabling triggers ---
#     disable_triggers_sql = sql.SQL("ALTER TABLE {} DISABLE TRIGGER ALL;").format(table_ident)
#     enable_triggers_sql = sql.SQL("ALTER TABLE {} ENABLE TRIGGER ALL;").format(table_ident)


#     print(f"\nStarting bulk insert into {table_name} in chunks of {chunk_size}...")
#     # print(f"Total rows to insert: {len(df_cleaned)}")

#     num_rows = len(df_cleaned)
#     num_chunks = (num_rows + chunk_size - 1) // chunk_size

#     # --- Main try...finally block to ensure triggers are re-enabled ---
#     try:
#         # --- Disable FK checks if requested ---
#         if disable_fk_checks:
#             print(f"  Disabling triggers (including FK checks) for {table_name}...")
#             with conn.cursor() as cur:
#                  cur.execute(disable_triggers_sql)
#                  conn.commit() # Commit the ALTER TABLE command
#             print(f"  Triggers disabled.")

#         # --- Insertion loop ---
#         with conn.cursor() as cur:
#             # --- Dynamically get Primary Key columns ---
#             pk_columns = get_primary_key_columns(cur, schema_name, table_name)

#             if pk_columns is None:
#                 # Option A: Print warning and skip ON CONFLICT (inserts will fail on PK violation)
#                 print(f"  Warning: Table {schema_name}.{table_name} has no primary key. Cannot use ON CONFLICT.")
#                 on_conflict_clause = sql.SQL("") # Empty string means no ON CONFLICT
#             else:
#                 print(f"  Primary key for {table_name}: {pk_columns}. Using ON CONFLICT DO NOTHING.")
#                 pk_columns_sql = sql.SQL(", ").join(map(sql.Identifier, pk_columns))
#                 on_conflict_clause = sql.SQL("ON CONFLICT ({pk_cols}) DO NOTHING").format(
#                     pk_cols=pk_columns_sql
#                 )

#             # Get column names from the DataFrame to build the insert statement safely
#             # Assuming DataFrame column names match database column names
#             df_columns = df_cleaned.columns.tolist()
#             columns_sql = sql.SQL(", ").join(map(sql.Identifier, df_columns ))
#             values_sql = sql.SQL(", ").join(sql.Placeholder() * len(df_columns)) # Use Placeholder() for each column

#             # Construct the INSERT statement with the dynamic ON CONFLICT clause
#             insert_sql = sql.SQL("""
#                 INSERT INTO {table} ({columns})
#                 VALUES ({values})
#                 {on_conflict_clause};
#             """).format(
#                 table=sql.Identifier(schema_name, table_name), # Include schema in table identifier
#                 columns=columns_sql,
#                 values=values_sql,
#                 on_conflict_clause=on_conflict_clause
#             )
            

#             for i in range(num_chunks):
#                 start_index = i * chunk_size
#                 end_index = min((i + 1) * chunk_size, num_rows)

#                 chunk_df = df_cleaned.iloc[start_index:end_index]
#                 # Convert the DataFrame chunk to a list of tuples
#                 # Ensure the order matches the columns_sql
#                 data_for_batch = list(chunk_df.itertuples(index=False, name=None))

#                 if table_name == 'salespersonquotahistory':
#                     print('this is table_name ',table_name)
#                     print(f"  debug 1: {insert_sql}")
#                     print(f"  debug 2: {data_for_batch}")

#                 if not data_for_batch:
#                     print(f"  Warning: Batch {i+1}/{num_chunks} is empty. Skipping.")
#                     continue

#                 try:
#                     # executemany with ON CONFLICT DO NOTHING will not raise an exception
#                     # for primary key or unique constraint violations on the specified columns.
#                     cur.executemany(insert_sql, data_for_batch)

#                     # Note: You won't know how many rows were skipped due to conflict
#                     # directly from executemany. The print message reflects this handling.
#                     status_msg = f"  Processed chunk {i+1}/{num_chunks} ({len(data_for_batch)} rows)."
#                     if pk_columns:
#                         status_msg += f" Conflicts on {', '.join(pk_columns)} skipped."
#                     print(status_msg)

#                     conn.commit() # Commit the transaction for this batch

#                 except Exception as e:
#                     # This block catches errors OTHER THAN unique constraint violations
#                     # handled by ON CONFLICT (e.g., data type errors, foreign key errors).
#                     conn.rollback() # Rollback the failed batch
#                     print(f"\n  Error inserting chunk {i+1}/{num_chunks} starting at row {start_index} into {table_name}. Rolling back batch.")
#                     print(f"  Error details: {e}")

#                     # Decide what to do here:
#                     # If loading multiple tables in an outer loop:
#                     #   - Raise the exception: Stops processing *this table* entirely, 
#                     #     allowing the outer loop to catch it and potentially move to the next table.
#                     raise e 

#                     # If processing only one table, and you want to just skip the bad chunk:
#                     #   - Remove 'raise e' and add a 'continue' to move to the next chunk.
#                     #   continue # Skip this bad chunk and try the next one

#         print(f"Finished insertion into {schema_name}.{table_name}.")

#     except Exception as e:
#         # This catches errors from the outer try block or re-raised exceptions
#         print(f"\nLoading process for table {schema_name}.{table_name} halted due to an error.")
#         print(f"Details: {e}")
#         # If this is part of a larger script loading multiple tables,
#         # the outer loop would catch this and decide whether to stop entirely
#         # or move to the next table.

#     finally:
#         # --- Ensure FK checks are re-enabled ---
#         if disable_fk_checks:
#             print(f"  Re-enabling triggers for {table_name}...")
#             try:
#                 with conn.cursor() as cur:
#                     cur.execute(enable_triggers_sql)
#                     conn.commit() # Commit the ALTER TABLE command
#                 print(f"  Triggers re-enabled.")
#             except Exception as e:
#                  print(f"  ERROR: Failed to re-enable triggers for {table_name}: {e}")
#                  print("  Manual intervention required to ENABLE TRIGGER ALL for this table!")
#                  # Note: You might want to log this critical error properly



# def main():
#     conn = None
#     try:
#         conn = psycopg2.connect(**DB_CONFIG)
#         print("Successfully connected to PostgreSQL database.")

#         # Process files in the defined order
#         for base_filename in TABLE_LOAD_ORDER:
#             csv_file_path = os.path.join(CSV_DIR, f"{base_filename}.csv")
#             if base_filename in TABLE_SCHEMA_MAPPING:
#                 full_table_name = TABLE_SCHEMA_MAPPING[base_filename].lower()
#                 if os.path.exists(csv_file_path):
#                     print(f"Processing file: {base_filename}")
#                     schema_name = full_table_name.split('.')[0].lower()
#                     table_name = full_table_name.split('.')[1].lower()
#                     # Check if the table is already created
#                     column_from_db = get_table_schema_with_schema(schema_name, table_name, conn)
#                     try:
#                         contents = parse_csv_file(csv_file_path)
#                         if contents is None:
#                             print(f"  Error parsing file {os.path.basename(csv_file_path)}. Skipping.")
#                             return
#                         # Check if the DataFrame is empty
#                         if contents.empty:
#                             print(f"  File {os.path.basename(csv_file_path)} is empty. Skipping.")
#                             return
#                         # Check if the DataFrame has any rows
#                         if contents.shape[0] == 0:
#                             print(f"  File {os.path.basename(csv_file_path)} has no rows. Skipping.")
#                             return
                        
#                     except Exception as e:
#                         print(f"Error processing file {csv_file_path}: {e}")
#                         return
            
#                     # Get number of columns from database
#                     columns_from_db = len(list(column_from_db.keys()))
#                     new_df = rename_dataframe_columns_from_schema(contents.copy(), column_from_db)
#                     converted_new_df = convert_dataframe_columns_to_db_types(new_df.copy(), column_from_db)
#                     # Load the DataFrame into the database
#                     insert_dataframe_in_chunks(converted_new_df, conn, full_table_name,column_from_db)
                        
#                 else:
#                     print(f"CSV file {csv_file_path} not found, but was in load order. Skipping.")
#             else:
#                 print(f"Warning: Filename '{base_filename}' is in load order but not in TABLE_SCHEMA_MAPPING. Skipping.")
        
#     except psycopg2.Error as e:
#         print(f"Database connection error: {e}")
#     except Exception as e:
#         print(f"An unexpected error occurred: {e}")
#     finally:
#         if conn:
#             conn.close()
#             print("Database connection closed.")

# if __name__ == "__main__":
#     if not os.path.isdir(CSV_DIR):
#         print(f"Error: CSV directory '{CSV_DIR}' not found. Please create it and add your CSV files.")
#     else:
#         main()

# import pandas as pd
# import psycopg2
# from psycopg2 import sql
# import numpy as np
# from typing import Any, Optional, List, Dict, Union
# import io
# import os
# import glob
# import datetime
# import uuid

# # --- Database Configuration (Keep this) ---
# DB_CONFIG = {
#     "host": "localhost",
#     "port ": "5433",
#     "dbname": "postgres",
#     "user": "data_eng",
#     "password": "12345pP"
# }

# # --- CSV Files Directory (Keep this) ---
# CSV_DIR = "/home/blueberry/Desktop/advanced_sql_ tutorial/data"

# # --- Table Schema Mapping (Keep this) ---
# TABLE_SCHEMA_MAPPING = {
#     "Address": "Person.Address",
#     "AddressType": "Person.AddressType",
#     "AWBuildVersion": "dbo.AWBuildVersion",
#     "BillOfMaterials": "Production.BillOfMaterials",
#     "BusinessEntity": "Person .BusinessEntity",
#     "BusinessEntityAddress": "Person.BusinessEntityAddress",
#     "BusinessEntityContact": "Person.BusinessEntityContact",
#     "ContactType": "Person.ContactType",
#     "Country Region": "Person.CountryRegion",
#     "CreditCard": "Sales.CreditCard",
#     "Culture": "Production.Culture",
#     "Currency": "Sales.Currency",
#     "CurrencyRate": "Sales .CurrencyRate",
#     "Customer": "Sales.Customer",
#     "Department": "HumanResources.Department",
#     "Document": "Production.Document",
#     "EmailAddress": "Person.EmailAddress",
#      "Employee": "HumanResources.Employee",
#     "EmployeeDepartmentHistory": "HumanResources.EmployeeDepartmentHistory",
#     "EmployeePayHistory": "HumanResources.EmployeePayHistory",
#     "Illustration": "Production. Illustration",
#     # "JobCandidate": "HumanResources.JobCandidate",
#     "Location": "Production.Location",
#     "Password": "Person.Password",
#     "Person": "Person.Person",
#     "PersonCreditCard": "Sales.PersonCreditCard",
#     "PersonPhone": "Person.PersonPhone",
#     "PhoneNumberType": "Person.PhoneNumberType",
#     "Product": "Production.Product",
#     "ProductCategory": "Production.ProductCategory",
#     "ProductCostHistory": "Production.ProductCostHistory",
#     "ProductDescription": "Production.ProductDescription",
#     "ProductDocument": "Production.Product Document",
#     "ProductInventory": "Production.ProductInventory",
#     "ProductListPriceHistory": "Production.ProductListPriceHistory",
#     "ProductModel": "Production.ProductModel",
#     "ProductModelIllustration ": "Production.ProductModelIllustration",
#     "ProductModelProductDescriptionCulture": "Production.ProductModelProductDescriptionCulture",
#     "ProductPhoto": "Production.ProductPhoto",
#     # "ProductReview": "Production.ProductReview",
#     "ProductSubcategory": "Production.ProductSubcategory",
#     "PurchaseOrderDetail": "Purchasing.PurchaseOrderDetail",
#     "PurchaseOrderHeader": "Purchasing.PurchaseOrderHeader",
#     "Sales OrderDetail": "Sales.SalesOrderDetail",
#     "SalesOrderHeader": "Sales.SalesOrderHeader",
#     "SalesOrderHeaderSalesReason": "Sales.SalesOrderHeaderSalesReason",
#     "SalesPerson": "Sales .SalesPerson",
#     "SalesPersonQuotaHistory": "Sales.SalesPersonQuotaHistory",
#     "SalesReason": "Sales.SalesReason",
#     "SalesTaxRate": "Sales.SalesTaxRate",
#      "SalesTerritory": "Sales.SalesTerritory",
#     "SalesTerritoryHistory": "Sales.SalesTerritoryHistory",
#     "ScrapReason": "Production.ScrapReason",
#     "Shift": " HumanResources.Shift",
#     "ShipMethod": "Purchasing.ShipMethod",
#     "ShoppingCartItem": "Sales.ShoppingCartItem",
#     "SpecialOffer": "Sales.SpecialOffer",
#     "SpecialOfferProduct ": "Sales.SpecialOfferProduct",
#     "StateProvince": "Person.StateProvince",
#     "Store": "Sales.Store",
#     "TransactionHistory": "Production.TransactionHistory",
#     "TransactionHistoryArchive ": "Production.TransactionHistoryArchive",
#     "UnitMeasure": "Production.UnitMeasure",
#     "Vendor": "Purchasing.Vendor",
#     "WorkOrder": "Production.WorkOrder",
#     "WorkOrder Routing": "Production.WorkOrderRouting",
#     "CountryRegionCurrency": "Sales.CountryRegionCurrency"
# }

# # --- Define the order of loading tables (Keep this, verify it's correct for FKs) ---
# TABLE_LOAD_ORDER = [
#     "AWBuildVersion", "CountryRegion", "AddressType", "ContactType", "PhoneNumberType", "UnitMeasure",
#     "ProductCategory", "Culture", "Currency", "Department", "Shift", " Illustration", "Location",
#     "ProductDescription", "ProductPhoto", "ScrapReason", "ShipMethod", "SpecialOffer", "SalesReason",
#     "BusinessEntity", "Person", "Password", "EmailAddress",  "PersonPhone", "StateProvince",
#     "Address", "BusinessEntityAddress",
#     "Employee", "EmployeeDepartmentHistory", "EmployeePayHistory", # "JobCandidate",
#     "Document",
#     "ProductSubcategory", "Product Model", "Product", "BillOfMaterials", "ProductCostHistory",
#     "ProductListPriceHistory", # "ProductReview",
#     "ProductInventory", "ProductProductPhoto", "ProductModelIllustration",
#     "ProductModelProductDescription Culture", "ProductDocument", "WorkOrder", "WorkOrderRouting",
#     "TransactionHistory", "TransactionHistoryArchive",
#     "Vendor", "PurchaseOrderHeader", "PurchaseOrderDetail",
#     "CountryRegionCurrency", " CurrencyRate", "SalesTerritory", "SalesPerson", "Store", "Customer",
#     "CreditCard", "PersonCreditCard", "SpecialOfferProduct", "SalesOrderHeader", "SalesOrderDetail",
#     "SalesOrderHeaderSalesReason",  "SalesPersonQuotaHistory", "SalesTaxRate", "SalesTerritoryHistory",
#     "ShoppingCartItem"
# ]

# # --- Mapping database type strings to pandas dtypes or conversion functions (Keep this) ---
# # Ensure this mapping is comprehensive  for your database types
# DB_TO_PANDAS_TYPE_MAP: Dict[str, Union[str, type, callable]] = {
#     'integer': 'Int64', 'smallint': 'Int6 4', 'bigint': 'Int64',
#     'decimal': 'Float64', 'numeric': 'Float64', 'real': 'Float64', 'double precision': 'Float64',
#      'boolean': 'boolean',
#     'character varying': 'string', 'varchar': 'string', 'text': 'string', 'character': 'string',
#     'date': 'datetime64[ns]', 'timestamp without  time zone': 'datetime64[ns]', 'timestamp with time zone': 'datetime64[ns]',
#     'time without time zone': 'object', 'time with time zone': 'object', # Time might need custom handling
#      'uuid': 'string', # Pass as string, let DB cast
#     # Add other types as needed based on your schema
#     # 'bytea': 'object',
#     # 'json': 'object',
#     # ' jsonb': 'object',
# }


# # --- parse_csv_file function (Keep your current implementation, but ensure it's robust) ---
# # Your implementation with specific encoding lists and handling null bytes is below.
# # It 's complex but seems necessary for your data. Ensure the logic correctly
# # returns a DataFrame with columns that pandas *could* infer or are left as 'object'.
# # The cleaning in insert_dataframe_in_chunks will handle converting  these to target types.

# def parse_csv_file(csv_filepath):
#     """
#     Parse a CSV file and return its contents as a pandas DataFrame.
#     Tries multiple common encodings if the default/specified one fails.
#     Includes specific handling for known file encodings and null bytes.
#     """
#     # List of encodings to try (fallback if specific lists don't cover it)
#     encodings_to_try = ['cp1252 ', 'utf-8-sig', 'utf-16-le', 'utf-16', 'latin-1'] # Added utf-16-le and utf-16 here too

#     # Lists for specific file  encodings based on your analysis
#     utf_16_le_files = ('BusinessEntityAddress', 'Employee', 'Person', 'EmailAddress', 'Password',
#                        'PersonPhone', 'PhoneNumberType', 'ProductPhoto ', 'BusinessEntity', 'ProductModel',
#                        'CountryRegionCurrency', 'Store', 'Illustration', 'JobCandidate', 'Document', 'ProductDescription')
#     utf_8_files = ('ProductReview', 'Product',  'Location') # Assuming UTF-8 SIG or just UTF-8

#     # Keep sep='\t' and header=None
#     read_csv_params = {'sep': '\t', 'header': None, 'low_memory':  False}

#     filename = os.path.basename(csv_filepath).split('.')[0]

#     # Determine the primary encoding strategy
#     primary_encoding = None
#     if filename in utf_16_le_files:
#         primary_encoding = 'utf-16-le'
#         print(f"  Using primary encoding '{primary_encoding}' for {filename}...")
#     elif filename in utf_8_files:
#         primary_encoding = 'utf-8 -sig' # Use utf-8-sig for safety with BOM
#         print(f"  Using primary encoding '{primary_encoding}' for {filename}...")
#     else:
#         # If not in specific lists, use  the general trial list
#         print(f"  File {filename} not in specific encoding lists. Trying general list...")
#         pass # Will proceed to the loop below

#     # Attempt reading with the primary encoding first if determined
#     if primary_encoding :
#         try:
#             # Use binary read + decode + StringIO for explicit null byte cleaning
#             with open(csv_filepath, 'rb') as f:
#                 raw_bytes = f.read()

#             #  Decode, handling potential errors by ignoring bad characters
#             try:
#                 decoded_string = raw_bytes.decode(primary_encoding)
#                 # print("File decoded successfully.") # Verbose
#             except UnicodeDecodeError as e:
#                 print(f"  UnicodeDecodeError during primary decoding ({primary_encoding}): {e}")
#                 print(f"  Trying '{primary_encoding}' with errors='ignore'.")
#                 decoded_string = raw_bytes.decode(primary_encoding, errors='ignore')

#             # Clean the string - remove null bytes (\x00) which are common in UTF-16 exports
#             cleaned_string = decoded_string.replace('\x00', '')
#              # print("Invisible characters removed.") # Verbose

#             # Use io.StringIO to make the cleaned string look like a file to pandas
#             data_io = io.StringIO(cleaned_string)

#             # Let pandas read from the StringIO object . No encoding needed here.
#             df = pd.read_csv(data_io, **read_csv_params)

#             print(f"  Successfully read {os.path.basename(csv_filepath)} with encoding ='{primary_encoding}' after cleaning.")
#             # print('columns list ',df.columns) # Verbose
#             return df # Success!

#         except FileNotFoundError:
#             print(f"  Error: File not found at  {csv_filepath}")
#             return None
#         except Exception as e:
#             # Catch other errors during primary attempt
#             print(f"  An unexpected error occurred reading {os.path.basename(csv_filepath)}  with primary encoding '{primary_encoding}': {e}")
#             print("  Trying general encoding list as fallback.")
#             # Fall through to the loop below

#     # If primary attempt failed or was skipped, try the general list
#     for encoding  in encodings_to_try:
#         # Skip the encoding already tried if it failed
#         if encoding == primary_encoding:
#              continue

#         print(f"  Attempting to read {os.path.basename(csv_filepath )} with encoding='{encoding}' (fallback)...")
#         try:
#             # Use binary read + decode + StringIO for explicit null byte cleaning
#             with open(csv_filepath, 'rb') as f:
#                 raw_bytes = f.read()

#             # Decode, handling potential errors by ignoring bad characters
#             try:
#                 decoded_string = raw_bytes.decode(encoding)
#                 # print("File decoded successfully.") # Verbose
#             except UnicodeDecodeError as e:
#                 print(f"  UnicodeDecodeError during fallback decoding ({encoding}): {e}")
#                 print(f"  Trying '{encoding}' with errors='ignore'.")
#                 decoded_string = raw_bytes.decode(encoding, errors='ignore')

#             # Clean the string - remove null bytes
#             cleaned_string = decoded_string.replace('\x00', '')

#             # Use io.StringIO
#             data_io = io.StringIO(cleaned_string)

#             # Let pandas read from the StringIO object
#             df = pd.read_csv(data_io, **read_csv_params)

#             print (f"  Successfully read {os.path.basename(csv_filepath)} with encoding='{encoding}' after cleaning (fallback).")
#             # print('columns list ',df.columns) # Verbose
#             return df  # Success!

#         except FileNotFoundError:
#              print(f"  Error: File not found at {csv_filepath}")
#              return None # File not found is a terminal error

#         except Exception as e:
#             #  Catch other errors during fallback attempt
#             print(f"  An unexpected error occurred reading {os.path.basename(csv_filepath)} with encoding='{encoding}' (fallback): {e}")
#             continue # Try the next encoding


#      # If the loop finishes without returning, it means all attempted encodings failed
#     print(f"  Failed to decode {os.path.basename(csv_filepath)} with all attempted encodings.")
#     return None


#  # --- Helper function to get Primary Key columns (Keep this) ---
# def get_primary_key_columns(cursor: Any, schema_name: str, table_name: str) -> Optional[List[str]]:
#     """
#     Retrieves the column names that make up the primary key for a given table.
#     """
#     query = f"""
#         SELECT kcu.column_name
#         FROM information_schema.table_constraints tc
#          JOIN information_schema.key_column_usage kcu
#           ON tc.constraint_name = kcu.constraint_name
#          AND tc.table_schema = kcu.table_schema
#          AND tc.table _name = kcu.table_name
#         WHERE tc.constraint_type = 'PRIMARY KEY'
#           AND tc.table_schema = %s
#           AND tc.table_name = %s
#         ORDER BY  kcu.ordinal_position;
#     """
#     try:
#         cursor.execute(query, (schema_name, table_name))
#         pk_columns = [row[0] for row in cursor.fetchall ()]
#         if pk_columns:
#             return pk_columns
#         else:
#             return None # No primary key found
#     except Exception as e:
#         print(f"Error retrieving primary key for {schema_name}.{table_name}: {e}")
#         return None


# # --- Function to get table schema (Keep this) ---
# def get_table_schema_with_schema(schema_name: str, table_name: str,  conn: Any) -> Dict[str, str]:
#     """
#     Get the column names and their database data types for a given table.
#     """
#     column_info: Dict[str, str] = {}
#     try:
#         with conn.cursor() as cur:
#             cur.execute(
#                 """
#                 SELECT column_name, data_type
#                 FROM information_schema.columns
#                 WHERE table_schema = %s AND table_name  = %s;
#                 """,
#                 (schema_name, table_name)
#             )
#         results = cur.fetchall()
#         for row in results:
#             column_info[row[0]] = row [1]
#             return column_info
#     except Exception as e:
#         print(f"Error fetching schema for {schema_name}.{table_name}: {e}")
#         return {}


# # --- Function to  rename DataFrame columns (Keep this) ---
# def rename_dataframe_columns_from_schema(df: pd.DataFrame, db_schema: Dict[str, str]) -> pd.DataFrame:
#     """
#     Renames  DataFrame columns (expected to be 0, 1, 2, ...) using keys
#     from a database schema dictionary.
#     """
#     # print('few data',df.columns) # Verbose

#     new_column_names = list(db_schema.keys())

#     if len(df.columns) != len(new_column_names):
#         raise ValueError(
#             f"Column count mismatch: DataFrame has {len(df.columns)} columns (0  to {len(df.columns)-1}), "
#             f"but schema dictionary has {len(new_column_names)} keys ({new_column_names}). " # Added schema keys to error msg
#             f"Cannot rename  columns if counts don't match."
#         )

#     df.columns = new_column_names

#     print(f"Successfully renamed {len(df.columns)} columns.")
#     return df


# # --- Function to  convert DataFrame column types (Keep this, it's used implicitly by the cleaning in insert_dataframe_in_chunks) ---
# # This function is actually NOT called directly anymore, its logic is integrated
# # into the cleaning step of insert_dataframe_in _chunks. You can remove this function
# # definition if you want, but keep the DB_TO_PANDAS_TYPE_MAP.
# # def convert_dataframe_columns_to_db_types(df: pd .DataFrame, db_schema: Dict[str, str]) -> pd.DataFrame:
# #     # ... (implementation is now inside insert_dataframe_in_chunks) ...
# #     pass


# # --- insert_dataframe_in_chunks  function (Modified) ---
# def insert_dataframe_in_chunks(
#     df: pd.DataFrame,
#     conn: Any, # Type hint for connection object
#     table_name_with_schema: str, #  Table name including schema (e.g., 'Sales.Address')
#     db_schema: Dict[str, str], # Pass the DB schema dictionary
#     chunk_size: int = 1000,
#     disable_fk_checks: bool = True,
#     use_on_conflict_do_nothing: bool = False
# ) -> None:
#     """
#     Inserts a pandas DataFrame into a PostgreSQL table in specified chunks.
#     Optionally  disables/re-enables foreign key checks and uses ON CONFLICT DO NOTHING.
#     Standardizes pandas/numpy types and handles common string issues (like 'NaT', 'np.float64').

#     Args:
#          df (pd.DataFrame): The DataFrame to insert.
#         conn: The active psycopg2 database connection object.
#         table_name_with_schema (str): The name of the target table including schema (e.g ., 'schema.my_table').
#         db_schema (Dict[str, str]): Dictionary mapping column names (matching DataFrame) to DB types.
#         chunk_size (int): The number of rows to insert in each  batch.
#         disable_fk_checks (bool): If True, disables foreign key triggers
#                                  before inserting and re-enables them after.
#         use_on_conflict_do_nothing (bool): If True , adds ON CONFLICT DO NOTHING clause
#                                            based on the table's primary key.

#     Raises:
#         Exception: If a database error occurs during insertion.
#         ValueError: If the DataFrame is empty or table_ name_with_schema is invalid.
#     """
#     if df.empty:
#         print(f"DataFrame is empty. Skipping insertion for table {table_name_with_schema}.")
#         return

#     # ---  Validate table_name_with_schema format and split ---
#     if '.' not in table_name_with_schema:
#          raise ValueError(f"Table name '{ table_name_with_schema}' must include schema (e.g., 'schema.table').")
#     schema_name, simple_table_name = table_name_with_schema.split('.', 1)


#     df_columns = df .columns.tolist()
#     num_columns = len(df_columns)

#     if num_columns == 0:
#         print(f"DataFrame has no columns. Skipping insertion for table  {table_name_with_schema}.")
#         return

#     # --- Data Cleaning and Type Standardization ---
#     df_cleaned = df.copy()
#     print("Starting data cleaning and type standardization...")

#     # Mapping DB  types to pandas types (or hints) - Access the mapping defined  outside
#     global DB_TO_PANDAS_TYPE_MAP # Assuming it's defined globally or at module level

#     # --- Define common string representations of missing/invalid values ---
#     # Add variations of 'NaT' and potentially other strings like 'NULL', 'None'
#     missing_string_values = {'NaT', 'NAT', 'nat', 'NULL', 'null', 'None', 'none', ''}
#     # Add 'np.float64' string as well if that was observed
#     missing_string_values.add('np.float64')


#     for col in df_cleaned.columns:
#         # Get the target database type string for this column from the  schema
#         # Use .get() with None default and lower() for case-insensitive matching
#         target_db_type_str = db_schema.get(col, '').lower()
#         pandas_target_hint = DB_TO_PANDAS_TYPE_MAP.get(target_db_type_str)

#         # --- Replace pandas/numpy missing values (NaN, NaT, NA) with None ---
#         # Do this first , so subsequent conversions don't fail on missing values
#         if df_cleaned[col].isna().any():
#              # print(f"  Replacing pandas missing values with None in column '{col}'...") # Verbose
#             df_cleaned.loc[df_cleaned[col].isna(), col] = None


#         # --- **CRITICAL: Handle Timestamp/Date Columns Explicitly** ---
#         # Check if the target DB type maps to pandas datetime
#         if pandas_target_hint == 'datetime64[ns]':
#             print(f"  Standardizing datetime column '{col}'...")

#             # --- **NEW: Explicitly replace specific missing/invalid strings with None BEFORE pd.to_datetime** ---
#             # Convert to string dtype safely for comparison/replacement
#             df_cleaned[col] = df_cleaned[col].astype(str)
#             # Replace specific strings like 'NaT', 'NULL', '' with None
#             df_cleaned[col] = df_cleaned[col].replace(list(missing_string_values), None)
#             # --- END NEW ---

#             # Use pd.to_datetime with errors='coerce'. This turns invalid parsing
#             # into pd.NaT. Since we replaced known strings above, this mainly handles
#             # other unexpected non-date strings.
#             # Add common formats if known for speed/reliability: format=['%Y-% m-%d %H:%M:%S', '%Y-%m-%d']
#             df_cleaned[col] = pd.to_datetime(df_cleaned[col], errors='coerce')

#             # Now replace the resulting pd.NaT values (from original NaT or coerced errors) with None
#             # This covers original missing values AND values that failed pd.to_datetime
#             if df_cleaned[col].isna().any(): # Check again for NaT after conversion
#                  # print (f"  Replacing pd.NaT (including coerced errors) with None in '{col}'...") # Verbose
#                  df_cleaned.loc[df_cleaned[col].isna(), col] = None


#         # ---  Specific handling for boolean columns ---
#         # Ensure values are standard Python bool or None
#         # Check if the target DB type maps to pandas boolean
#         elif pandas_target_hint == 'boolean': # Use the pandas hint string
#              print(f"  Standardizing boolean column '{col}'...")

#              # --- **NEW: Explicitly replace specific missing/invalid strings with None for boolean** ---
#              # Convert to string dtype safely for comparison/replacement
#              df_cleaned[col] = df_cleaned[col].astype(str)
#              # Replace specific strings like 'NULL', '' with None before boolean conversion
#              # Note: 'NaT' might not appear in boolean columns, but doesn't hurt to include
#              df_cleaned[col] = df_cleaned[col].replace(list(missing_string_values), None)
#              # --- END NEW ---

#              # Convert to pandas nullable boolean first, then apply lambda for safety
#              # astype('boolean') handles 'True', 'False', '0', '1' and None/pd.NA
#              df_cleaned[col] = df_cleaned[col].astype('boolean', errors='coerce') # Convert to nullable boolean
#              # Apply lambda to ensure Python bool or None
#              df_cleaned[col] = df_cleaned[col].apply(
#                  lambda x: True if x is True else (False if x is False else None) # Handles pd.NA correctly
#              )


#         # --- Standardize Numeric Columns ---
#         # Convert to standard Python int/float or None
#         # Check if the target DB type maps to a pandas numeric hint
#         elif pandas_target_hint in ['Int64', 'Float64']: # Use the pandas hint strings
#              print(f"  Standardizing numeric column '{col}' (target pandas dtype : {pandas_target_hint})...") # Verbose

#              # --- **NEW: Explicitly replace specific missing/invalid strings with None for numeric** ---
#              # Convert to string dtype safely for comparison/replacement
#              df_cleaned[col] = df_cleaned[col].astype(str)
#              # Replace specific strings like 'NaT', 'np.float64', 'NULL', '' with None
#              df_cleaned[col] = df_cleaned[col].replace(list(missing_string_values), None)
#              # --- END NEW ---

#              # Use pd.to_numeric with errors='coerce' then handle resulting NaN/None
#              df_cleaned[col] = pd.to_numeric( df_cleaned[col], errors='coerce')

#              # After pd.to_numeric, missing values are NaN. Replace them with None.
#              if df_cleaned[col].isna().any(): # Check for NaN after conversion
#                   #  print(f"  Replacing NaN (including coerced errors) with None in '{col}'...") # Verbose
#                   df_cleaned.loc[df_cleaned[col].isna(), col] = None
#              else:
#                  # If no NaN after coerce, ensure it's float/int if not None
#                 # Convert to standard Python float/int using .apply only if no NaNs were found
#                 # This avoids issues if the column was all  NaNs initially
#                 if pandas_target_hint == 'Int64':
#                      # Ensure the column is nullable before attempting int conversion if it contains None
#                      if df_cleaned[col].isna().any(): # This check is redundant due to .loc[isna()] above, but defensive
#                           df_cleaned[col] = df_cleaned[col].astype('Int64') # Convert to nullable pandas Int64
#                      else:
#                           df_cleaned[col] = df_cleaned[col].apply(lambda x: int(x) if x is not None else None) # Convert to Python int or None
#                 else: # Float64
#                      df_cleaned[col] = df_cleaned[col].apply(lambda x: float(x) if x is not None else None) # Convert to Python float or None


#         # --- Specific handling for UUID columns ---
#         # Check if the target DB type is 'uuid'
#         elif target_db_type_str == 'uuid':
#             print(f"  Standardizing UUID column '{col}'...")

#             # --- **NEW: Explicitly replace specific missing/invalid strings with None for UUID** ---
#             # Convert to string dtype safely for comparison/replacement
#             df_cleaned[col] = df_cleaned[col].astype(str)
#             # Replace specific strings like 'NaT', 'NULL', '' with None
#             df_cleaned[col] = df_cleaned[col].replace(list(missing_string_values), None)
#             # --- END NEW ---

#             # Apply conversion to UUID object or None (handles valid UUID strings)
#             df_cleaned[col] = df_cleaned[col].apply(convert_to_uuid_or_none) # Need convert_to_uuid_or_none

#             # Convert back to string representation for psycopg2 (without braces) or keep None
#             df_cleaned[col] = df_cleaned[col].apply(lambda x: str(x).strip('{}') if isinstance(x, uuid.UUID) else None)


#     # Verify replacement for NaT specifically if it's a datetime column (Keep this check)
#     # This check is now mainly to confirm the logic *after* the main datetime handling
#     # It should print "successfully replaced" if the above logic worked.
#     for col in df_cleaned.columns:
#         if df_cleaned[col].dtype == 'datetime64[ns]':
#              has_pd_nat_after = (df_cleaned[col] == pd. NaT).any()
#              if has_pd_nat_after:
#                   print(f"  CRITICAL DEBUG: Column '{col}' (datetime) *still* contains pd.NaT after replacement.")
#              else :
#                   print(f"  Debug: Column '{col}' (datetime) successfully replaced pd.NaT with None.") # Confirm success


#     print("\nData cleaning and standardization complete. Final dtypes of cleaned DataFrame:")
#     print (df_cleaned.dtypes)
#     print("-" * 30)
#     # --- End Data Cleaning ---


#     # --- Construct the INSERT SQL statement using psycopg2.sql (Keep this) ---
#     # Correctly  handle schema.table notation using sql.Identifier with multiple arguments
#     table_ident = sql.Identifier(schema_name, simple_table_name)
#     column_idents = [sql.Identifier(col) for col in df_columns ]
#     columns_list_sql = sql.SQL(', ').join(column_idents)
#     placeholders_sql = sql.SQL(', ').join(sql.Placeholder() * num_columns)

#     # --- Build  ON CONFLICT clause if requested (Keep this) ---
#     on_conflict_clause = sql.SQL("")
#     if use_on_conflict_do_nothing:
#         # Need a cursor to fetch PK columns *before * the main insertion loop
#         try:
#             with conn.cursor() as cur:
#                  pk_columns = get_primary_key_columns(cur, schema_name, simple_table_name)

#             if  pk_columns is None:
#                 print(f"  Warning: Table {schema_name}.{simple_table_name} has no primary key. Cannot use ON CONFLICT DO NOTHING.")
#                 # on_conflict_clause  remains empty
#             else:
#                 print(f"  Primary key for {simple_table_name}: {pk_columns}. Using ON CONFLICT DO NOTHING.")
#                 pk_columns_sql = sql.SQL(", ").join(map( sql.Identifier, pk_columns))
#                 on_conflict_clause = sql.SQL("ON CONFLICT ({pk_cols}) DO NOTHING").format(
#                     pk_cols=pk_columns_sql
#                 )
#         except Exception as e:
#             print(f"  Error determining primary key for ON CONFLICT: {e}")
#             print(f"  Proceeding without ON CONFLICT DO NOTHING for {table_name_with_schema}.")
#             # on_conflict_clause remains empty

#     # --- Build the final INSERT statement template ---
#     insert_sql = sql.SQL("""
#         INSERT INTO {table} ({columns})
#         VALUES ({values})
#         {on _conflict_clause}
#         ;
#     """).format(
#         table=table_ident,
#         columns=columns_list_sql,
#         values=placeholders_sql,
#         on_conflict_clause =on_conflict_clause # Include the built clause
#     )

#     # --- DEBUG: Print the generated SQL query string (Keep this) ---
#     try:
#         print(f"\nGenerated INSERT SQL: {insert_sql.as_string(conn)}")
#     except Exception as e:
#         print(f"\nError generating SQL string for debug: {e }")
#         print(f"SQL Structure (approximated): INSERT INTO  {table_name_with_schema} ({', '.join(df_columns)}) VALUES (%s, ...) [ON CONFLICT ...]")
#     # --- END DEBUG ---


#     print(f"\nStarting bulk insert into {table_name_with_schema} in chunks of {chunk_size}...")
#     # print(f"Total rows to insert: {len(df_cleaned)}") # Verbose

#     num_rows = len(df_cleaned)
#     num_chunks = (num_rows + chunk_size - 1) // chunk_size

#     # --- Main try...finally block to ensure triggers are re-enabled ---
#     try:
#         # --- Disable FK checks if requested ---
#         # Use  a separate cursor for ALTER TABLE commands if needed, or ensure the main cursor is not mid-transaction
#         # A dedicated cursor and commit for ALTER TABLE is safer.
#         if disable_fk_checks:
#             print(f"  Disabling triggers (including FK checks) for {table_name_with_schema}...")
#             try:
#                 with conn.cursor() as cur_alter: # Use a dedicated cursor for ALTER TABLE
#                      cur_alter.execute(sql.SQL("ALTER TABLE {} DISABLE TRIGGER ALL;").format(table_ident))
#                      conn.commit() # Commit the ALTER TABLE command
#                 print(f"  Triggers disabled.")
#             except Exception as e:
#                 print(f"  ERROR : Failed to disable triggers for {table_name_with_schema}: {e}")
#                 print("  Proceeding without disabling triggers.")
#                 disable_fk_checks = False # Turn off the flag so re-enable doesn't run in finally


#         # --- Insertion loop ---
#         # The main cursor for insertions
#         with conn.cursor() as cur_insert:
#              for i in range(num_chunks):
#                 start_index = i * chunk_size
#                 end_index = min((i + 1) * chunk_size, num_rows)

#                 chunk_df = df_cleaned.iloc[start_index:end_index]

#                 # --- CRITICAL DEBUG: Inspect the data types in the first few tuples (Modified) ---
#                 # Check the first few tuples instead of just the first
#                 num_debug_tuples = min(5, len(chunk_df)) # Check up to 5 tuples
#                 if i == 0 and num_debug_tuples > 0:
#                     print(f"\nDEBUG: Inspecting types in the FIRST {num_debug_tuples} tuple(s) of batch {i+1}/{num_chunks}...")
#                     debug_tuples = list(chunk_df.itertuples(index=False, name=None))[:num_debug_tuples]
#                     for tuple_index, current_tuple in enumerate(debug_tuples):
#                         print(f"  Tuple {tuple_index}:")
#                         for j, item in enumerate(current_tuple):
#                             col_name = df_columns[j] if j < len(df_columns) else f"UnknownCol_{j}"
#                             # Use repr() to show strings clearly
#                             print(f"    Element {j} (Column '{col_name}'): Type is {type(item)}, Value is {item!r}")
#                     print("--- END DEBUG ---")
#                 # --- End CRITICAL DEBUG ---


#                 data_for_batch = list(chunk_df.itertuples(index=False, name=None ))

#                 if not data_for_batch:
#                     print(f"  Warning: Batch {i+1}/{ num_chunks} is empty. Skipping.")
#                     continue

#                 try:
#                     # executemany
#                     # If ON CONFLICT is used, PK/UNIQUE violations here are silent skips, not errors.
#                     # Other  errors (data type, FK, etc.) will still raise exceptions.
#                     cur_insert.executemany(insert_sql, data_for_batch)

#                     status_msg = f"  Processed chunk {i +1}/{num_chunks} ({len(data_for_batch)} rows)."
#                     if use_on_conflict_do_nothing and pk_columns:
#                          # Note: We don't know *how many* were  skipped, just that conflicts were handled.
#                         status_msg += f" (Conflicts on {', '.join(pk_columns)} skipped)"
#                     print(status_msg)

#                     conn.commit() # Commit the transaction  for this batch

#                 except Exception as e:
#                     # This block catches errors OTHER THAN unique constraint violations
#                     # handled by ON CONFLICT DO NOTHING (e.g., data type errors, foreign key errors if not disabled). 
#                     conn.rollback() # Rollback the failed batch
#                     print(f"\n  Error inserting chunk {i+1}/{num_chunks} starting at row {start_index} into {table_name_with_ schema}. Rolling back batch.")
#                     print(f"  Error details: {e}")
#                     raise e # Re-raise the exception


#         print(f"Successfully inserted all {len(df_cleaned)} rows into { table_name_with_schema}.")

#     except Exception as e:
#         print(f"\nLoading process for table {table_name_with_schema} halted due to an error.")
#         print(f"Details : {e}")
#         # If this is part of a larger script loading multiple tables,
#         # the outer loop would catch this and decide whether to stop entirely
#         # or move to the next table.

#     finally:
#          # --- Ensure FK checks are re-enabled ---
#         if disable_fk_checks: # Only try to re-enable if we successfully disabled
#             print(f"  Re-enabling triggers for {table_name_with_schema}...")
#             try:
#                 with conn.cursor() as cur_alter: # Use a dedicated cursor
#                     cur_alter.execute(sql.SQL("ALTER TABLE {} ENABLE TRIGGER ALL;").format( table_ident))
#                     conn.commit() # Commit the ALTER TABLE command
#                 print(f"  Triggers re-enabled.")
#             except Exception as e:
#                  print(f"  ERROR : Failed to re-enable triggers for {table_name_with_schema}: {e}")
#                  print(f"  Manual intervention required to ENABLE TRIGGER ALL for table {table_name_with_schema}!")


# # --- Main Execution Logic (Keep this) ---
# def main():
#     conn = None
#     try:
#         conn = psycopg2.connect(**DB_CONFIG)
#         print("Successfully connected to PostgreSQL database.")

#         # Process files in the defined order
#         for base_filename in TABLE_LOAD_ORDER:
#             csv_file_path = os.path.join(CSV_DIR, f"{base_filename}.csv")
#             if base_filename in TABLE_SCHEMA_MAPPING: 
#                 full_table_name_with_schema = TABLE_SCHEMA_MAPPING[base_filename].lower()
#                 schema_name = full_table_name_with_schema.split('.')[0] # Keep original  casing for schema? No, lower() is safer
#                 table_name = full_table_name_with_schema.split('.')[1] # Keep original casing for table? No, lower() is safer

#                 if os.path. exists(csv_file_path):
#                     print(f"\n--- Processing file: {base_filename}.csv for table {full_table_name_with_schema} ---")

#                     # --- Get DB Schema ---
#                     db_schema = get_table_schema_with_schema(schema_name, table_name, conn)
#                     if not db_schema:
#                          print(f"  Could not fetch schema for {full_table_name_with_schema}. Skipping file.")
#                          continue # Skip this file if schema fetching failed
#                     # print("  DB Schema:", db_schema) # Verbose

#                     # --- Parse CSV ---
#                     contents = parse_csv_file (csv_file_path)
#                     if contents is None:
#                         print(f"  Error parsing file {os.path.basename(csv_file_path)}. Skipping.")
#                         continue # Skip this file if parsing failed

#                     # ---  Basic DataFrame Checks ---
#                     if contents.empty or contents.shape[0] == 0:
#                         print(f"  File {os.path.basename(csv_file_path)} is empty or has no rows after  parsing. Skipping.")
#                         continue
#                     # Check if DataFrame has columns (should be true if not empty, but defensive)
#                     if contents.shape[1] == 0:
#                         print(f"  File {os.path. basename(csv_file_path)} has no columns after parsing. Skipping.")

# ```python
import pandas as pd
import psycopg2
from psycopg2 import sql
import numpy as np
from typing import Any, Optional, List, Dict, Union
import io
import os
import glob
import datetime
import uuid

# --- Database Configuration (Keep this) ---
DB_CONFIG = {
    "host": "localhost",
    "port ": "5433",
    "dbname": "postgres",
    "user": "data_eng",
    "password": "12345pP"
}

# --- CSV Files Directory (Keep this) ---
CSV_DIR = "/home/blueberry/Desktop/advanced_sql_tutorial/data"

# --- Table Schema Mapping (Keep this) ---
# Make sure this mapping is correct and matches your DB schema definitions
TABLE_SCHEMA_MAPPING = {
    # "Address": "Person.Address",
    # "AddressType": "Person.AddressType",
    # "AWBuildVersion": "dbo.AWBuildVersion",
    # "BillOfMaterials": "Production.BillOfMaterials",
    # "BusinessEntity": "Person.BusinessEntity",
    # "BusinessEntityAddress": "Person.BusinessEntityAddress",
    # "BusinessEntityContact": "Person.BusinessEntityContact",
    # "ContactType": "Person.ContactType",
    # "CountryRegion": "Person.CountryRegion",
    # "CreditCard": "Sales.CreditCard",
    # "Culture": "Production.Culture",
    # "Currency": "Sales.Currency",
    # "CurrencyRate": "Sales.CurrencyRate",
    # "Customer": "Sales.Customer",
    # "Department": "HumanResources.Department",
    # "Document": "Production.Document", # Note: DocumentNode PK might be tricky with auto-gen
    # "EmailAddress": "Person.EmailAddress",
    # "Employee": "HumanResources.Employee",
    # "EmployeeDepartmentHistory": "HumanResources.EmployeeDepartmentHistory",
    # "EmployeePayHistory": "HumanResources.EmployeePayHistory",
    # "Illustration": "Production.Illustration",
    # "JobCandidate": "HumanResources.JobCandidate",
    # "Location": "Production.Location",
    # "Password": "Person.Password",
    # "Person": "Person.Person",
    # "PersonCreditCard": "Sales.PersonCreditCard",
    # "PersonPhone": "Person.PersonPhone",
    # "PhoneNumberType": "Person.PhoneNumberType",
    # "Product": "Production.Product",
    # "ProductCategory": "Production.ProductCategory",
    # "ProductCostHistory": "Production.ProductCostHistory",
    # "ProductDescription": "Production.ProductDescription",
    # "ProductDocument": "Production.ProductDocument",
    # "ProductInventory": "Production.ProductInventory",
    # "ProductListPriceHistory": "Production.ProductListPriceHistory",
    # "ProductModel": "Production.ProductModel",
    # "ProductModelIllustration": "Production.ProductModelIllustration",
    # "ProductModelProductDescriptionCulture": "Production.ProductModelProductDescriptionCulture",
    # "ProductPhoto": "Production.ProductPhoto",
    # "ProductProductPhoto": "Production.ProductProductPhoto",
    # "ProductReview": "Production.ProductReview",
    # "ProductSubcategory": "Production.ProductSubcategory",
    # "PurchaseOrderDetail": "Purchasing.PurchaseOrderDetail",
    # "PurchaseOrderHeader": "Purchasing.PurchaseOrderHeader",
    # "SalesOrderDetail": "Sales.SalesOrderDetail",
    "SalesOrderHeader": "Sales.SalesOrderHeader" #,
    # "SalesOrderHeaderSalesReason": "Sales.SalesOrderHeaderSalesReason",
    # "SalesPerson": "Sales.SalesPerson",
    # "SalesPersonQuotaHistory": "Sales.SalesPersonQuotaHistory",
    # "SalesReason": "Sales.SalesReason",
    # "SalesTaxRate": "Sales.SalesTaxRate",
    # "SalesTerritory": "Sales.SalesTerritory",
    # "SalesTerritoryHistory": "Sales.SalesTerritoryHistory",
    # "ScrapReason": "Production.ScrapReason",
    # "Shift": "HumanResources.Shift",
    # "ShipMethod": "Purchasing.ShipMethod",
    # "ShoppingCartItem": "Sales.ShoppingCartItem",
    # "SpecialOffer": "Sales.SpecialOffer",
    # "SpecialOfferProduct": "Sales.SpecialOfferProduct",
    # "StateProvince": "Person.StateProvince",
    # "Store": "Sales.Store",
    # "TransactionHistory": "Production.TransactionHistory",
    # "TransactionHistoryArchive": "Production.TransactionHistoryArchive",
    # "UnitMeasure": "Production.UnitMeasure",
    # "Vendor": "Purchasing.Vendor",
    # "WorkOrder": "Production.WorkOrder",
    # "WorkOrderRouting": "Production.WorkOrderRouting",
    # "CountryRegionCurrency": "Sales.CountryRegionCurrency"
}

# --- Define the order of loading tables (Keep this, verify it's correct for FKs) ---
TABLE_LOAD_ORDER = [
    # Core/Lookup Tables (Fewest Dependencies)
    # "AWBuildVersion",
    # "CountryRegion",
    # "AddressType",
    # "ContactType",
    # "PhoneNumberType",
    # "UnitMeasure",
    # "ProductCategory",
    # "Culture",
    # "Currency",
    # "Department",
    # "Shift",
    # "Illustration",
    # "Location",
    # "ProductDescription",
    # "ProductPhoto",
    # "ScrapReason",
    # "ShipMethod",
    # "SpecialOffer", # Depends on nothing major for its own columns
    # "SalesReason",

    # # Business Entity and Person related (Core)
    # "BusinessEntity", # Parent for Person, Store, Vendor
    # "Person", # Depends on BusinessEntity
    # "Password", # Depends on Person
    # "EmailAddress", # Depends on Person
    # "PersonPhone", # Depends on Person, PhoneNumberType
    # "StateProvince", # Depends on CountryRegion, SalesTerritory (FK added later in DDL)
    # "Address", # Depends on StateProvince
    # "BusinessEntityAddress", # Depends on BusinessEntity, Address, AddressType

    # # Human Resources
    # "Employee", # Depends on Person
    # "EmployeeDepartmentHistory", # Depends on Employee, Department, Shift
    # "EmployeePayHistory", # Depends on Employee
    # # "JobCandidate", # Can depend on Employee (nullable FK)
    # "Document", # Depends on Employee (Owner) - HIERARCHYID (DocumentNode) needs careful handling if not text

    # # Production (Products and related components)
    # "ProductSubcategory", # Depends on ProductCategory
    # "ProductModel", # No direct FKs in its main columns, but linked from Product
    # "Product", # Depends on ProductModel, ProductSubcategory, UnitMeasure
    # "BillOfMaterials", # Depends on Product (ProductAssemblyID, ComponentID), UnitMeasure
    # "ProductCostHistory", # Depends on Product
    # "ProductListPriceHistory", # Depends on Product
    # # "ProductReview", # Depends on Product
    # "ProductInventory", # Depends on Product, Location
    # "ProductProductPhoto", # Depends on Product, ProductPhoto
    # "ProductModelIllustration", # Depends on ProductModel, Illustration
    # "ProductModelProductDescriptionCulture", # Depends on ProductModel, ProductDescription, Culture
    # "ProductDocument", # Depends on Product, Document
    # "WorkOrder", # Depends on Product, ScrapReason
    # "WorkOrderRouting", # Depends on WorkOrder, Product, Location
    # "TransactionHistory", # Depends on Product
    # "TransactionHistoryArchive", # Data comes from TransactionHistory, not direct FK usually

    # # Purchasing
    # "Vendor", # Depends on BusinessEntity
    # "PurchaseOrderHeader", # Depends on Employee, Vendor, ShipMethod
    # "PurchaseOrderDetail", # Depends on PurchaseOrderHeader, Product

    # # Sales (Core sales structures)
    # "CountryRegionCurrency", # Assuming FKs to CountryRegion, Currency. Create this table.
    # "CurrencyRate", # Depends on Currency (From and To)
    # "SalesTerritory", # Depends on CountryRegion
    # "SalesPerson", # Depends on Employee, SalesTerritory (nullable)
    # "Store", # Depends on BusinessEntity, SalesPerson (nullable)
    # "Customer", # Depends on Person (nullable), Store (nullable), SalesTerritory
    # "CreditCard",
    # "PersonCreditCard", # Depends on Person, CreditCard
    # "SpecialOfferProduct", # Depends on SpecialOffer, Product
    "SalesOrderHeader" #, # Depends on Customer, SalesPerson (nullable), Territory, Address, ShipMethod, CreditCard (nullable), CurrencyRate (nullable)
    # "SalesOrderDetail", # Depends on SalesOrderHeader, Product, SpecialOffer (via SpecialOfferProduct or direct)
    # "SalesOrderHeaderSalesReason", # Depends on SalesOrderHeader, SalesReason
    # "SalesPersonQuotaHistory", # Depends on SalesPerson
    # "SalesTaxRate", # Depends on StateProvince
    # "SalesTerritoryHistory", # Depends on SalesPerson, SalesTerritory
    # "ShoppingCartItem" # Depends on Product
]

# --- Mapping database type strings to pandas dtypes or conversion functions (Keep this) ---
# Ensure this mapping is comprehensive  for your database types
DB_TO_PANDAS_TYPE_MAP: Dict[str, Union[str, type, callable]] = {
    'integer': 'Int64', 'smallint': 'Int6 4', 'bigint': 'Int64',
    'decimal': 'Float64', 'numeric': 'Float64', 'real': 'Float64', 'double precision': 'Float64',
     'boolean': 'boolean',
    'character varying': 'string', 'varchar': 'string', 'text': 'string', 'character': 'string',
    'date': 'datetime64[ns]', 'timestamp without  time zone': 'datetime64[ns]', 'timestamp with time zone': 'datetime64[ns]',
    'time without time zone': 'object', 'time with time zone': 'object', # Time might need custom handling
     'uuid': 'string', # Pass as string, let DB cast
    # Add other types as needed based on your schema
    # 'bytea': 'object',
    # 'json': 'object',
    # ' jsonb': 'object',
}

# --- Helper function to convert a value to UUID or None, handling errors and empty strings ---
# Needed if you map UUID to 'object' and convert, OR if you convert to string and need to handle invalid strings
def convert_to_uuid_or_none(value: Any) -> Union[uuid.UUID, None]:
    """Helper to convert a value to UUID or None, handling errors and empty strings."""
    # Check for pandas NaN or None, or empty string after stripping
    if pd.isna(value) or value is None or str(value).strip() == '':
         return None
    try:
        # Attempt conversion, stripping potential braces {}
        return uuid.UUID(str(value).strip('{}'))
    except (ValueError, TypeError):
        # If conversion fails (invalid UUID string), return None
        # print(f"Warning: Could not convert '{value}' to UUID.") # Optional warning
        return None


def parse_csv_file(csv_filepath):
    """
    Parse a CSV file and return its contents as a pandas DataFrame.
    Tries multiple common encodings if the default/specified one fails.
    """
    # List of encodings to try, in order of likelihood based on common issues
    # Put UTF-16 LE and UTF-8 SIG high up as they are common for non-ASCII datae
    encodings_to_try = ['cp1252', 'utf-8-sig', 'utf-16','latin-1'] 
    utf_16_encodings_file = ('BusinessEntityAddress', 'Employee', 'Person', 'EmailAddress', 'Password', \
                            'PersonPhone', 'PhoneNumberType', 'ProductPhoto','BusinessEntity', 'ProductModel', \
                            'CountryRegionCurrency', 'Store', 'Illustration', 'JobCandidate', 'Document', 'ProductDescription')
    utf_8_encodings_file = ('ProductReview', 'Product', 'Location', 'SalesOrderHeader')


    # Keep sep='\t' and header=None as they seem correct for your file format
    read_csv_params = {'sep': '\t', 'header': None, 'low_memory': False} # Keep low_memory=False
    filename = os.path.basename(csv_filepath).split('.')[0] # Get the base filename without extension
    if not filename in utf_16_encodings_file and not filename in utf_8_encodings_file:
        for encoding in encodings_to_try:
            print(f"  Attempting to read {os.path.basename(csv_filepath)} with encoding='{encoding}'...")
            try:
                # Use pandas to read the file with the current encoding
                df = pd.read_csv(csv_filepath, encoding=encoding, **read_csv_params)
                print(f"  Successfully read {os.path.basename(csv_filepath)} with encoding='{encoding}'.")
                print('columns list ',df.columns)
                return df # Success! Return the DataFrame

            except UnicodeDecodeError as e:
                print(f"  Decode error with encoding='{encoding}': {e}")
                # Continue to the next encoding in the loop
                continue
            except FileNotFoundError:
                print(f"  Error: File not found at {csv_filepath}")
                # No need to try other encodings if the file itself isn't found
                return None
            except Exception as e:
                # Catch other potential errors during parsing (e.g., CSV format issues, permission errors)
                print(f"  An unexpected error occurred reading {os.path.basename(csv_filepath)} with encoding='{encoding}': {e}")
                # If it's not a decode error, the encoding might be correct but something else failed.
                # You might decide to stop trying encodings here or continue depending on how robust you need it.
                return None
    elif filename in utf_8_encodings_file:
        # If the filename is in the utf_8_encodings_file list, try UTF-8 SIG first
        t_tab_files = []
        file_encoding = 'utf-8'
        params = {'sep': r'\t', 'header': None}  # Multiple separators r'\+\||\t'
        print(f"  Attempting to read {os.path.basename(csv_filepath)} with encoding='utf-8'...")
        cleaned_data = None
        try:
            # 1. Read the file in binary mode
            with open(csv_filepath, 'rb') as f:
                raw_bytes = f.read()

            try:
                decoded_string = raw_bytes.decode(file_encoding)
                print("File decoded successfully.")
            except UnicodeDecodeError as e:
                print(f"UnicodeDecodeError during decoding: {e}")
                print("The file might not be strictly '{file_encoding}'. Trying with errors='ignore'.")
                decoded_string = raw_bytes.decode(file_encoding, errors='ignore')

            cleaned_string = decoded_string.replace('\x00', '') # Remove null bytes
            print("Invisible characters removed.")

            # Use io.StringIO to make the cleaned string look like a file to pandas
            data_io = io.StringIO(cleaned_string)

            # Let pandas read from the StringIO object
            # Don't pass encoding or low_memory when reading from StringIO
            if os.path.basename(csv_filepath).split('.')[0] == 'ProductReview':
                params = {'sep': r'\t', 'header': None}
                df = pd.read_csv(data_io, sep=params['sep'], header=params['header'])
            else:
                df = pd.read_csv(data_io, sep=params['sep'], header=params['header'])

            print("File read successfully with pandas from cleaned data!")
            return df # Success! Return the DataFrame

        except FileNotFoundError:
            print(f"Error: File not found at {csv_filepath}")
            return None
        except Exception as e:
            print(f"\nAn unexpected error occurred: {e}")
            print("Please double-check the file content and encoding.")
            return None
 
    else:
        t_tab_files = ['Employee','CountryRegionCurrency','ProductDescription']
        file_encoding = 'utf-16-le'
        params = {'sep': r'\+\|', 'header': None}
        # If the filename is in the utf_16_encodings_file list, try UTF-16 LE first
        print(f"  Attempting to read {os.path.basename(csv_filepath)} with encoding='utf-16-le'...")
        cleaned_data = None
        try:
            # Read the file in binary mode
            with open(csv_filepath, 'rb') as f:
                raw_bytes = f.read()

            # Decode the bytes using the specified encoding
            try:
                decoded_string = raw_bytes.decode(file_encoding)
                print("File decoded successfully.")
            except UnicodeDecodeError as e:
                print(f"UnicodeDecodeError during decoding: {e}")
                print("The file might not be strictly '{file_encoding}'. Trying with errors='ignore'.")
                decoded_string = raw_bytes.decode(file_encoding, errors='ignore')

            # Clean the string - remove common invisible characters like null bytes
            # The VS Code warning strongly suggests this is needed.
            cleaned_string = decoded_string.replace('\x00', '') # Remove null bytes
            print("Invisible characters removed.")

            # Use io.StringIO to make the cleaned string look like a file to pandas
            data_io = io.StringIO(cleaned_string)

            # Let pandas read from the StringIO object
            print(f"  debug 1 Attempting to read {os.path.basename(csv_filepath)} with encoding='utf-16-le'...")
            if os.path.basename(csv_filepath).split('.')[0] in t_tab_files:
                print(f"  debug 2 Attempting to read 2 {os.path.basename(csv_filepath)} with encoding='utf-16-le'...")
                params = {'sep': r'\t', 'header': None}
                df = pd.read_csv(data_io, sep=params['sep'], header=params['header'])
            else:
                df = pd.read_csv(data_io, sep=params['sep'], header=params['header'])


            print("File read successfully with pandas from cleaned data!")
            return df # Success! Return the DataFrame

        except FileNotFoundError:
            print(f"Error: File not found at {os.path.basename(csv_filepath)}")
            return None
        except Exception as e:
            print(f"\nAn unexpected error occurred: {e}")
            print("Please double-check the file content and encoding.")
            return None
    

    # If the loop finishes without returning, it means all attempted encodings failed to decode
    print(f"  Failed to decode {os.path.basename(csv_filepath)} with all attempted encodings.")
    return None


 # --- Helper function to get Primary Key columns (Keep this) ---
def get_primary_key_columns(cursor: Any, schema_name: str, table_name: str) -> Optional[List[str]]:
    """
    Retrieves the column names that make up the primary key for a given table.
    """
    query = """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
         JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
         AND tc.table _name = kcu.table_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = %s
          AND tc.table_name = %s
        ORDER BY  kcu.ordinal_position;
    """
    try:
        cursor.execute(query, (schema_name, table_name))
        pk_columns = [row[0] for row in cursor.fetchall ()]
        if pk_columns:
            return pk_columns
        else:
            return None # No primary key found
    except Exception as e:
        print(f"Error retrieving primary key for {schema_name}.{table_name}: {e}")
        return None


# --- Function to get table schema (Keep this) ---
def get_table_schema_with_schema(schema_name: str, table_name: str,  conn: Any) -> Dict[str, str]:
    """
    Get the column names and their database data types for a given table.
    """
    column_info: Dict[str, str] = {}
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name  = %s;
                """,
                (schema_name, table_name)
            )
            results = cur.fetchall()
            for row in results:
                column_info[row[0]] = row [1]
        return column_info
    except Exception as e:
        print(f"Error fetching schema for {schema_name}.{table_name}: {e}")
        return {}




# --- Function to  rename DataFrame columns (Keep this) ---
def rename_dataframe_columns_from_schema(df: pd.DataFrame, db_schema: Dict[str, str]) -> pd.DataFrame:
    """
    Renames  DataFrame columns (expected to be 0, 1, 2, ...) using keys
    from a database schema dictionary.
    """
    # print('few data',df.columns) # Verbose

    new_column_names = list(db_schema.keys())

    if len(df.columns) != len(new_column_names):
        raise ValueError(
            f"Column count mismatch: DataFrame has {len(df.columns)} columns (0  to {len(df.columns)-1}), "
            f"but schema dictionary has {len(new_column_names)} keys ({new_column_names}). " # Added schema keys to error msg
            f"Cannot rename  columns if counts don't match."
        )

    df.columns = new_column_names

    print(f"Successfully renamed {len(df.columns)} columns.")
    return df


# --- Function to  convert DataFrame column types (Keep this, it's used implicitly by the cleaning in insert_dataframe_in_chunks) ---
# This function is actually NOT called directly anymore, its logic is integrated
# into the cleaning step of insert_dataframe_in _chunks. You can remove this function
# definition if you want, but keep the DB_TO_PANDAS_TYPE_MAP.
# def convert_dataframe_columns_to_db_types(df: pd .DataFrame, db_schema: Dict[str, str]) -> pd.DataFrame:
#     # ... (implementation is now inside insert_dataframe_in_chunks) ...
#     pass


# --- insert_dataframe_in_chunks  function (Modified) ---
def insert_dataframe_in_chunks(
    df: pd.DataFrame,
    conn: Any, # Type hint for connection object
    table_name_with_schema: str, #  Table name including schema (e.g., 'Sales.Address')
    db_schema: Dict[str, str], # Pass the DB schema dictionary
    chunk_size: int = 1,
    disable_fk_checks: bool = True,
    use_on_conflict_do_nothing: bool = False
) -> None:
    """
    Inserts a pandas DataFrame into a PostgreSQL table in specified chunks.
    Optionally  disables/re-enables foreign key checks and uses ON CONFLICT DO NOTHING.
    Standardizes pandas/numpy types and handles common string issues (like 'NaT', 'np.float64').

    Args:
         df (pd.DataFrame): The DataFrame to insert.
        conn: The active psycopg2 database connection object.
        table_name_with_schema (str): The name of the target table including schema (e.g ., 'schema.my_table').
        db_schema (Dict[str, str]): Dictionary mapping column names (matching DataFrame) to DB types.
        chunk_size (int): The number of rows to insert in each  batch.
        disable_fk_checks (bool): If True, disables foreign key triggers
                                 before inserting and re-enables them after.
        use_on_conflict_do_nothing (bool): If True , adds ON CONFLICT DO NOTHING clause
                                           based on the table's primary key.

    Raises:
        Exception: If a database error occurs during insertion.
        ValueError: If the DataFrame is empty or table_ name_with_schema is invalid.
    """

    print('Debug Location 1')
    if df.empty:
        print(f"DataFrame is empty. Skipping insertion for table {table_name_with_schema}.")
        return

    # ---  Validate table_name_with_schema format and split ---
    if '.' not in table_name_with_schema:
         raise ValueError(f"Table name '{ table_name_with_schema}' must include schema (e.g., 'schema.table').")
    schema_name, simple_table_name = table_name_with_schema.split('.', 1)


    df_columns = df .columns.tolist()
    num_columns = len(df_columns)

    if num_columns == 0:
        print(f"DataFrame has no columns. Skipping insertion for table  {table_name_with_schema}.")
        return

    # --- Data Cleaning and Type Standardization ---
    df_cleaned = df.copy()
    print("Starting data cleaning and type standardization...")

    # Mapping DB  types to pandas types (or hints) - Access the mapping defined  outside
    global DB_TO_PANDAS_TYPE_MAP # Assuming it's defined globally or at module level

    # --- Define common string representations of missing/invalid values ---
    # Add variations of 'NaT' and potentially other strings like 'NULL', 'None'
    # Include empty string ''
    missing_string_values = {'NaT', 'NAT', 'nat', 'NULL', 'null', 'None', 'none', ''}
    # Add 'np.float64' string as well if that was observed
    missing_string_values.add('np.float64')


    for col in df_cleaned.columns:
        # Get the target database type string for this column from the  schema
        # Use .get() with None default and lower() for case-insensitive matching
        target_db_type_str = db_schema.get(col, '').lower()
        pandas_target_hint = DB_TO_PANDAS_TYPE_MAP.get(target_db_type_str)

        # --- Replace pandas/numpy missing values (NaN, NaT, NA) with None ---
        # Do this first , so subsequent conversions don't fail on missing values
        if df_cleaned[col].isna().any():
            print(f"  Replacing pandas missing values with None in column '{col}'...") # Verbose
            df_cleaned.loc[df_cleaned[col].isna(), col] = None


        # --- **CRITICAL: Handle Timestamp/Date Columns Explicitly** ---
        # Check if the target DB type maps to pandas datetime
        if pandas_target_hint == 'datetime64[ns]':
            print(f"  Standardizing datetime column '{col}'...")

            # --- **NEW: Explicitly replace specific missing/invalid strings with None BEFORE pd.to_datetime** ---
            # Convert to string dtype safely for comparison/replacement
            # .astype(str) turns None/NaN/NaT into string representations like 'None', 'nan', 'NaT'
            # We want to replace these string representations *if they exist* in the raw data
            # before attempting datetime parsing.
            original_col_as_str = df_cleaned[col].astype(str)

            # Identify where these specific strings occur
            mask_missing_strings = original_col_as_str.isin(missing_string_values)

            # Replace these specific strings with None in the *original* column
            if mask_missing_strings.any():
                 print(f"  Replacing specific missing/invalid strings in column '{col}'...")
                 df_cleaned.loc[mask_missing_strings, col] = None
            # --- END NEW ---


            # Use pd.to_datetime with errors='coerce'. This turns invalid parsing
            # into pd.NaT. Since we replaced known strings above, this mainly handles
            # other unexpected non-date strings.
            # Add common formats if known for speed/reliability: format=['%Y-% m-%d %H:%M:%S', '%Y-%m-%d']
            # It's generally safe to apply pd.to_datetime even after setting values to None
            df_cleaned[col] = pd.to_datetime(df_cleaned[col], errors='coerce')

            # Now replace the resulting pd.NaT values (from original NaT or coerced errors) with None
            # This covers original missing values AND values that failed pd.to_datetime
            if df_cleaned[col].isna().any(): # Check again for NaT after conversion
                 # print (f"  Replacing pd.NaT (including coerced errors) with None in '{col}'...") # Verbose
                 df_cleaned.loc[df_cleaned[col].isna(), col] = None


        # ---  Specific handling for boolean columns ---
        # Ensure values are standard Python bool or None
        # Check if the target DB type maps to pandas boolean
        elif pandas_target_hint == 'boolean': # Use the pandas hint string
             print(f"  Standardizing boolean column '{col}'...")

             # --- **NEW: Explicitly replace specific missing/invalid strings with None for boolean** ---
             original_col_as_str = df_cleaned[col].astype(str)
             mask_missing_strings = original_col_as_str.isin(missing_string_values)
             if mask_missing_strings.any():
                 print(f"  Replacing specific missing/invalid strings in column '{col}'...")
                 df_cleaned.loc[mask_missing_strings, col] = None
             # --- END NEW ---

             # Convert to boolean using pd.Series.map with a dictionary for common boolean values
             bool_map = {'true': True, 'false': False, \
                         't': True, 'f': False,
                 'yes': True, 'no': False,
                 'y': True, 'n': False,
                 '1': True, '0': False,
                 1: True, 0: False,
                 True: True, False: False
             }
             # Convert to string first to handle numeric values
             df_cleaned[col] = df_cleaned[col].astype(str).str.lower()
             df_cleaned[col] = df_cleaned[col].map(bool_map)
             # Values not in the map will become None automatically


        # --- Standardize Numeric Columns ---
        # Convert to standard Python int/float or None
        # Check if the target DB type maps to a pandas numeric hint
        elif pandas_target_hint in ['Int64', 'Float64']: # Use the pandas hint strings
             print(f"  Standardizing numeric column '{col}' (target pandas dtype : {pandas_target_hint})...") # Verbose

             # --- **NEW: Explicitly replace specific missing/invalid strings with None for numeric** ---
             original_col_as_str = df_cleaned[col].astype(str)
             mask_missing_strings = original_col_as_str.isin(missing_string_values)
             if mask_missing_strings.any():
                 print(f"  Replacing specific missing/invalid strings in column '{col}'...")
                 df_cleaned.loc[mask_missing_strings, col] = None
             # --- END NEW ---

             # Use pd.to_numeric with errors='coerce' then handle resulting NaN/None
             df_cleaned[col] = pd.to_numeric( df_cleaned[col], errors='coerce')

             # After pd.to_numeric, missing values are NaN. Replace them with None.
             if df_cleaned[col].isna().any(): # Check for NaN after conversion
                  #  print(f"  Replacing NaN (including coerced errors) with None in '{col}'...") # Verbose
                  df_cleaned.loc[df_cleaned[col].isna(), col] = None
             else:
                 # If no NaN after coerce, ensure it's float/int if not None
                # Convert to standard Python float/int using .apply only if no NaNs were found
                # This avoids issues if the column was all  NaNs initially
                if pandas_target_hint == 'Int64':
                     # Ensure the column is nullable before attempting int conversion if it contains None
                     # If the column has NaNs, astype('Int64') is the way to get nullable integers
                     if df_cleaned[col].isna().any(): # This check is redundant due to .loc[isna()] above, but defensive
                          df_cleaned[col] = df_cleaned[col].astype('Int64') # Convert to nullable pandas Int64
                     else:
                          # If no NaNs, apply int conversion. This branch might be rarely hit if errors='coerce' is used.
                          df_cleaned[col] = df_cleaned[col].apply(lambda x: int(x) if x is not None else None) # Convert to Python int or None
                else: # Float64
                     # If no NaNs, apply float conversion. This branch might be rarely hit if errors='coerce' is used.
                     df_cleaned[col] = df_cleaned[col].apply(lambda x: float(x) if x is not None else None) # Convert to Python float or None


        # --- Specific handling for UUID columns ---
        # Check if the target DB type is 'uuid'
        elif target_db_type_str == 'uuid':
            print(f"  Standardizing UUID column '{col}'...")

            # --- **NEW: Explicitly replace specific missing/invalid strings with None for UUID** ---
            original_col_as_str = df_cleaned[col].astype(str)
            mask_missing_strings = original_col_as_str.isin(missing_string_values)
            if mask_missing_strings.any():
                 print(f"  Replacing specific missing/invalid strings in column '{col}'...")
                 df_cleaned.loc[mask_missing_strings, col] = None
            # --- END NEW ---

            # Apply conversion to UUID object or None (handles valid UUID strings)
            df_cleaned[col] = df_cleaned[col].apply(convert_to_uuid_or_none) # Need convert_to_uuid_or_none

            # Convert back to string representation for psycopg2 (without braces) or keep None
            # Handle pd.NA safely after apply
            df_cleaned[col] = df_cleaned[col].apply(lambda x: str(x).strip('{}') if isinstance(x, uuid.UUID) else None)
            # Ensure dtype is object or string after this
            # if not pd.api.types.is_string_dtype(df_cleaned[col].dtype) and df_cleaned[col].dtype != 'object':
            #      print(f"  Warning: UUID column '{col}' resulted in unexpected dtype {df_cleaned[col].dtype} after standardization.")

    # Verify replacement for NaT specifically if it's a datetime column (Keep this check)
    # This check is now mainly to confirm the logic *after* the main datetime handling
    # It should print "successfully replaced" if the above logic worked.
    for col in df_cleaned.columns:
        if df_cleaned[col].dtype == 'datetime64[ns]':
             has_pd_nat_after = (df_cleaned[col] == pd. NaT).any()
             if has_pd_nat_after:
                  print(f"  CRITICAL DEBUG: Column '{col}' (datetime) *still* contains pd.NaT after replacement.")
             else :
                  print(f"  Debug: Column '{col}' (datetime) successfully replaced pd.NaT with None.") # Confirm success


    print("\nData cleaning and standardization complete. Final dtypes of cleaned DataFrame:")
    print (df_cleaned.dtypes)
    print("-" * 30)
    # --- End Data Cleaning ---


    # --- Construct the INSERT SQL statement using psycopg2.sql (Keep this) ---
    # Correctly  handle schema.table notation using sql.Identifier with multiple arguments
    table_ident = sql.Identifier(schema_name, simple_table_name)
    column_idents = [sql.Identifier(col) for col in df_columns ]
    columns_list_sql = sql.SQL(', ').join(column_idents)
    placeholders_sql = sql.SQL(', ').join(sql.Placeholder() * num_columns)

    # --- Build  ON CONFLICT clause if requested (Keep this) ---
    on_conflict_clause = sql.SQL("")
    if use_on_conflict_do_nothing:
        # Need a cursor to fetch PK columns *before * the main insertion loop
        try:
            with conn.cursor() as cur:
                 pk_columns = get_primary_key_columns(cur, schema_name, simple_table_name)

            if  pk_columns is None:
                print(f"  Warning: Table {schema_name}.{simple_table_name} has no primary key. Cannot use ON CONFLICT DO NOTHING.")
                # on_conflict_clause  remains empty
            else:
                print(f"  Primary key for {simple_table_name}: {pk_columns}. Using ON CONFLICT DO NOTHING.")
                pk_columns_sql = sql.SQL(", ").join(map( sql.Identifier, pk_columns))
                on_conflict_clause = sql.SQL("ON CONFLICT ({pk_cols}) DO NOTHING").format(
                    pk_cols=pk_columns_sql
                )
        except Exception as e:
            print(f"  Error determining primary key for ON CONFLICT: {e}")
            print(f"  Proceeding without ON CONFLICT DO NOTHING for {table_name_with_schema}.")
            # on_conflict_clause remains empty

    # --- Build the final INSERT statement template ---
    insert_sql = sql.SQL("""
        INSERT INTO {table} ({columns})
        VALUES ({values})
        {on_conflict_clause}
        ;
    """).format(
        table=table_ident,
        columns=columns_list_sql,
        values=placeholders_sql,
        on_conflict_clause =on_conflict_clause # Include the built clause
    )

    # --- DEBUG: Print the generated SQL query string (Keep this) ---
    try:
        print(f"\nGenerated INSERT SQL: {insert_sql.as_string(conn)}")
    except Exception as e:
        print(f"\nError generating SQL string for debug: {e }")
        print(f"SQL Structure (approximated): INSERT INTO  {table_name_with_schema} ({', '.join(df_columns)}) VALUES (%s, ...) [ON CONFLICT ...]")
    # --- END DEBUG ---


    print(f"\nStarting bulk insert into {table_name_with_schema} in chunks of {chunk_size}...")
    # print(f"Total rows to insert: {len(df_cleaned)}") # Verbose

    num_rows = len(df_cleaned)
    num_chunks = (num_rows + chunk_size - 1) // chunk_size

    # --- Main try...finally block to ensure triggers are re-enabled ---
    try:
        # --- Disable FK checks if requested ---
        # Use  a separate cursor for ALTER TABLE commands if needed, or ensure the main cursor is not mid-transaction
        # A dedicated cursor and commit for ALTER TABLE is safer.
        if disable_fk_checks:
            print(f"  Disabling triggers (including FK checks) for {table_name_with_schema}...")
            try:
                with conn.cursor() as cur_alter: # Use a dedicated cursor for ALTER TABLE
                    cur_alter.execute(sql.SQL("ALTER TABLE {} DISABLE TRIGGER ALL;").format(table_ident))
                    conn.commit() # Commit the ALTER TABLE command
                    print(f"  Triggers disabled.")
            except Exception as e:
                 print(f"  ERROR : Failed to disable triggers for {table_name_with_schema}: {e}")
                 print("  Proceeding without disabling triggers.")
                 disable_fk_checks = False # Turn off the flag so re-enable doesn't run in finally

        print('Debug Location, starting insert section')
        # --- Insertion loop ---
        # The main cursor for insertions
        with conn.cursor() as cur_insert:
             for i in range(num_chunks):
                start_index = i * chunk_size
                end_index = min((i + 1) * chunk_size, num_rows)

                chunk_df = df_cleaned.iloc[start_index:end_index]

                # --- CRITICAL DEBUG: Inspect the data types in the first few tuples (Modified) ---
                # Check the first few tuples instead of just the first
                num_debug_tuples = min(5, len(chunk_df)) # Check up to 5 tuples
                if i == 0 and num_debug_tuples > 0:
                    print(f"\nDEBUG: Inspecting types in the FIRST {num_debug_tuples} tuple(s) of batch {i+1}/{num_chunks}...")
                    debug_tuples = list(chunk_df.itertuples(index=False, name=None))[:num_debug_tuples]
                    for tuple_index, current_tuple in enumerate(debug_tuples):
                        print(f"  Tuple {tuple_index}:")
                        for j, item in enumerate(current_tuple):
                            col_name = df_columns[j] if j < len(df_columns) else f"UnknownCol_{j}"
                            # Use repr() to show strings clearly
                            print(f"    Element {j} (Column '{col_name}'): Type is {type(item)}, Value is {item!r}")
                    print("--- END DEBUG ---")
                # --- End CRITICAL DEBUG ---


                data_for_batch = list(chunk_df.itertuples(index=False, name=None ))

                if not data_for_batch:
                    print(f"  Warning: Batch {i+1}/{ num_chunks} is empty. Skipping.")
                    continue

                try:
                    # executemany
                    # If ON CONFLICT is used, PK/UNIQUE violations here are silent skips, not errors.
                    # Other  errors (data type, FK, etc.) will still raise exceptions.
                    cur_insert.executemany(insert_sql, data_for_batch)

                    status_msg = f"  Processed chunk {i +1}/{num_chunks} ({len(data_for_batch)} rows)."
                    if use_on_conflict_do_nothing and pk_columns:
                         # Note: We don't know *how many* were  skipped, just that conflicts were handled.
                        status_msg += f" (Conflicts on {', '.join(pk_columns)} skipped)"
                    print(status_msg)

                    conn.commit() # Commit the transaction  for this batch

                except Exception as e:
                    # This block catches errors OTHER THAN unique constraint violations
                    # handled by ON CONFLICT DO NOTHING (e.g., data type errors, foreign key errors if not disabled). 
                    conn.rollback() # Rollback the failed batch
                    print(f"\n  Error inserting chunk {i+1}/{num_chunks} starting at row {start_index} into {table_name_with_schema}. Rolling back batch.")
                    print(f"  Error details: {e}")
                    raise e # Re-raise the exception


        print(f"Successfully inserted all {len(df_cleaned)} rows into { table_name_with_schema}.")

    except Exception as e:
        print(f"\nLoading process for table {table_name_with_schema} halted due to an error.")
        print(f"Details : {e}")
        # If this is part of a larger script loading multiple tables,
        # the outer loop would catch this and decide whether to stop entirely
        # or move to the next table.

    finally:
         # --- Ensure FK checks are re-enabled ---
        if disable_fk_checks: # Only try to re-enable if we successfully disabled
            print(f"  Re-enabling triggers for {table_name_with_schema}...")
            try:
                with conn.cursor() as cur_alter: # Use a dedicated cursor
                    cur_alter.execute(sql.SQL("ALTER TABLE {} ENABLE TRIGGER ALL;").format( table_ident))
                    conn.commit() # Commit the ALTER TABLE command
                print(f"  Triggers re-enabled.")
            except Exception as e:
                 print(f"  ERROR : Failed to re-enable triggers for {table_name_with_schema}: {e}")
                 print(f"  Manual intervention required to ENABLE TRIGGER ALL for table {table_name_with_schema}!")


# --- Main Execution Logic (Keep this) ---
# def main(conn):
#     # conn = None
#     # try:
#     #     conn = psycopg2.connect(**DB_CONFIG)
#     #     print("Successfully connected to PostgreSQL database.")

#     # Process files in the defined order
#     for base_filename in TABLE_LOAD_ORDER:
#         csv_file_path = os.path.join(CSV_DIR, f"{base_filename}.csv")
#         if base_filename in TABLE_SCHEMA_MAPPING:
#             full_table_name_with_schema = TABLE_SCHEMA_MAPPING[base_filename].lower()
#             schema_name = full_table_name_with_schema.split('.')[0] # Keep original  casing for schema? No, lower() is safer
#             table_name = full_table_name_with_schema.split('.')[1] # Keep original casing for table? No, lower() is safer

#             if os.path. exists(csv_file_path):
#                 print(f"\n--- Processing file: {base_filename}.csv for table {full_table_name_with_schema} ---")

#                 # --- Get DB Schema ---
#                 print('schema name is ', schema_name)
#                 print('Table Name : ', table_name)
#                 db_schema = get_table_schema_with_schema(schema_name, table_name, conn)
#                 print('what was return as db_schema : ', db_schema)

#                 if not db_schema:
#                         print(f"  Could not fetch schema for {full_table_name_with_schema}. Skipping file.")
#                         continue # Skip this file if schema fetching failed
#                 # print("  DB Schema:", db_schema) # Verbose

#                 # --- Parse CSV ---
#                 contents = parse_csv_file (csv_file_path)
#                 if contents is None:
#                     print(f"  Error parsing file {os.path.basename(csv_file_path)}. Skipping.")
#                     continue # Skip this file if parsing failed

#                 # ---  Basic DataFrame Checks ---
#                 if contents.empty or contents.shape[0] == 0:
#                     print(f"  File {os.path.basename(csv_file_path)} is empty or has no rows after  parsing. Skipping.")
#                     continue
#                 # Check if DataFrame has columns (should be true if not empty, but defensive)
#                 if contents.shape[1] == 0:
#                     print(f"  File {os.path. basename(csv_file_path)} has no columns after parsing. Skipping.")
#                     continue


#                 # --- Rename Columns ---
#                 try:
#                     # Use .copy() to avoid modifying the original 'contents' DataFrame
#                     df_renamed = rename_dataframe_columns_from_schema(contents.copy(), db_schema)
#                 except ValueError as e:
#                     print(f"  Error renaming columns for {os.path.basename(csv_file_path)}: {e}. Skipping file.")
#                     continue # Skip this file if renaming failed


#                 # --- Data Cleaning and Insertion ---
#                 # The cleaning logic is now integrated into insert_dataframe_in_chunks
#                 try:
#                     # Call the insertion function with the renamed DataFrame and schema
#                     insert_dataframe_in_chunks(
#                         df_renamed, # Pass the renamed DataFrame
#                         conn,
#                         full_table_name_with_schema , # Pass the full name with schema
#                         db_schema, # Pass the DB schema dictionary
#                         chunk_size=1000, # Adjust chunk size as needed
#                         disable_fk_checks=True, #  Set to True if FKs need to be temporarily bypassed
#                         use_on_conflict_do_nothing=True # Set to True if you want to skip PK duplicates
#                     )

#                 except Exception as e:
#                         print(f"  Error  loading data into {full_table_name_with_schema}: {e}")
#                         # The insert function already re-raises exceptions and handles rollback/trigger re-enable in finally
#                         # You might add logic here to decide  if you want to stop the whole process or continue to the next file
#                         # For now, the error message from insert_dataframe_in_chunks is informative enough.
#                         # If you want to stop the entire script on * any* file error, you could add 'raise e' here.
#                         pass # Continue to the next file if one fails

#             else:
#                 print(f"CSV file {csv_file_path} not found, but was  in load order. Skipping.")
#         else:
#             print(f"Warning: Filename '{base_filename}' is in load order but not in TABLE_SCHEMA_MAPPING. Skipping.")

#     print("\n--- Data  loading process finished ---")


def main(conn):
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
            
                    # Get number of columns from database
                    columns_from_db = len(list(column_from_db.keys()))
                    new_df = rename_dataframe_columns_from_schema(contents.copy(), column_from_db)
                    # converted_new_df = convert_dataframe_columns_to_db_types(new_df.copy(), column_from_db)
                    # Load the DataFrame into the database
                    insert_dataframe_in_chunks(new_df, conn, full_table_name,column_from_db)
                        
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
        print(f"Error : CSV directory '{CSV_DIR}' not found. Please create it and add your CSV files.")
    else:
        conn = None
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            print("Successfully connected to PostgreSQL database.")
            main(conn)
        except psycopg2.Error as e:
            print(f"Database connection error: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
        finally:
            if conn:
                conn.close()
                print("Database connection closed.")




# df: pd.DataFrame,
#     conn: Any, # Type hint for connection object
#     table_name_with_schema: str, #  Table name including schema (e.g., 'Sales.Address')
#     db_schema: Dict[str, str], # Pass the DB schema dictionary
#     chunk_size: int = 1000,
#     disable_fk_checks: bool = True,
#     use_on_conflict_do_nothing: bool = False