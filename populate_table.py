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
    "SalesOrderHeader": "Sales.SalesOrderHeader" ,
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
    "CountryRegionCurrency": "Sales.CountryRegionCurrency"
}

# --- Define the order of loading tables (Keep this, verify it's correct for FKs) ---
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
    # "JobCandidate", # Can depend on Employee (nullable FK)
    "Document", # Depends on Employee (Owner) - HIERARCHYID (DocumentNode) needs careful handling if not text

    # Production (Products and related components)
    "ProductSubcategory", # Depends on ProductCategory
    "ProductModel", # No direct FKs in its main columns, but linked from Product
    "Product", # Depends on ProductModel, ProductSubcategory, UnitMeasure
    "BillOfMaterials", # Depends on Product (ProductAssemblyID, ComponentID), UnitMeasure
    "ProductCostHistory", # Depends on Product
    "ProductListPriceHistory", # Depends on Product
    # "ProductReview", # Depends on Product
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