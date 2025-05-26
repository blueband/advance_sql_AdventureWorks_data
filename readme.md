# Advanced SQL Tutorial with AdventureWorks Database

## Table of Contents
1. Project Overview
   - Purpose
   - Database Description
   - Project Structure

2. Setup Requirements
   - Prerequisites
   - System Requirements
   - Required Software
   - Python Dependencies

3. Database Setup
   - Schema Creation
   - Table Creation
   - Data Import Process

4. Data Processing Pipeline
   - CSV File Structure
   - Data Transformation
   - Data Loading Process
   - Handling Encodings

5. SQL Examples
   - Basic Queries
   - Advanced Examples
   - Performance Optimization

6. Project Structure
   - Directory Layout
   - File Descriptions
   - Key Components

7. Usage Guide
   - Installation Steps
   - Configuration
   - Running the Scripts
   - Troubleshooting

8. Contributing
   - Guidelines
   - Development Setup
   - Testing

9. License & Attribution
   - License Information
   - Data Source Credits
   - Acknowledgments

## 1. Project Overview

### Purpose
This project serves as a practical guide for learning advanced SQL concepts using the AdventureWorks database. It includes data processing scripts, database setup, and example queries for real-world business scenarios.

### Database Description
AdventureWorks represents a fictional bicycle manufacturer, featuring:
- Multiple business areas (Sales, Production, Purchasing, etc.)
- Complex relationships between entities
- Real-world business scenarios
- Rich dataset for advanced SQL practice

The complete database structure is visualized in the Entity-Relationship Diagram:
[View Full ER Diagram](./images/AdventureWorksERD.jpeg)

The database consists of five main schemas:
1. **Person**: Customer and person contact information
2. **HumanResources**: Employee-related data
3. **Production**: Product details and inventory
4. **Purchasing**: Vendor and purchase order information
5. **Sales**: Customer, sales orders, and store data

### Table Creation Details
The tables are created using a structured SQL script (`ddLsql/create_tables.sql`) that:

1. Sets up initial requirements:
   ```sql
   -- Enable UUID generation
   CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

   -- Create Schemas
   CREATE SCHEMA IF NOT EXISTS Person;
   CREATE SCHEMA IF NOT EXISTS HumanResources;
   CREATE SCHEMA IF NOT EXISTS Production;
   CREATE SCHEMA IF NOT EXISTS Purchasing;
   CREATE SCHEMA IF NOT EXISTS Sales;
   CREATE SCHEMA IF NOT EXISTS dbo;
   ```

2. Implements comprehensive data types:
   - SERIAL for auto-incrementing IDs
   - UUID for unique identifiers
   - Timestamps for date tracking
   - Decimal for precise financial calculations
   - Various string types (VARCHAR, TEXT)

3. Enforces data integrity through:
   - Primary and Foreign Keys
   - CHECK constraints
   - DEFAULT values
   - NOT NULL constraints
   - UNIQUE constraints

### Project Structure
The project is organized into two main sections:
1. Data Processing & Setup
2. SQL Examples & Tutorials

## 2. Setup Requirements

### Prerequisites
- Linux/Unix-based operating system
- Python 3.8 or higher
- PostgreSQL 12 or higher

### System Requirements
- Minimum 4GB RAM
- 2GB free disk space
- Internet connection for initial setup

### Required Software
- PostgreSQL Server
- Python 3.x
- pip (Python package manager)

### Python Dependencies
```bash
pandas
psycopg2-binary
numpy
```

## 3. Database Setup

### Schema Creation
1. Database schemas are defined in `ddLsql/create_schema.sql`
2. Includes schemas for:
   - Person
   - Production
   - Sales
   - Purchasing
   - HumanResources

### Table Creation
Tables are created using `ddLsql/create_tables.sql`, which follows a systematic approach:

1. **Schema Organization**:
   - Tables are grouped by business function
   - Each schema represents a distinct business area
   - Logical separation of concerns

2. **Table Dependencies**:
   - Tables are created in order of their dependencies
   - Foreign key relationships are properly established
   - Referential integrity is maintained

3. **Key Examples**:
   ```sql
   -- Person tables
   CREATE TABLE Person.Person (
       BusinessEntityID INT PRIMARY KEY,
       PersonType CHAR(2) NOT NULL,
       NameStyle BOOLEAN NOT NULL DEFAULT FALSE,
       -- ... additional columns
   );

   -- HumanResources tables
   CREATE TABLE HumanResources.Employee (
       BusinessEntityID INT PRIMARY KEY,
       NationalIDNumber VARCHAR(15) NOT NULL,
       -- ... additional columns
       FOREIGN KEY (BusinessEntityID) REFERENCES Person.Person
   );
   ```

4. **Data Integrity**:
   - CHECK constraints for data validation
   - DEFAULT values for standard fields
   - UNIQUE constraints where needed

### Data Import Process
The data import process is handled by `populate_table.py`, which:
- Reads CSV files from the `data/` directory
- Handles various encodings (UTF-8, UTF-16)
- Manages data type conversions
- Maintains referential integrity

## 4. Data Processing Pipeline

### CSV File Structure
The `data/` directory contains 68 CSV files, including:
- Business entity data
- Product information
- Sales records
- Employee data
- Geographic information

### Data Transformation
Handled by `populate_table.py`:
- Automatic data type conversion
- NULL value handling
- Date/time format standardization
- UUID processing
- Boolean value normalization

### Data Loading Process
The loading process:
1. Reads CSV files with proper encoding
2. Transforms data to match PostgreSQL types
3. Loads data in chunks for better performance
4. Handles foreign key constraints

### Handling Encodings and Common Issues
The project includes sophisticated encoding handling for CSV files, particularly in `populate_table.py`. Different files require different encoding approaches:

#### File-Specific Encodings
1. **UTF-16 LE Files**:
   ```python
   utf_16_encodings_file = (
       'BusinessEntityAddress', 'Employee', 'Person', 'EmailAddress',
       'Password', 'PersonPhone', 'PhoneNumberType', 'ProductPhoto',
       'BusinessEntity', 'ProductModel', 'CountryRegionCurrency', 'Store',
       'Illustration', 'JobCandidate', 'Document', 'ProductDescription'
   )
   ```

2. **UTF-8 Files**:
   ```python
   utf_8_encodings_file = (
       'ProductReview',
       'Product',
       'Location'
   )
   ```

#### Common Issues and Solutions

1. **Encoding Detection Issues**:
   - **Problem**: Character encoding mismatch causing garbled data
   - **Solution**: Script attempts multiple encodings in order:
     ```python
     encodings_to_try = [
         'cp1252',
         'utf-8-sig',
         'utf-16',
         'latin-1'
     ]
     ```

2. **Null Bytes in Data**:
   - **Problem**: Null bytes (\x00) causing parsing errors
   - **Solution**: Automatic removal of null bytes:
     ```python
     cleaned_string = decoded_string.replace('\x00', '')
     ```

3. **BOM (Byte Order Mark) Issues**:
   - **Problem**: BOM interfering with data parsing
   - **Solution**: Using 'utf-8-sig' encoding where appropriate

4. **Memory Efficiency**:
   - **Problem**: Large files causing memory issues
   - **Solution**: Using `low_memory=False` in pandas for reliable parsing:
     ```python
     read_csv_params = {
         'sep': '\t',
         'header': None,
         'low_memory': False
     }
     ```

#### Troubleshooting Guide

If you encounter encoding issues:

1. **Check File Type**:
   ```bash
   file -i your_file.csv    # Check file encoding
   hexdump -C -n 32 your_file.csv  # View file header bytes
   ```

2. **Manual Encoding Override**:
   - Modify the encoding lists in `populate_table.py`
   - Add specific files to appropriate encoding groups

3. **Data Validation**:
   - Use the debug output to verify correct character encoding
   - Check for data integrity after import
   - Verify special characters are preserved

#### Performance Considerations
- Binary reading mode for better encoding handling
- StringIO for efficient in-memory processing
- Chunk-based processing for large files
- Explicit error handling with fallback options

## 5. SQL Examples

### Basic Queries
Located in `example/` directory:
- Data retrieval operations
- Filtering and sorting
- Joins and relationships
- Aggregation functions

### Advanced Examples
Complex SQL operations demonstrating:
- Window functions
- Common Table Expressions (CTEs)
- Recursive queries
- Performance optimization techniques

### Performance Optimization
- Indexing strategies
- Query optimization techniques
- Best practices for complex queries

## 6. Project Structure

### Directory Layout
```
advanced_sql_tutorial/
├── data/               # CSV data files
├── ddLsql/            # SQL schema and table definitions
├── example/           # SQL example queries
├── images/            # Documentation images
├── populate_table.py  # Main data loading script
├── populate2.py       # Additional loading utilities
└── readme.md         # Project documentation
```

### File Descriptions
- `populate_table.py`: Main data processing and loading script
- `create_schema.sql`: Database schema definitions
- `create_tables.sql`: Table creation scripts
- `example/*.sql`: Example SQL queries and tutorials

### Key Components
1. Data Processing Scripts
2. Database Setup Files
3. Example Queries
4. Documentation

## 7. Usage Guide

### Installation Steps
1. Clone the repository
2. Install PostgreSQL
3. Install Python dependencies
4. Create the database
5. Run schema and table creation scripts

### Configuration
1. Update database configuration in `populate_table.py`:
```python
DB_CONFIG = {
    "host": "localhost",
    "port": "5433",
    "dbname": "postgres",
    "user": "data_eng",
    "password": "12345pP"
}
```

### Running the Scripts
1. Create database schemas:
```bash
psql -U data_eng -d postgres -f ddLsql/create_schema.sql
```

2. Create tables:
```bash
psql -U data_eng -d postgres -f ddLsql/create_tables.sql
```

3. Load data:
```bash
python populate_table.py
```

### Troubleshooting
Common issues and solutions:
- Encoding errors: Check file encoding and use appropriate parameters
- Memory issues: Adjust chunk size in data loading
- Permission errors: Verify database user privileges

## 8. Contributing

### Guidelines
- Follow PEP 8 style guide for Python code
- Document all SQL queries
- Include tests for new features

### Development Setup
1. Fork the repository
2. Create a virtual environment
3. Install development dependencies
4. Create a feature branch

### Testing
- Test new SQL queries
- Verify data integrity
- Check performance impact

## 9. License & Attribution

### License Information
This project is licensed under MIT License.

### Data Source Credits
- Original database design by Microsoft
- AdventureWorks sample database
- Official Dataset: [Microsoft AdventureWorks Sample Databases](https://github.com/Microsoft/sql-server-samples/tree/master/samples/databases/adventure-works)
- Download Link: [AdventureWorks2019.bak](https://github.com/Microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorks2019.bak)

### Download Instructions
1. Direct download:
   ```bash
   wget https://github.com/Microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorks2019.bak
   ```
2. Alternative sources:
   - [Microsoft Docs - AdventureWorks Installation Guide](https://learn.microsoft.com/en-us/sql/samples/adventureworks-install-configure)
   - [GitHub Release Page](https://github.com/Microsoft/sql-server-samples/releases/tag/adventureworks)

### Acknowledgments
- Microsoft for the original AdventureWorks database (2019)
- PostgreSQL community
- Contributors to the project
- Microsoft SQL Server Samples team for maintaining the dataset