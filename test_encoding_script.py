
filename = "/home/blueberry/Desktop/advanced_sql_tutorial/data/Person.csv" # <--- CHANGE THIS TO YOUR FILENAME
# num_lines_to_inspect = 10

# try:
#     with open(filename, 'rb') as f: # 'rb' means read in binary mode
#         print(f"Inspecting raw bytes of the first {num_lines_to_inspect} lines of '{filename}':\n")
#         for i in range(num_lines_to_inspect):
#             line_bytes = f.readline()
#             if not line_bytes: # End of file
#                 print(f"--- End of file reached before {num_lines_to_inspect} lines ---")
#                 break
#             print(f"Line {i+1} (raw bytes): {line_bytes!r}") # !r shows escape sequences
#             # Let's try to decode with common encodings to see what it looks like
#             try:
#                 print(f"  Decoded as UTF-8:       '{line_bytes.decode('utf-8', errors='replace')[:100].strip()}'")
#             except Exception:
#                 print("  Could not decode as UTF-8")
#             try:
#                 print(f"  Decoded as UTF-16-LE:   '{line_bytes.decode('utf-16-le', errors='replace')[:100].strip()}'")
#             except Exception:
#                 print("  Could not decode as UTF-16-LE")
#             try:
#                 print(f"  Decoded as UTF-16-BE:   '{line_bytes.decode('utf-16-be', errors='replace')[:100].strip()}'")
#             except Exception:
#                 print("  Could not decode as UTF-16-BE")
#             try:
#                 print(f"  Decoded as latin-1:     '{line_bytes.decode('latin-1', errors='replace')[:100].strip()}'")
#             except Exception:
#                 print("  Could not decode as latin-1")
#             print("-" * 30)
# except FileNotFoundError:
#     print(f"Error: File '{filename}' not found.")
# except Exception as e:
#     print(f"An error occurred: {e}")


# Run this script and paste the output here. The !r in print(f"{line_bytes!r}") will show the byte string representation (e.g., b'\xff\xfeH\x00e\x00l\x00l\x00o\x00'). This is what I need to analyze.

# Method 2: Use a library to detect the encoding.

# The chardet library is excellent for this.

# Install chardet (if you haven't already):

# pip install chardet pandas
# IGNORE_WHEN_COPYING_START
# content_copy
# download
# Use code with caution.
# Bash
# IGNORE_WHEN_COPYING_END

# Run this Python script:
# Replace "your_file.csv" with your actual filename. This script will try to detect the encoding of a sample of your file and then attempt to read the first 10 lines with Pandas using that detected encoding.

# import pandas as pd
# import chardet

# filename = "your_file.csv"  # <--- CHANGE THIS TO YOUR FILENAME
# sample_size = 1024 * 1024  # Read first 1MB for detection, adjust if needed

# detected_encoding = None
# detected_confidence = 0

# try:
#     print(f"Attempting to detect encoding for '{filename}'...")
#     with open(filename, 'rb') as f:
#         raw_data = f.read(sample_size) # Read a sample of the file

#     result = chardet.detect(raw_data)
#     detected_encoding = result['encoding']
#     detected_confidence = result['confidence']

#     if detected_encoding:
#         print(f"Detected encoding: {detected_encoding} (Confidence: {detected_confidence*100:.2f}%)")

#         # If chardet is very confident about UTF-16, it might detect 'UTF-16'
#         # Pandas needs the specific 'utf-16-le' or 'utf-16-be'
#         # Or sometimes it might detect 'UTF-16LE' and pandas needs 'utf-16-le'
#         if detected_encoding.upper().startswith('UTF-16'):
#             if b'\xff\xfe' == raw_data[:2]: # BOM for little-endian
#                 print("BOM detected: UTF-16-LE")
#                 detected_encoding = 'utf-16-le'
#             elif b'\xfe\xff' == raw_data[:2]: # BOM for big-endian
#                 print("BOM detected: UTF-16-BE")
#                 detected_encoding = 'utf-16-be'
#             # If no BOM, chardet might still be right, or pandas might guess from content
#             # Forcing to lowercase and replacing common variations for pandas
#             detected_encoding = detected_encoding.lower().replace('utf-16le', 'utf-16-le').replace('utf-16be', 'utf-16-be')


#         print(f"Attempting to read with pandas using encoding: '{detected_encoding}'")
#         # Try reading the first 10 lines with pandas
#         df = pd.read_csv(filename, encoding=detected_encoding, nrows=10)
#         print("\nSuccessfully read first 10 lines with pandas:")
#         print(df)

#     else:
#         print("Could not detect encoding with chardet.")
#         print("You might need to manually inspect the file or try common encodings.")

# except FileNotFoundError:
#     print(f"Error: File '{filename}' not found.")
# except UnicodeDecodeError as e:
#     print(f"\nUnicodeDecodeError when reading with pandas using '{detected_encoding}': {e}")
#     print("This means the detected encoding might be incorrect, or there are mixed encodings in the file.")
#     print("Try other common encodings like 'utf-8', 'latin1', 'iso-8859-1', or ensure the file is consistently encoded.")
# except Exception as e:
#     print(f"\nAn error occurred: {e}")
#     print("If chardet suggested an encoding, ensure it's one pandas recognizes (e.g., 'utf-8', 'utf-16-le', 'latin-1').")

# print("\n--- Common Pandas `read_csv` attempts (if above failed or for comparison) ---")
# common_encodings_to_try = ['utf-8', 'utf-16-le', 'utf-16-be', 'latin1', 'iso-8859-1', 'cp1252']
# if detected_encoding and detected_encoding not in common_encodings_to_try:
#     common_encodings_to_try.insert(0, detected_encoding) # Try detected first

# for enc in common_encodings_to_try:
#     print(f"\nTrying encoding: '{enc}'")
#     try:
#         df_attempt = pd.read_csv(filename, encoding=enc, nrows=10)
#         print(f"Successfully read with '{enc}':")
#         print(df_attempt.head())
#         # If you want to be sure, check if the text looks correct
#         # For example, print a specific column known to have non-ASCII text
#         # if not df_attempt.empty and len(df_attempt.columns) > 0:
#         # print(f"Sample from first column: {df_attempt.iloc[0,0]}")
#         break # Stop if successful
#     except UnicodeDecodeError:
#         print(f"UnicodeDecodeError with '{enc}'.")
#     except Exception as e_pandas:
#         print(f"Error with '{enc}': {e_pandas}")


import pandas as pd
import os
import io # Import io to treat a string as a file

csv_filepath = 'Person.csv' # Make sure this path is correct

# Define your parameters
params = {
    'sep': r'\+\|',  # Correct regex for literal '+|'
    'header': None,  # No header row in the data section
    # We will handle encoding manually first, so don't pass it to read_csv directly
    # 'encoding': 'utf-16-le', 
    # 'low_memory': False # Irrelevant when reading from StringIO
}

file_encoding = 'utf-16-le' # Specify the encoding for manual decoding

print(f"Attempting to read and clean {filename} with encoding='{file_encoding}'...")

cleaned_data = None
try:
    # 1. Read the file in binary mode
    with open(filename, 'rb') as f:
        raw_bytes = f.read()

    # 2. Decode the bytes using the specified encoding
    # Errors='ignore' can help if there are genuinely un-decodable sequences,
    # but ideally, the encoding is correct. Try without errors='ignore' first.
    try:
        decoded_string = raw_bytes.decode(file_encoding)
        print("File decoded successfully.")
    except UnicodeDecodeError as e:
        print(f"UnicodeDecodeError during decoding: {e}")
        print("The file might not be strictly '{file_encoding}'. Trying with errors='ignore'.")
        decoded_string = raw_bytes.decode(file_encoding, errors='ignore')


    # 3. Clean the string - remove common invisible characters like null bytes
    # The VS Code warning strongly suggests this is needed.
    cleaned_string = decoded_string.replace('\x00', '') # Remove null bytes
    # You might need to add other characters here if inspection reveals them

    print("Invisible characters removed.")

    # 4. Use io.StringIO to make the cleaned string look like a file to pandas
    data_io = io.StringIO(cleaned_string)

    # 5. Let pandas read from the StringIO object
    # Don't pass encoding or low_memory when reading from StringIO
    df = pd.read_csv(data_io, sep=params['sep'], header=params['header'])

    print("File read successfully with pandas from cleaned data!")
    print(f"Shape of DataFrame: {df.shape}")
    print("First 5 rows:")
    print(df.head())

except FileNotFoundError:
    print(f"Error: File not found at {csv_filepath}")
except Exception as e:
    print(f"\nAn unexpected error occurred: {e}")
    print("Please double-check the file content and encoding.")

