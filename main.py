import os
import re
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables from .env file
load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', 5432),
    'database': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres'),
    'schema': os.getenv('DB_SCHEMA', 'public')  # Default schema is 'public'
}

# Function to connect to PostgreSQL database and return connection
def connect_db(config):
  try:
    conn = psycopg2.connect(
        host=config['host'],
        port=config['port'],
        database=config['database'],
        user=config['user'],
        password=config['password'],
        client_encoding='UTF8'
    )
    return conn
  except Exception as e:
    print(f"Error connecting to database: {str(e)}")
    return None

# Function to inject a single CSV file into a PostgreSQL table
def inject_single(csv_path, table_name, schema_name='public', conn=None):
  try:
    print("\n" + "=" * 108)
    print("Start Injection...")
    print("-" * 108)
    print(f"CSV File: {csv_path}")
    print(f"Postgres Schema: {schema_name}")
    print(f"Postgres Table Name: {table_name}")

    # Check if file exists
    if not os.path.exists(csv_path):
      print(f"Error: File not found! - '{csv_path}'")
      print("-" * 108)
      return False

    # Use provided connection or create a new one if None
    conn_provided = conn is not None
    if not conn_provided:
      conn = connect_db(DB_CONFIG)
    if not conn:
      return False

    cursor = conn.cursor()

    # Ensure schema exists
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    conn.commit()

    # Analyze CSV Header
    df_headers = pd.read_csv(csv_path, nrows=0, dtype=str)
    headers = list(df_headers.columns)

    # Clean column names (PostgreSQL compatibility)
    clean_headers = []
    for header in headers:
      clean_header = re.sub(r'[^\w가-힣]', '_', header).lower()
      # if clean_header and clean_header[0].isdigit():
      #   clean_header = 'col_' + clean_header
      clean_headers.append(clean_header)

    # Drop existing temp table if exists
    cursor.execute("DROP TABLE IF EXISTS csv_temp")

    # Create new temp table
    columns_def = ', '.join([f'"{h}" TEXT' for h in clean_headers])
    create_temp_table_sql = f"CREATE TEMP TABLE csv_temp ({columns_def})"
    cursor.execute(create_temp_table_sql)

    # Count total rows for progress tracking
    total_rows = sum(1 for _ in open(csv_path)) - 1  # Subtract header row

    # Load data with progress bar
    with tqdm(total=total_rows, desc="Loading data", ncols=100) as pbar:
      # Use chunked processing to show progress
      chunk_size = 10000
      for chunk in pd.read_csv(csv_path, chunksize=chunk_size, dtype=str):
        # Replace column names with clean names
        chunk.columns = clean_headers

        # Insert chunk data
        with conn.cursor() as chunk_cursor:
          # Create a temporary file for this chunk
          tmp_file = 'tmp_chunk.csv'
          chunk.to_csv(tmp_file, index=False)

          # Use COPY command for this chunk
          with open(tmp_file, 'r') as f:
            next(f)  # Skip header
            chunk_cursor.copy_expert("COPY csv_temp FROM STDIN WITH CSV", f)

        # Update progress bar
        pbar.update(len(chunk))

      # Clean up temp file if it exists
      if os.path.exists('tmp_chunk.csv'):
        os.remove('tmp_chunk.csv')

    # Create JSON table
    full_table_name = f"{schema_name}.{table_name}"
    cursor.execute(f"DROP TABLE IF EXISTS {full_table_name}")
    create_json_table_sql = f"""
      CREATE TABLE {full_table_name} (
        data JSON
      )
    """
    cursor.execute(create_json_table_sql)

    # Convert and insert data as JSON
    with tqdm(total=1, desc="Insert Data into Postgres", ncols=100) as pbar:
      json_convert_sql = f"""
        INSERT INTO {full_table_name} (data)
        SELECT row_to_json(t)
        FROM (SELECT * FROM csv_temp) t
      """
      cursor.execute(json_convert_sql)
      pbar.update(1)

    # Commit and clean up
    conn.commit()
    cursor.close()

    # Only close connection if we created it here
    if not conn_provided and conn:
      conn.close()

    print("-" * 108)
    print("Injection is done!")
    print("=" * 108 + "\n")
    return True

  except Exception as e:
    print(f"Error: {str(e)}")
    print("-" * 108)
    # Only close connection if we created it here
    if not conn_provided and 'conn' in locals() and conn:
      conn.close()
    return False

# Function to inject multiple CSV files into PostgreSQL tables
def inject_multiple(directory, schema_name='public', use_prefix=False, table_prefix='data_', conn=None):
  # Check if directory exists
  if not os.path.exists(directory) or not os.path.isdir(directory):
    print(f"Error: Directory not found or not a directory - {directory}")
    return False

  # Get list of CSV files
  csv_files = [f for f in os.listdir(directory) if f.lower().endswith('.csv')]
  if not csv_files:
    print("No CSV files found in the directory.")
    return False

  print("\n" + "=" * 108)
  print("Show Information")
  print("-" * 108)
  print(f"CSV Files Directory: {directory}")
  print(f"Postgres Schema: {schema_name}")
  if use_prefix:
    print(f"Postgres Table Prefix: {table_prefix}")
  else:
    print("No prefix will be used for table names.")
  print("=" * 108)

  # Use provided connection or create a new one if None
  conn_provided = conn is not None
  if not conn_provided:
    conn = connect_db(DB_CONFIG)
  if not conn:
    return False

  # Process each CSV file with progress bar
  success_count = 0
  for csv_file in csv_files:
    csv_path = os.path.join(directory, csv_file)

    # Create table name based on file name
    if use_prefix:
      table_name = table_prefix + os.path.splitext(csv_file)[0].lower()
    else:
      table_name = os.path.splitext(csv_file)[0].lower()

    table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)

    # Process file without showing individual processing message
    success = inject_single(csv_path, table_name, schema_name, conn)

    if success:
      success_count += 1

  # Only close connection if we created it here
  if not conn_provided and conn:
    conn.close()

  print(f"{success_count}/{len(csv_files)} files processed successfully.")
  return success_count > 0

# Function to show interactive menu for CSV to PostgreSQL injection
def show_interactive(conn=None):
  # Use provided connection or create a new one if None
  conn_provided = conn is not None
  if not conn_provided:
    conn = connect_db(DB_CONFIG)

  while True:
    print("\n" + "*" * 108)
    print("*" + " " * 38 + "CSV Files to Postgres Injector" + " " * 38 + "*")
    print("*" * 108)
    print("1. Inject a single CSV file")
    print("2. Inject multiple CSV files")
    print("3. Exit")
    print("-" * 108)

    choice = input("Choose an Option [1-3]: ")

    if choice == '1':
      csv_path = input("Enter the CSV File Path: ")
      schema_name = input("Enter the Postgres Schema Name [default: public]: ")
      table_name = input("Enter the Postgres Table Name: ")

      if not schema_name:
        schema_name = 'public'

      inject_single(csv_path, table_name, schema_name, conn)

    elif choice == '2':
      directory = input("Enter the CSV Files Directory: ")
      schema_name = input("Enter the Postgres Schema Name [default: public]: ")

      if not schema_name:
        schema_name = 'public'

      use_prefix = input("Do you want to use a Prefix for Table Names? (y/n) [default: n]: ").lower() == 'y'

      table_prefix = ''
      if use_prefix:
        table_prefix = input("Enter the Table Prefix [default: data_]: ")
        if not table_prefix:
          table_prefix = 'data_'

      inject_multiple(directory, schema_name, use_prefix, table_prefix, conn)

    elif choice == '3':
      print("\nExiting...\n")
      # Close connection before exiting if we created it
      if not conn_provided and conn:
        conn.close()
      break

    else:
      print("Invalid choice. Please try again.")

# Main function to establish DB connection and start interactive mode
def main():
  # Establish a single database connection for the entire session
  conn = connect_db(DB_CONFIG)
  if conn:
    # Pass the connection to the interactive interface
    show_interactive(conn)
  else:
    print("Failed to connect to the database. Please check your configuration.")
    return

# Call main() only if script is executed directly
if __name__ == "__main__":
  main()
