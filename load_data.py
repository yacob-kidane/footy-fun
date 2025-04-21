import sqlite3
import pandas as pd
import os
# Remove glob as we are back to specific CSV names
# import glob 

# --- Configuration ---
DB_FILE = "transfermarkt_data.db"
# Assume CSV files are in a 'data' subdirectory relative to this script
# User needs to download these from Kaggle and place them here.
DATA_DIR = "data"
CSV_FILES = {
    # Key = table name, Value = expected CSV filename in DATA_DIR
    # *** Adjust filenames if the downloaded CSVs have different names ***
    "leagues": "leagues.csv",          # Might be competitions.csv in download
    "clubs": "clubs.csv",
    "players": "players.csv",
    "player_valuations": "player_valuations.csv"
    # Add others like 'appearances.csv' if needed
}

# --- Database Functions (remain mostly the same) ---
def create_connection(db_file):
    """ Create a database connection to the SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"SQLite version: {sqlite3.sqlite_version}")
        print(f"Connected to database: {db_file}")
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
    return conn

def create_table(conn, create_table_sql):
    """ Create a table from the create_table_sql statement """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error creating table: {e}")

def define_schema(conn):
    """ Define and create all necessary tables AND indexes """
    print("Defining database schema (if tables don't exist)...")

    # Keep the same table definitions as before
    sql_create_leagues_table = """ CREATE TABLE IF NOT EXISTS leagues (
                                        league_id TEXT PRIMARY KEY,
                                        name TEXT,
                                        country TEXT
                                    ); """
    sql_create_clubs_table = """ CREATE TABLE IF NOT EXISTS clubs (
                                    club_id INTEGER PRIMARY KEY,
                                    name TEXT,
                                    domestic_competition_id TEXT,
                                    FOREIGN KEY (domestic_competition_id) REFERENCES leagues (league_id)
                                ); """
    sql_create_players_table = """ CREATE TABLE IF NOT EXISTS players (
                                    player_id INTEGER PRIMARY KEY,
                                    name TEXT,
                                    current_club_id INTEGER,
                                    date_of_birth DATE,
                                    position TEXT,
                                    sub_position TEXT,
                                    foot TEXT,
                                    height_cm INTEGER,
                                    nationality TEXT,
                                    image_url TEXT,
                                    agent_name TEXT,
                                    FOREIGN KEY (current_club_id) REFERENCES clubs (club_id)
                                ); """
    sql_create_player_valuations_table = """ CREATE TABLE IF NOT EXISTS player_valuations (
                                                valuation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                player_id INTEGER NOT NULL,
                                                date DATE,
                                                market_value_in_eur INTEGER,
                                                current_club_id INTEGER,
                                                player_club_domestic_competition_id TEXT,
                                                FOREIGN KEY (player_id) REFERENCES players (player_id),
                                                FOREIGN KEY (current_club_id) REFERENCES clubs (club_id),
                                                FOREIGN KEY (player_club_domestic_competition_id) REFERENCES leagues (league_id)
                                            ); """

    # Create tables
    create_table(conn, sql_create_leagues_table)
    create_table(conn, sql_create_clubs_table)
    create_table(conn, sql_create_players_table)
    create_table(conn, sql_create_player_valuations_table)

    print("Creating indexes (if they don't exist)...")
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_player_valuations_player_date ON player_valuations (player_id, date);",
        "CREATE INDEX IF NOT EXISTS idx_player_valuations_date ON player_valuations (date);",
        "CREATE INDEX IF NOT EXISTS idx_players_club ON players (current_club_id);",
        "CREATE INDEX IF NOT EXISTS idx_clubs_league ON clubs (domestic_competition_id);"
    ]
    for index_sql in indexes:
        try:
             c = conn.cursor()
             c.execute(index_sql)
             conn.commit()
        except sqlite3.Error as e:
             print(f"Error creating index '{index_sql[:50]}...': {e}")

    print("Schema definition complete.")

# --- Data Loading Function (Reverted to CSV) ---
def load_csv_to_table(conn, table_name, csv_file_path):
    """ Load data from a CSV file into the specified table using pandas """
    if not os.path.exists(csv_file_path):
        print(f"Error: CSV file not found at {csv_file_path}. Skipping table '{table_name}'.")
        return False # Indicate failure

    print(f"Loading data from {csv_file_path} into table '{table_name}'...")
    try:
        df = pd.read_csv(csv_file_path)
        
        # Basic check for empty dataframe
        if df.empty:
             print(f"Warning: CSV file {csv_file_path} is empty or contains only headers. Skipping table '{table_name}'.")
             return True # Not an error, just no data

        # *** Add Data Cleaning/Type Conversion if needed before loading ***
        # Example: Convert date columns
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y-%m-%d')
        if 'date_of_birth' in df.columns:
             df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce').dt.strftime('%Y-%m-%d')
        # Coerce numeric types (example for market_value_in_eur)
        if 'market_value_in_eur' in df.columns:
            df['market_value_in_eur'] = pd.to_numeric(df['market_value_in_eur'], errors='coerce').fillna(0).astype(int)

        # Get table info to check columns more accurately (optional enhancement)
        # cursor = conn.cursor()
        # cursor.execute(f"PRAGMA table_info({table_name})")
        # table_columns = [info[1] for info in cursor.fetchall()]
        # print(f"Table {table_name} columns: {table_columns}")
        # print(f"CSV {os.path.basename(csv_file_path)} columns: {df.columns.tolist()}")
        
        # Ensure CSV columns match table columns (simple subset check)
        # More robust checking would involve renaming columns in the DataFrame
        # df_columns_lower = [col.lower() for col in df.columns]
        # table_columns_lower = [col.lower() for col in table_columns]
        # valid_columns = [col for col in df.columns if col.lower() in table_columns_lower]
        # if len(valid_columns) != len(df.columns):
        #      print(f"Warning: Some CSV columns might not map to table {table_name}")
        # df_to_load = df[valid_columns] # Select only columns that exist in the table
        df_to_load = df # Assuming columns match for now

        # Use pandas.to_sql to load data
        # 'replace' drops the table first - useful for full reloads
        df_to_load.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"Successfully loaded {len(df_to_load)} rows into '{table_name}'.")
        return True # Indicate success

    except pd.errors.EmptyDataError:
        print(f"Warning: CSV file {csv_file_path} is empty. Skipping table '{table_name}'.")
        return True # Not an error
    except Exception as e:
        print(f"Error loading data into table '{table_name}' from {csv_file_path}: {e}")
        return False # Indicate failure

# --- Main Execution ---
if __name__ == "__main__":
    print("Starting data loading process from CSV files...")
    conn = create_connection(DB_FILE)

    if conn is not None:
        # Define schema (Create tables and indexes first)
        define_schema(conn)

        # Load data from CSVs
        print("\nLoading data from CSV files...")
        all_successful = True
        # Load in order respecting foreign keys
        load_order = ["leagues", "clubs", "players", "player_valuations"]

        for table_key in load_order:
            if table_key in CSV_FILES:
                csv_file = CSV_FILES[table_key]
                file_path = os.path.join(DATA_DIR, csv_file)
                success = load_csv_to_table(conn, table_key, file_path)
                if not success:
                    all_successful = False # Mark failure if any load fails
            else:
                 print(f"Warning: CSV file key '{table_key}' not found in CSV_FILES dictionary.")


        # Close the connection
        print("\nClosing database connection.")
        conn.close()
        if all_successful:
            print("CSV loading process finished.")
        else:
            print("CSV loading process finished with errors.")

    else:
        print("Error! Cannot create the database connection.") 