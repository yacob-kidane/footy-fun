# update_player_form.py
# Placeholder script to fetch player form data using soccerdata (FotMob)
# and update the local SQLite database.

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import time
import warnings 

# Suppress specific pandas warnings if they become noisy, use with caution
# warnings.filterwarnings('ignore', category=FutureWarning)

# --- Requires Installation: pip install soccerdata python-Levenshtein rapidfuzz ---
# (rapidfuzz is recommended for fuzzy matching)
try:
    from soccerdata.fotmob import FotMob
except ImportError:
    print("Error: soccerdata library not found.")
    print("Please install it using: pip install soccerdata")
    exit()
try:
    from rapidfuzz import process, fuzz
except ImportError:
     print("Warning: rapidfuzz library not found. Fuzzy matching will be basic.")
     print("Install using: pip install rapidfuzz")
     # Basic fallback or disable fuzzy matching if needed
     process = None
     fuzz = None

# --- Configuration ---
DATABASE = 'transfermarkt_data.db'
NUMBER_OF_MATCHES_FOR_FORM = 10 # How many recent matches to consider
UPDATE_INTERVAL_HOURS = 24 # Re-calculate form for players if data is older than this
FUZZY_MATCH_THRESHOLD = 85 # Minimum score for fuzzy matching name confidence

# --- Database Helper Functions ---
def get_db_connection():
    """ Creates a database connection. """
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn

def get_players_to_update(conn):
    """ Gets players from the local DB who need form updates. """
    # Fetch players who either don't have form data or whose data is too old
    threshold_time = datetime.now() - timedelta(hours=UPDATE_INTERVAL_HOURS)
    query = """
        SELECT p.player_id, p.name, p.fotmob_player_id, c.name AS club_name
        FROM players p
        JOIN clubs c ON p.current_club_id = c.club_id
        LEFT JOIN player_form_stats pfs ON p.player_id = pfs.player_id
        WHERE pfs.player_id IS NULL OR pfs.calculation_timestamp < ?
        ORDER BY p.player_id -- Or order by value etc. to prioritize
        -- LIMIT 50 -- Optional: Limit the number of players per run?
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query, (threshold_time,))
        players = cursor.fetchall()
        print(f"Found {len(players)} players potentially needing form update.")
        return [dict(row) for row in players]
    except sqlite3.Error as e:
        print(f"Database error fetching players to update: {e}")
        return []

def update_player_fotmob_id_in_db(conn, player_id, fotmob_id):
    """ Updates the fotmob_player_id in the players table. """
    query = "UPDATE players SET fotmob_player_id = ? WHERE player_id = ?;"
    try:
        cursor = conn.cursor()
        cursor.execute(query, (fotmob_id, player_id))
        conn.commit()
        print(f"    Successfully stored FotMob ID {fotmob_id} for player {player_id}")
        return True
    except sqlite3.Error as e:
        print(f"Database error updating FotMob ID for player {player_id}: {e}")
        conn.rollback()
        return False

def update_player_form_in_db(conn, player_id, avg_rating, goals, assists):
    """ Inserts or replaces form data for a player in the database. """
    timestamp = datetime.now()
    query = """
        INSERT OR REPLACE INTO player_form_stats 
            (player_id, average_rating_last_10, goals_last_10, assists_last_10, calculation_timestamp)
        VALUES (?, ?, ?, ?, ?);
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query, (player_id, avg_rating, goals, assists, timestamp))
        conn.commit()
        # print(f"Successfully updated form for player {player_id}")
        return True
    except sqlite3.Error as e:
        print(f"Database error updating form for player {player_id}: {e}")
        conn.rollback() # Rollback on error
        return False

# --- FotMob Data Fetching and Processing ---

def get_fotmob_player_id(fm: FotMob, db_player_id: int, player_name: str, club_name: str):
    """
    Attempts to find the FotMob player ID using team lookup and fuzzy matching.
    Requires significant testing and likely adaptation based on soccerdata's actual behavior.
    """
    print(f"  Mapping FotMob ID for: {player_name} ({club_name}) - TM ID: {db_player_id}")

    fotmob_team_id = None
    team_roster_df = pd.DataFrame()

    # 1. Team Resolution (Attempt)
    try:
        # ASSUMPTION: soccerdata might allow reading league tables which contain team names & IDs.
        # We might need to read tables for relevant leagues first to build a team map.
        # This is inefficient if done repeatedly. Caching team IDs would be better.
        # OR, perhaps FotMob class has a direct team search? (Not obvious from docs)
        print(f"    Attempting to find FotMob team ID for '{club_name}'... (This part needs specific soccerdata logic)")
        # Placeholder: Replace with actual soccerdata logic to find FotMob team ID for club_name
        # found_team = fm.find_team(club_name) # Hypothetical function
        # if found_team:
        #    fotmob_team_id = found_team['id']
        pass # Cannot implement without knowing soccerdata details

    except Exception as e:
        print(f"    Error during FotMob team resolution for {club_name}: {e}")
        # Continue, maybe direct player search could work (less likely)

    if fotmob_team_id is None:
        print(f"    Could not resolve FotMob team ID for club: '{club_name}'. Skipping player mapping.")
        return None # Cannot proceed reliably without team context
    else:
        print(f"    Resolved FotMob Team ID for {club_name}: {fotmob_team_id}")

    # 2. Player List Retrieval (Attempt)
    try:
        print(f"    Fetching FotMob roster for team ID: {fotmob_team_id}")
        # ASSUMPTION: soccerdata might have a way to get a team's player list.
        # Placeholder: Replace with actual soccerdata logic
        # team_roster_df = fm.read_team_players(team_id=fotmob_team_id)
        pass # Cannot implement without knowing soccerdata details

    except Exception as e:
        print(f"    Error fetching FotMob roster for team ID {fotmob_team_id}: {e}")
        return None

    if team_roster_df.empty:
        print(f"    Could not retrieve FotMob roster for team ID {fotmob_team_id}")
        return None

    # 3. Fuzzy Matching & Validation (using rapidfuzz)
    # ASSUMPTION: The DataFrame has columns 'name' and 'id' (FotMob player ID)
    if 'name' not in team_roster_df.columns or 'id' not in team_roster_df.columns:
        print(f"    Error: FotMob roster DataFrame missing expected 'name' or 'id' columns.")
        return None

    # Prepare choices for fuzzy matching (list of names)
    choices = team_roster_df['name'].tolist()
    
    if not choices:
         print(f"    FotMob roster for team {fotmob_team_id} contains no player names.")
         return None

    if process and fuzz: # Check if rapidfuzz is available
        print(f"    Running fuzzy match for '{player_name}' against {len(choices)} roster names...")
        best_match = process.extractOne(player_name, choices, scorer=fuzz.WRatio, score_cutoff=FUZZY_MATCH_THRESHOLD)
        
        if best_match:
            match_name, score, index = best_match
            matched_fotmob_row = team_roster_df.iloc[index]
            fotmob_player_id = matched_fotmob_row.get('id') # Get FotMob player ID
            print(f"    Potential FotMob match: '{match_name}' (Score: {score:.1f}) with ID: {fotmob_player_id}")
            
            # TODO: Add validation (position, age, nationality) if available in team_roster_df
            
            if fotmob_player_id:
                return str(fotmob_player_id) # Return the found FotMob ID as string for consistency
            else:
                print(f"    Match found ('{match_name}') but FotMob ID is missing in roster data.")
                return None
        else:
            print(f"    No confident match found for '{player_name}' (Threshold: {FUZZY_MATCH_THRESHOLD}).")
            return None
    else:
        print("    Skipping fuzzy matching - rapidfuzz library not available or failed import.")
        return None

def get_player_form_stats_from_fotmob(fm: FotMob, fotmob_player_id):
    """
    Fetches recent matches and calculates form stats using FotMob data.
    Highly dependent on soccerdata's ability to provide player match logs/ratings.
    """
    if fotmob_player_id is None:
        return None
        
    print(f"    Fetching FotMob match history for player ID: {fotmob_player_id}")
    try:
        # --- !!! CRITICAL IMPLEMENTATION NEEDED !!! ---
        # ASSUMPTION: Must find a way within soccerdata to get player match data.
        # Option 1: A direct player match log function (Ideal but maybe non-existent for FotMob)
        #   match_logs_df = fm.read_player_match_logs(player_id=fotmob_player_id, last_n=NUMBER_OF_MATCHES_FOR_FORM)
        
        # Option 2: Get team schedule, then get player stats for each recent match ID
        #   schedule_df = fm.read_schedule(team_id=...) # Need team ID again?
        #   recent_matches = schedule_df.sort_values('date', ascending=False).head(NUMBER_OF_MATCHES_FOR_FORM)
        #   all_match_stats = []
        #   for match_id in recent_matches['match_id']:
        #       match_player_stats = fm.read_match_player_stats(match_id=match_id) # Hypothetical
        #       player_stats_in_match = match_player_stats[match_player_stats['player_id'] == fotmob_player_id]
        #       if not player_stats_in_match.empty:
        #           all_match_stats.append(player_stats_in_match.iloc[0]) # Assuming one row per player
        #   match_logs_df = pd.DataFrame(all_match_stats)

        # Option 3: Other soccerdata sources (FBref?) might have better player logs, 
        # but require mapping to *their* IDs.

        # --- Using Placeholder Until Real Implementation --- 
        print("      !!! Using Placeholder Stats - soccerdata logic for player match history needed !!!")
        simulated_df = pd.DataFrame([
            {'rating': 7.8, 'goals': 1, 'assists': 0},{'rating': 8.2, 'goals': 2, 'assists': 1},
            {'rating': 6.5, 'goals': 0, 'assists': 0},{'rating': 7.1, 'goals': 0, 'assists': 1},
            {'rating': 7.5, 'goals': 1, 'assists': 0},{'rating': None, 'goals': 0, 'assists': 0},
            {'rating': 6.9, 'goals': 0, 'assists': 0},{'rating': 8.5, 'goals': 1, 'assists': 2},
            {'rating': 7.0, 'goals': 0, 'assists': 0},{'rating': 7.2, 'goals': 1, 'assists': 0},
        ])
        match_logs_df = simulated_df # Use placeholder
        # --- End Placeholder ---

        if match_logs_df.empty:
            print(f"      No recent match data found via soccerdata for FotMob ID: {fotmob_player_id}")
            return None

        # Ensure required columns exist (adjust names based on actual soccerdata output)
        # ASSUMPTION: Columns are named 'rating', 'goals', 'assists'
        required_cols = ['rating', 'goals', 'assists']
        if not all(col in match_logs_df.columns for col in required_cols):
            print(f"      Error: Missing required columns in fetched match data. Found: {match_logs_df.columns}")
            return None

        # Calculate stats
        # Filter for valid ratings, handle potential non-numeric data
        valid_ratings = pd.to_numeric(match_logs_df['rating'], errors='coerce').dropna()
        total_goals = pd.to_numeric(match_logs_df['goals'], errors='coerce').sum()
        total_assists = pd.to_numeric(match_logs_df['assists'], errors='coerce').sum()
        
        avg_rating = valid_ratings.mean() if not valid_ratings.empty else None
        
        print(f"      Calculated Stats: Avg Rating={avg_rating:.2f if avg_rating is not None else 'N/A'}, Goals={int(total_goals)}, Assists={int(total_assists)}")
        return {
            'avg_rating': avg_rating,
            'goals': int(total_goals),
            'assists': int(total_assists)
        }

    except Exception as e:
        print(f"    Error fetching/processing FotMob data for player {fotmob_player_id}: {e}")
        # Potentially log traceback for debugging
        # import traceback
        # traceback.print_exc()
        return None

# --- Main Execution Logic ---
if __name__ == "__main__":
    start_time = time.time()
    print(f"Starting player form update process at {datetime.now()}")
    conn = get_db_connection()
    if not conn:
        print("Fatal: Could not connect to database.")
        exit()
        
    players_to_update = get_players_to_update(conn)
    
    if not players_to_update:
        print("No players require form updates at this time.")
        conn.close()
        exit()
        
    print("Initializing FotMob data reader...")
    try:
        # You might need to specify leagues here if required by your mapping/fetching strategy
        fm = FotMob() 
        print("FotMob reader initialized.")
    except Exception as e:
        print(f"Fatal: Failed to initialize FotMob reader: {e}")
        conn.close()
        exit()

    updated_count = 0
    failed_count = 0
    map_failed_count = 0

    for player in players_to_update:
        player_id_tm = player['player_id'] # Transfermarkt ID
        print(f"Processing player: {player['name']} (TM ID: {player_id_tm}) - {player['club_name']}")
        
        fotmob_id = player.get('fotmob_player_id') # Check if ID is already known from DB
        
        if fotmob_id:
            print(f"  FotMob ID found in database: {fotmob_id}")
        else:
            # --- Step 1: Attempt to Map to FotMob ID (Needs Implementation within function) ---
            fotmob_id = get_fotmob_player_id(fm, player_id_tm, player['name'], player['club_name'])
            
            if fotmob_id:
                # Store the newly found ID in the players table
                update_player_fotmob_id_in_db(conn, player_id_tm, fotmob_id)
            else:
                print(f"  Skipping form update - Could not map {player['name']} to FotMob ID.")
                map_failed_count += 1
                time.sleep(0.2) # Small delay even on mapping failure
                print("------------------------------------")
                continue # Move to next player if mapping failed
            
        # --- Step 2: Fetch and Calculate Form Stats (Needs Implementation within function) ---
        form_stats = get_player_form_stats_from_fotmob(fm, fotmob_id)
        
        if form_stats:
            # --- Step 3: Update player_form_stats Table ---
            success = update_player_form_in_db(
                conn,
                player_id_tm, # Use the Transfermarkt ID here
                form_stats['avg_rating'],
                form_stats['goals'],
                form_stats['assists']
            )
            if success:
                updated_count += 1
            else:
                failed_count += 1
        else:
            print(f"  Skipping DB update for {player['name']} due to stat calculation error or no data.")
            failed_count += 1
            
        # --- Step 4: Delay between players ---
        print("------------------------------------")
        time.sleep(1.0) # Be respectful to FotMob API - adjust as needed

    conn.close()
    end_time = time.time()
    print(f"\nForm update process finished at {datetime.now()}")
    print(f"Total time: {end_time - start_time:.2f} seconds")
    print(f"Successfully updated: {updated_count}")
    print(f"Failed/Skipped (Fetch/Calc Error): {failed_count}")
    print(f"Failed (ID Mapping): {map_failed_count}")

    # --- Start Experimenting Here ---
    print("\\nMethods available on fm:")
    print(dir(fm))

    # Example: Try reading league table for Premier League (GB1)
    try:
        print("\\nTrying to read PL table...")
        pl_table = fm.read_league_table(league='GB1')
        print("PL Table:")
        print(pl_table.head())
        # Does this table contain usable FotMob team IDs alongside names?
    except Exception as e:
        print(f"Error reading league table: {e}")

    # Example: Try reading schedule (does it contain match IDs?)
    try:
        print("\\nTrying to read PL schedule...")
        pl_sched = fm.read_schedule(league='GB1', season='2324') # Adjust season
        print("PL Schedule:")
        print(pl_sched.head())
        # Does this contain useful FotMob match IDs?
    except Exception as e:
        print(f"Error reading schedule: {e}")

    # TODO: Add more exploration - try searching, getting team details etc.
    # based on methods listed by dir(fm) or potential guesses. 