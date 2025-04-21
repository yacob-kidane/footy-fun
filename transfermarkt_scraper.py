import requests
# Remove BeautifulSoup import
# from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime

# Base URL for the API and Headers
API_BASE_URL = "https://transfermarkt-api.fly.dev"
HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36'} # Use a generic user agent

# Update LEAGUES to use API league codes
LEAGUES = {
    "Premier League": "GB1",
    "La Liga": "ES1",
    "Ligue 1": "FR1",
    "Serie A": "IT1",
    "Bundesliga": "L1"
}

# --- Helper Functions ---
def calculate_age(birth_date_str):
    """Calculates age from 'YYYY-MM-DD' string."""
    if not birth_date_str:
        return 'N/A'
    try:
        birth_date = datetime.strptime(birth_date_str, '%Y-%m-%d')
        today = datetime.today()
        age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
        return age
    except (ValueError, TypeError):
        return 'N/A'

def get_market_value_int(player):
    """Safely extracts market value as integer for sorting."""
    try:
        # Based on screenshot, marketValue is a direct integer
        value = player.get('marketValue')
        return int(value) if value is not None else 0
    except (ValueError, TypeError):
        return 0

def format_market_value(value_int):
    """Formats integer market value back to string '€XXX,XXX'."""
    if value_int is None or value_int == 0:
        return "N/A"
    # Assuming Euro as default currency based on Transfermarkt standard
    return f"€{value_int:,}"

# --- Main Data Fetching Function ---
def get_top_league_players_by_value(league_name: str, league_code: str, num_players: int = 100):
    league_all_players = []
    print(f"[{league_name}] Fetching competition details to find clubs...")

    # 1. Get competition details - Trying /competitions/{code}/clubs first
    comp_url = f"{API_BASE_URL}/competitions/{league_code}/clubs"
    clubs = []
    try:
        print(f"  Trying endpoint: {comp_url}")
        comp_response = requests.get(comp_url, headers=HEADERS, timeout=30)
        comp_response.raise_for_status() # Raise error for bad status (like 404)
        comp_data = comp_response.json()

        # *** Adjust based on actual API response structure for clubs ***
        # Possible keys: 'clubs', 'teams', 'results'
        possible_keys = ['clubs', 'teams', 'results'] # Add other potential keys if needed
        found_clubs = False
        if isinstance(comp_data, dict):
            for key in possible_keys:
                if key in comp_data and isinstance(comp_data[key], list):
                    clubs = comp_data[key]
                    print(f"  [{league_name}] Found clubs list under key '{key}'.")
                    found_clubs = True
                    break
        elif isinstance(comp_data, list): # Check if the response itself is the list of clubs
             clubs = comp_data
             print(f"  [{league_name}] Found clubs list directly in response.")
             found_clubs = True

        if not found_clubs:
             print(f"Error: Could not find a list of clubs for {league_name} in response from {comp_url}")
             print(f"  Response sample: {str(comp_data)[:300]}...")
             return [] # Stop if clubs cannot be found

    except requests.exceptions.RequestException as e:
        # If /clubs endpoint fails (e.g., 404), maybe try base /competitions/{code}? Often includes clubs.
        print(f"Warning: Endpoint {comp_url} failed ({e}). Trying base competition URL...")
        comp_url = f"{API_BASE_URL}/competitions/{league_code}"
        try:
            print(f"  Trying fallback endpoint: {comp_url}")
            comp_response = requests.get(comp_url, headers=HEADERS, timeout=30)
            comp_response.raise_for_status()
            comp_data = comp_response.json()
            found_clubs = False
            if isinstance(comp_data, dict):
                for key in possible_keys:
                    if key in comp_data and isinstance(comp_data[key], list):
                        clubs = comp_data[key]
                        print(f"  [{league_name}] Found clubs list under key '{key}' at base URL.")
                        found_clubs = True
                        break
            elif isinstance(comp_data, list):
                 clubs = comp_data
                 print(f"  [{league_name}] Found clubs list directly in fallback response.")
                 found_clubs = True
           
            if not found_clubs:
                 print(f"Error: Could not find clubs list in fallback response for {league_name}.")
                 print(f"  Response sample: {str(comp_data)[:300]}...")
                 return []
                
        except requests.exceptions.RequestException as e2:
             print(f"Error fetching competition/club data for {league_name} from fallback {comp_url}: {e2}")
             return []
        except requests.exceptions.JSONDecodeError:
             print(f"Error decoding competition/club JSON for {league_name} from fallback {comp_url}")
             return []
            
    except requests.exceptions.JSONDecodeError:
        print(f"Error decoding competition/club JSON for {league_name} from initial {comp_url}")
        return []

    print(f"[{league_name}] Found {len(clubs)} clubs. Fetching players for each club (this may take a while)...")

    # 2. Get players for each club
    club_count = 0
    for club in clubs:
        club_count += 1
        club_id = club.get('id')
        club_name = club.get('name', 'Unknown Club')
        if not club_id:
            print(f"Warning: [{league_name}] Skipping club with missing ID: {club}")
            continue

        print(f"  [{league_name}] Fetching players for \"{club_name}\" ({club_count}/{len(clubs)})...", end='')
        players_url = f"{API_BASE_URL}/clubs/{club_id}/players"
        try:
            players_response = requests.get(players_url, headers=HEADERS, timeout=45) # Slightly longer timeout for player lists
            players_response.raise_for_status()
            player_data = players_response.json()

            # Structure based on screenshot: {'players': [...]} or similar
            current_club_players = player_data.get('players', []) # Default assumption
            found_list = False
            if isinstance(current_club_players, list) and current_club_players: # Check if primary key worked
                 found_list = True
            elif isinstance(player_data, list): # Check if root is list
                 current_club_players = player_data
                 found_list = True
            elif isinstance(player_data, dict): # Check other keys if 'players' failed or was empty
                for key, value in player_data.items():
                    if isinstance(value, list):
                       current_club_players = value
                       found_list = True
                       print(f" (using key '{key}')", end='')
                       break
                      
            if not found_list:
                 # Log if no list found, but maybe club has 0 players? Don't treat as error unless list type expected but not found.
                 if 'players' in player_data and not isinstance(player_data['players'], list):
                      print(f"\nWarning: [{league_name}] Expected player list for {club_name}, got {type(player_data.get('players'))}. Skipping.")
                      continue # Skip if structure is wrong
                 else:
                      # Assume 0 players if no list found or primary key was empty list
                      current_club_players = []
                      print(" Found 0 players.")
                      # Continue processing the club, just add no players

            # Add club name and store raw player data
            player_count_for_club = 0
            for player in current_club_players:
                player['clubName'] = club_name # Add team context
                league_all_players.append(player)
                player_count_for_club += 1
           
            if player_count_for_club > 0:
                 print(f" Found {player_count_for_club} players.")

            time.sleep(0.6) # Delay between club requests to be nice to the API

        except requests.exceptions.RequestException as e:
            print(f"\nWarning: [{league_name}] Error fetching players for {club_name} (ID: {club_id}): {e}. Skipping club.")
            time.sleep(1.0) # Longer delay on error
            continue
        except requests.exceptions.JSONDecodeError:
            print(f"\nWarning: [{league_name}] Error decoding player JSON for {club_name} (ID: {club_id}). Skipping club.")
            continue

    if not league_all_players:
        print(f"[{league_name}] No players collected for any club.")
        return []

    print(f"[{league_name}] Collected {len(league_all_players)} total players. Sorting by market value...")

    # 3. Sort all players by market value
    league_all_players.sort(key=get_market_value_int, reverse=True)

    # 4. Get top N players
    top_players_raw = league_all_players[:num_players]

    print(f"[{league_name}] Extracting details for top {len(top_players_raw)} players...")

    # 5. Format final output list
    final_players_data = []
    for player in top_players_raw:
        market_val_int = get_market_value_int(player)
        player_id = player.get('id') # *** CONFIRM THIS KEY ('id', 'playerID', etc.) ***
        if player_id is None:
             print(f"Warning: Skipping player with missing ID in raw data: {player.get('name')}")
             continue # Skip players without an ID
             
        final_players_data.append({
            # *** ADD player_id HERE ***
            'player_id': player_id, 
            'League': league_name,
            'Name': player.get('name', 'N/A'),
            'Position': player.get('position', 'N/A'),
            'Team': player.get('clubName', 'N/A'), 
            'Age': calculate_age(player.get('dateOfBirth')), 
            'Market Value': format_market_value(market_val_int),
            'Market Value Int': market_val_int # Also save the integer value for easier sorting in API
        })

    print(f"[{league_name}] Finished processing.")
    return final_players_data

# --- Main execution block ---
if __name__ == "__main__":
    all_leagues_top_players = []
    num_players_per_league = 100

    start_total_time = time.time()

    for name, code in LEAGUES.items():
        league_start_time = time.time()
        # Call the updated function
        top_players = get_top_league_players_by_value(name, code, num_players_per_league)
        all_leagues_top_players.extend(top_players)
        league_duration = time.time() - league_start_time
        print(f"[{name}] Processing took {league_duration:.2f} seconds.")
        # Add a small delay between leagues if needed, maybe longer if rate limited
        time.sleep(2)

    total_duration = time.time() - start_total_time
    print(f"\nTotal execution time: {total_duration:.2f} seconds.")

    if not all_leagues_top_players:
        print("\nNo players were collected overall. Please check API availability, endpoints, league codes, and club/player data structure in API responses.")
    else:
        # Convert to DataFrame and save
        df = pd.DataFrame(all_leagues_top_players)
        print("\n--- Sample Data (First 10 rows) ---")
        print(df.head(10))

        # Use a new filename to avoid confusion
        output_filename = "top_players_by_market_value_api_v2.csv"
        try:
            df.to_csv(output_filename, index=False, encoding='utf-8')
            print(f"\nSuccessfully saved data for {len(all_leagues_top_players)} players to {output_filename}")
        except Exception as e:
            print(f"\nError saving data to CSV: {e}") 