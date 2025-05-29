import requests
# Remove BeautifulSoup import
# from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime

# --- Global Constants ---
API_BASE_URL = "https://transfermarkt-api.fly.dev"
USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36'
HEADERS = {'User-Agent': USER_AGENT}

CLUB_REQUEST_DELAY = 0.6  # Seconds to wait between requests for players of different clubs
LEAGUE_REQUEST_DELAY = 2.0  # Seconds to wait between processing different leagues
REQUEST_TIMEOUT_CLUBS = 30  # Seconds before timeout for club-related requests
REQUEST_TIMEOUT_PLAYERS = 45 # Seconds before timeout for player-list requests

MAX_RETRIES = 3
RETRY_DELAY_BASE = 1 # Base seconds for retry backoff (multiplied by attempt number)


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
    """Calculates age from 'YYYY-MM-DD' string. Returns None if invalid."""
    if not birth_date_str:
        return None
    try:
        birth_date = datetime.strptime(birth_date_str, '%Y-%m-%d')
        today = datetime.today()
        age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
        return age
    except (ValueError, TypeError):
        return None

def get_market_value_int(player):
    """Safely extracts market value as integer for sorting. Returns None if missing/invalid."""
    try:
        value = player.get('marketValue')
        if value is None:
            return None
        return int(value)
    except (ValueError, TypeError):
        return None

def format_market_value(value_int):
    """Formats integer market value (or 0) to string '€XXX,XXX'. Returns 'N/A' if value_int is None."""
    if value_int is None:
        return "N/A"
    # Assuming Euro as default currency based on Transfermarkt standard
    return f"€{value_int:,}"

# --- Robust Request Function ---
def make_request_with_retry(url: str, headers: dict, timeout: int, retries: int = MAX_RETRIES, delay_base: int = RETRY_DELAY_BASE):
    """Makes a GET request with a retry mechanism and exponential backoff."""
    last_exception = None
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            return response
        except requests.exceptions.Timeout as e:
            last_exception = e
            print(f"Timeout on attempt {attempt + 1}/{retries} for {url}. Retrying in {delay_base * (attempt + 1)}s...")
            time.sleep(delay_base * (attempt + 1))
        except requests.exceptions.RequestException as e: # Catches ConnectionError, HTTPError, etc.
            last_exception = e
            print(f"Request failed on attempt {attempt + 1}/{retries} for {url}: {e}. Retrying in {delay_base * (attempt + 1)}s...")
            time.sleep(delay_base * (attempt + 1))
        
        if attempt == retries - 1: # Last attempt failed
            print(f"All {retries} retries failed for {url}.")
            if last_exception:
                raise last_exception # Re-raise the last caught exception
            else:
                # This case should ideally not be reached if retries > 0,
                # as an exception should have been caught.
                # For safety, raise a generic error.
                raise requests.exceptions.RequestException(f"All {retries} retries failed for {url} without a specific exception.")
    return None # Should only be reached if retries = 0


# --- Helper for Club Extraction ---
def _extract_clubs_from_response(response_data: dict | list, league_name: str, url_attempted: str):
    """
    Extracts a list of clubs from API response data.
    Searches for clubs under common keys or if the response itself is a list.
    Returns list of clubs or None if not found.
    """
    possible_keys = ['clubs', 'teams', 'results'] # Common keys for lists of clubs
    extracted_clubs = None

    if isinstance(response_data, dict):
        for key in possible_keys:
            if key in response_data and isinstance(response_data[key], list):
                extracted_clubs = response_data[key]
                print(f"  [{league_name}] Found clubs list under key '{key}' from {url_attempted}.")
                break
    elif isinstance(response_data, list):
        extracted_clubs = response_data
        print(f"  [{league_name}] Found clubs list directly in response from {url_attempted}.")
    
    if not extracted_clubs: # Covers case where extracted_clubs is None or an empty list from a valid key
        # If it's an empty list, that's valid (0 clubs), so we don't print an error here.
        # This function's role is to find the list; caller decides if empty list is an issue.
        # Only log error if NO list structure was found at all.
        if extracted_clubs is None: # Explicitly check for None if no structure matched
             print(f"Error: Could not find a list of clubs for {league_name} in response from {url_attempted}.")
             print(f"  Response sample: {str(response_data)[:300]}...")
        # If extracted_clubs is an empty list [], it means a valid structure was found (e.g. {'clubs': []})
        # and it correctly represents zero clubs. This is not an error for _extract_clubs_from_response.
    
    return extracted_clubs # Returns list of clubs (can be empty) or None if structure not found


# --- Main Data Fetching Function ---
def get_top_league_players_by_value(league_name: str, league_code: str, num_players: int = 100):
    league_all_players = []
    print(f"[{league_name}] Fetching competition details to find clubs...")

    clubs = None
    primary_club_url = f"{API_BASE_URL}/competitions/{league_code}/clubs"
    primary_response_obj = None # To store response object for logging JSON content on decode error

    # 1. Try primary endpoint: /competitions/{league_code}/clubs
    print(f"  [{league_name}] Trying primary endpoint: {primary_club_url}")
    try:
        primary_response_obj = make_request_with_retry(primary_club_url, headers=HEADERS, timeout=REQUEST_TIMEOUT_CLUBS)
        comp_data = primary_response_obj.json()
        clubs = _extract_clubs_from_response(comp_data, league_name, primary_club_url)
    except requests.exceptions.RequestException as e:
        print(f"Warning: [{league_name}] Primary endpoint {primary_club_url} failed: {e}")
    except requests.exceptions.JSONDecodeError as e_json:
        print(f"Error: [{league_name}] Failed to decode JSON from {primary_club_url}: {e_json}")
        if primary_response_obj:
            print(f"  Response text sample: {primary_response_obj.text[:200]}")

    # 2. If clubs not found or list is empty, try fallback endpoint: /competitions/{league_code}
    # An empty list from _extract_clubs_from_response is valid (0 clubs from a source),
    # but we might still want to try the fallback if the primary yields 0 clubs,
    # depending on API behavior (e.g., if one endpoint is more comprehensive).
    # For now, let's try fallback if primary explicitly failed to find a *structure* (clubs is None)
    # OR if the primary found an empty list (clubs is []).
    # The problem implies fallback is for when the *first attempt fails*, which could mean request failure OR no clubs.
    if not clubs: # This will be true if clubs is None or an empty list
        if clubs is None: # Primary attempt failed to find a club list structure
             print(f"  [{league_name}] Primary endpoint did not yield a recognizable club list structure. Trying fallback.")
        else: # Primary attempt yielded an empty list of clubs
             print(f"  [{league_name}] Primary endpoint yielded 0 clubs. Trying fallback for potentially more data.")

        fallback_club_url = f"{API_BASE_URL}/competitions/{league_code}"
        fallback_response_obj = None
        print(f"  [{league_name}] Trying fallback endpoint: {fallback_club_url}")
        try:
            fallback_response_obj = make_request_with_retry(fallback_club_url, headers=HEADERS, timeout=REQUEST_TIMEOUT_CLUBS)
            comp_data_fallback = fallback_response_obj.json()
            clubs = _extract_clubs_from_response(comp_data_fallback, league_name, fallback_club_url)
        except requests.exceptions.RequestException as e_fallback:
            print(f"Warning: [{league_name}] Fallback endpoint {fallback_club_url} failed: {e_fallback}")
        except requests.exceptions.JSONDecodeError as e_json_fallback:
            print(f"Error: [{league_name}] Failed to decode JSON from {fallback_club_url}: {e_json_fallback}")
            if fallback_response_obj:
                print(f"  Response text sample: {fallback_response_obj.text[:200]}")

    # 3. Final check and processing of clubs
    if not clubs: # If still no clubs (None or empty list) after primary and fallback
        print(f"Error: [{league_name}] Unable to fetch club list after all attempts. Skipping league.")
        return []

    print(f"[{league_name}] Found {len(clubs)} clubs. Fetching players for each club (this may take a while)...")

    # 2. Get players for each club (This part remains unchanged from before)
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
        players_response = None
        try:
            players_response = make_request_with_retry(players_url, headers=HEADERS, timeout=REQUEST_TIMEOUT_PLAYERS)
            # If make_request_with_retry re-raises, this part won't be reached on failure.
            player_data = players_response.json()

            # Structure based on screenshot: {'players': [...]} or similar
            current_club_players = None
            players_source_key = None # To log where players were found

            # 1. Check if player_data is a dictionary and if player_data.get('players') is a list.
            if isinstance(player_data, dict) and 'players' in player_data:
                if isinstance(player_data['players'], list):
                    current_club_players = player_data['players']
                    players_source_key = "'players'"
                # If 'players' key exists but is not a list, it's an unexpected structure for this primary key.
                # We will then fall through to check other keys or if player_data itself is a list.
            
            # 2. Else, if player_data itself is a list, use it.
            if current_club_players is None and isinstance(player_data, list):
                current_club_players = player_data
                players_source_key = "root list"

            # 3. Else, if player_data is a dictionary and players not found yet, iterate through its values.
            if current_club_players is None and isinstance(player_data, dict):
                for key, value in player_data.items():
                    if key == 'players': # Already checked above, skip
                        continue
                    if isinstance(value, list):
                        current_club_players = value
                        players_source_key = f"'{key}'"
                        print(f" (using key {players_source_key})", end='')
                        break
            
            # 4. If none of the above, then it means no players were found or the structure is unexpected.
            if current_club_players is None:
                # This means no list of players was found anywhere.
                # Could be an error in API response, or genuinely 0 players and API sends e.g. {} or {'players': null}
                print(f"\nWarning: [{league_name}] Could not find a list of players for {club_name} (ID: {club_id}). Response sample: {str(player_data)[:100]}...")
                current_club_players = [] # Assume 0 players and continue, or one could choose to skip the club.
            
            if players_source_key and current_club_players is not None:
                 # Only print "Found X players" if we successfully identified a player list.
                 # If current_club_players is an empty list [], len() is 0.
                 print(f" Found {len(current_club_players)} players" + (f" in {players_source_key}." if players_source_key != "'players'" else "."))


            # Add club name and store raw player data
            player_count_for_club = 0
            if current_club_players: # Ensure it's not None and is a list (even if empty)
                for player in current_club_players:
                    if isinstance(player, dict): # Make sure player entries are dicts
                        player['clubName'] = club_name # Add team context
                        league_all_players.append(player)
                        player_count_for_club += 1
                    else:
                        print(f"\nWarning: [{league_name}] Unexpected item in player list for {club_name}: {str(player)[:100]}. Skipping item.")
            
            # No need to print "Found X players" again here, it's covered above or implicitly by lack of players.
            # if player_count_for_club > 0:
            #      print(f" Found {player_count_for_club} players.")


            time.sleep(CLUB_REQUEST_DELAY) # Delay between club requests

        except requests.exceptions.RequestException as e: # Catches re-raised from make_request_with_retry
            print(f"\nWarning: [{league_name}] Error fetching players for {club_name} (ID: {club_id}): {e}. Skipping club.")
            # time.sleep(1.0) # Longer delay on error - retry logic handles delays
            continue
        except requests.exceptions.JSONDecodeError as e_json_players:
            print(f"\nWarning: [{league_name}] Error decoding player JSON for {club_name} (ID: {club_id}): {e_json_players}")
            if players_response:
                 print(f"  Player Response text sample: {players_response.text[:200]}")
            continue

    if not league_all_players:
        print(f"[{league_name}] No players collected for any club.")
        return []

    print(f"[{league_name}] Collected {len(league_all_players)} total players. Sorting by market value...")

    # 3. Sort all players by market value
    # Handle None values in sort key: treat None as negative infinity for descending sort (None will be last)
    league_all_players.sort(key=lambda p: get_market_value_int(p) if get_market_value_int(p) is not None else float('-inf'), reverse=True)

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
        # Add a small delay between leagues if needed
        time.sleep(LEAGUE_REQUEST_DELAY)

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