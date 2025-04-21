import sqlite3
from flask import Flask, jsonify, request
from flask_cors import CORS # Import CORS

# --- Configuration ---
DATABASE = 'transfermarkt_data.db'

# --- Flask App Setup ---
app = Flask(__name__)
CORS(app) # Enable CORS for all routes by default

# --- Database Helper Function ---
def query_db(query, args=(), one=False):
    """ Queries the database and returns results as a list of dicts. """
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row # Return rows as dict-like objects
    cur = conn.cursor()
    try:
        cur.execute(query, args)
        rv = cur.fetchall()
        conn.close()
        # Convert Row objects to dictionaries
        results = [dict(row) for row in rv]
        return (results[0] if results else None) if one else results
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        conn.close()
        return None # Or raise an exception
    except Exception as e:
        print(f"Error in query_db: {e}")
        conn.close()
        return None

# --- API Endpoints ---
@app.route('/')
def index():
    return "Welcome to the Transfermarkt Data API! Try /api/leagues or /api/players/search"

@app.route('/api/leagues', methods=['GET'])
def get_leagues():
    """ Endpoint to fetch all leagues. """
    print("Received request for /api/leagues")
    leagues = query_db("SELECT competition_id AS league_id, name, country_name AS country FROM leagues ORDER BY name")
    if leagues is not None:
        print(f"Found {len(leagues)} leagues.")
        return jsonify(leagues)
    else:
        print("Failed to fetch leagues from database.")
        return jsonify({"error": "Failed to fetch leagues"}), 500

@app.route('/api/players/<int:player_id>/valuations', methods=['GET'])
def get_player_valuations(player_id):
    """ Endpoint to fetch historical valuations for a specific player. """
    print(f"Received request for valuations for player_id: {player_id}")
    
    # Query to get date and market value, ordered by date
    query = """
        SELECT 
            date, 
            market_value_in_eur
        FROM player_valuations 
        WHERE player_id = ? 
        ORDER BY date ASC;
    """
    
    valuations = query_db(query, args=(player_id,))
    
    if valuations is not None:
        # If query ran but found no valuations for this player, return empty list
        print(f"Found {len(valuations)} valuation records for player_id: {player_id}.")
        return jsonify(valuations) 
    else:
        # This branch is hit if query_db itself returned None (database error)
        print(f"Failed to fetch valuations for player_id: {player_id} from database.")
        return jsonify({"error": "Failed to fetch player valuations"}), 500

@app.route('/api/players/search', methods=['GET'])
def search_players():
    """ Endpoint to search/filter players. """
    # Get query parameters
    league_id_filter = request.args.get('league') # e.g., ?league=GB1
    name_filter = request.args.get('name')       # e.g., ?name=Smith
    limit = request.args.get('limit', default=50, type=int) # Default limit

    print(f"Received player search request: league='{league_id_filter}', name='{name_filter}', limit={limit}")

    # Base query
    query = """
        SELECT
            p.player_id,
            p.name,
            p.position,
            p.sub_position,
            p.date_of_birth, 
            c.name as club_name,
            c.domestic_competition_id as league_id,
            -- Subquery to get the latest market value for each player
            (SELECT pv.market_value_in_eur 
             FROM player_valuations pv 
             WHERE pv.player_id = p.player_id 
             ORDER BY pv.date DESC 
             LIMIT 1) as current_market_value_eur
        FROM players p
        LEFT JOIN clubs c ON p.current_club_id = c.club_id
        WHERE 1=1 
    """ # Start with a condition that's always true
    
    params = []

    # Add filters dynamically
    if league_id_filter:
        query += " AND c.domestic_competition_id = ?"
        params.append(league_id_filter)
    
    if name_filter:
        query += " AND p.name LIKE ?"
        params.append(f"%{name_filter}%") # Use wildcard for partial matches

    # Add ordering and limit
    query += " ORDER BY p.name LIMIT ?"
    params.append(limit)

    print(f"Executing query: {query} with params: {params}")
    players = query_db(query, args=params)

    if players is not None:
        # Could calculate age here if needed, or leave for frontend
        print(f"Found {len(players)} players matching criteria.")
        return jsonify(players)
    else:
        print("Failed to fetch players based on search criteria.")
        return jsonify({"error": "Failed to search players"}), 500

@app.route('/api/players/<int:player_id>', methods=['GET'])
def get_player_details(player_id):
    """ Endpoint to fetch details for a specific player. """
    print(f"Received request for details for player_id: {player_id}")

    # Query to get player details, joining with club for club name
    query = """
        SELECT
            p.player_id,
            p.name,
            p.position,
            p.sub_position,
            p.date_of_birth, 
            p.current_club_id,
            c.name as club_name,
            c.domestic_competition_id as league_id,
             -- Optionally include latest market value here too if needed by the caller
            (SELECT pv.market_value_in_eur 
             FROM player_valuations pv 
             WHERE pv.player_id = p.player_id 
             ORDER BY pv.date DESC 
             LIMIT 1) as current_market_value_eur
        FROM players p
        LEFT JOIN clubs c ON p.current_club_id = c.club_id
        WHERE p.player_id = ?;
    """
    
    player_details = query_db(query, args=(player_id,), one=True) # Use one=True as we expect only one result
    
    if player_details:
        print(f"Found details for player_id: {player_id}")
        return jsonify(player_details)
    else:
        # If query ran but found no player with that ID
        print(f"Player details not found for player_id: {player_id}")
        return jsonify({"error": "Player not found"}), 404

@app.route('/api/players/<int:player_id>/form', methods=['GET'])
def get_player_form(player_id):
    """ Endpoint to fetch recent form data for a player from the database. """
    print(f"Received request for form data for player_id: {player_id}")
    
    # Query the pre-calculated form stats table
    query = """
        SELECT 
            player_id, 
            average_rating_last_10 AS avgRating, 
            goals_last_10 AS goalsLast10, 
            assists_last_10 AS assistsLast10,
            calculation_timestamp
        FROM player_form_stats 
        WHERE player_id = ?;
    """
    
    form_data = query_db(query, args=(player_id,), one=True) # Use one=True
    
    if form_data:
        print(f"Found pre-calculated form data for player_id: {player_id}")
        # Optionally format timestamp if needed, otherwise return as is
        # form_data['calculation_timestamp'] = form_data['calculation_timestamp'].isoformat() if form_data.get('calculation_timestamp') else None
        return jsonify(form_data)
    else:
        # If query ran but found no form data row for this player
        print(f"No pre-calculated form data found for player_id: {player_id}")
        return jsonify({"error": "Form data not available for this player"}), 404

@app.route('/api/players/top', methods=['GET'])
def get_top_players():
    """ Endpoint to fetch top N players by current market value. """
    limit = request.args.get('limit', default=10, type=int)
    print(f"Received request for top {limit} players by market value.")

    # Query to find the latest valuation date for each player
    # Then join back to get player details and the value on that latest date.
    # This uses a subquery to find the max date per player.
    query = """
        SELECT
            p.player_id,
            p.name,
            p.position,
            p.sub_position,
            c.name AS club_name,
            pv.market_value_in_eur AS current_market_value_eur
        FROM players p
        JOIN clubs c ON p.current_club_id = c.club_id
        JOIN player_valuations pv ON p.player_id = pv.player_id
        JOIN (
            SELECT 
                player_id, 
                MAX(date) as max_date 
            FROM player_valuations 
            GROUP BY player_id
        ) latest_val ON pv.player_id = latest_val.player_id AND pv.date = latest_val.max_date
        WHERE pv.market_value_in_eur IS NOT NULL -- Exclude players with no valuation
        ORDER BY pv.market_value_in_eur DESC
        LIMIT ?;
    """

    params = [limit]

    print(f"Executing query: {query} with params: {params}")
    top_players = query_db(query, args=params)

    if top_players is not None:
        print(f"Found {len(top_players)} top players.")
        return jsonify(top_players)
    else:
        print("Failed to fetch top players.")
        return jsonify({"error": "Failed to fetch top players"}), 500

@app.route('/api/test/<int:test_id>', methods=['GET'])
def test_dynamic_route(test_id):
    print(f"!!! TEST ROUTE HIT with ID: {test_id} !!!")
    return jsonify({"message": f"Test successful for ID {test_id}"})

# Add more endpoints here later, e.g.:
# @app.route('/api/players/search')
# def search_players():
#     # Add query parameters for filtering (league, position, age etc.)
#     pass

# --- Run the App ---
if __name__ == '__main__':
    # Runs the Flask development server
    # Debug=True enables auto-reloading and detailed error pages
    # Specify a different port (e.g., 5001)
    app.run(debug=True, port=5001) 