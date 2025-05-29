import unittest
from unittest.mock import patch, MagicMock
import requests # Import requests for requests.exceptions.RequestException
# Assuming transfermarkt_scraper.py is in the same directory or accessible in PYTHONPATH
from transfermarkt_scraper import get_top_league_players_by_value, LEAGUES

# Minimal player data needed for the function to run without error
MINIMAL_PLAYER_DATA = {
    'id': '1',
    'name': 'Test Player',
    'dateOfBirth': '1990-01-01', # Needed for calculate_age
    'marketValue': 1000000,    # Needed for get_market_value_int
    'position': 'Forward',     # Optional, but good to have
}

class TestPlayerFetching(unittest.TestCase):

    def _setup_mock_get(self, mock_get, club_response_data, player_response_data):
        """Helper to configure mock for requests.get for simple success cases."""
        mock_club_response = MagicMock()
        mock_club_response.json.return_value = club_response_data
        mock_club_response.raise_for_status = MagicMock()
        mock_club_response.status_code = 200 # Good practice to set status for successful mocks

        mock_player_response = MagicMock()
        mock_player_response.json.return_value = player_response_data
        mock_player_response.raise_for_status = MagicMock()
        mock_player_response.status_code = 200 # Good practice to set status for successful mocks
        
        # This simple side_effect list is suitable for tests where each call succeeds on the first try.
        # For tests involving retries or specific URL handling, side_effect will be set directly in the test.
        mock_get.side_effect = [mock_club_response, mock_player_response]

    # Default club data for tests
    default_club_data = {'clubs': [{'id': '123', 'name': 'Test Club'}]}
    # Define default URLs based on common test parameters for clarity in mock setups
    base_api_url = "https://transfermarkt-api.fly.dev" 
    default_clubs_url = f"{base_api_url}/competitions/TL1/clubs" # Assuming 'TL1' is a common test league_code
    default_competition_fallback_url = f"{base_api_url}/competitions/TL1"
    default_players_url = f"{base_api_url}/clubs/123/players" # Assuming club '123' from default_club_data


    @patch('transfermarkt_scraper.requests.get')
    def test_scenario_1_players_under_players_key(self, mock_get):
        player_data = [
            {**MINIMAL_PLAYER_DATA, 'id': '1', 'name': 'Player A'},
            {**MINIMAL_PLAYER_DATA, 'id': '2', 'name': 'Player B'}
        ]
        self._setup_mock_get(mock_get, self.default_club_data, {'players': player_data})

        result = get_top_league_players_by_value('Test League', 'TL1', num_players=2)
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['Name'], 'Player A')
        self.assertEqual(result[1]['Name'], 'Player B')
        mock_get.assert_any_call("https://transfermarkt-api.fly.dev/clubs/123/players", headers=unittest.mock.ANY, timeout=unittest.mock.ANY)

    @patch('transfermarkt_scraper.requests.get')
    def test_scenario_2_zero_players_empty_list(self, mock_get):
        self._setup_mock_get(mock_get, self.default_club_data, {'players': []})

        result = get_top_league_players_by_value('Test League', 'TL1', num_players=10)
        
        self.assertEqual(len(result), 0)
        mock_get.assert_any_call("https://transfermarkt-api.fly.dev/clubs/123/players", headers=unittest.mock.ANY, timeout=unittest.mock.ANY)

    @patch('transfermarkt_scraper.requests.get')
    def test_scenario_3_player_data_is_root_list(self, mock_get):
        player_data = [{**MINIMAL_PLAYER_DATA, 'id': '3', 'name': 'Player C'}]
        self._setup_mock_get(mock_get, self.default_club_data, player_data)

        result = get_top_league_players_by_value('Test League', 'TL1', num_players=1)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['Name'], 'Player C')
        mock_get.assert_any_call("https://transfermarkt-api.fly.dev/clubs/123/players", headers=unittest.mock.ANY, timeout=unittest.mock.ANY)

    @patch('transfermarkt_scraper.requests.get')
    def test_scenario_4_players_under_alternative_key(self, mock_get):
        player_data = [{**MINIMAL_PLAYER_DATA, 'id': '4', 'name': 'Player D'}]
        self._setup_mock_get(mock_get, self.default_club_data, {'data': player_data})

        result = get_top_league_players_by_value('Test League', 'TL1', num_players=1)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['Name'], 'Player D')
        mock_get.assert_any_call("https://transfermarkt-api.fly.dev/clubs/123/players", headers=unittest.mock.ANY, timeout=unittest.mock.ANY)

    @patch('transfermarkt_scraper.requests.get')
    def test_scenario_5a_players_key_is_null(self, mock_get):
        self._setup_mock_get(mock_get, self.default_club_data, {'players': None})

        result = get_top_league_players_by_value('Test League', 'TL1', num_players=10)
        
        self.assertEqual(len(result), 0) # Should be treated as zero players
        mock_get.assert_any_call("https://transfermarkt-api.fly.dev/clubs/123/players", headers=unittest.mock.ANY, timeout=unittest.mock.ANY)

    @patch('transfermarkt_scraper.requests.get')
    def test_scenario_5b_players_key_not_a_list(self, mock_get):
        self._setup_mock_get(mock_get, self.default_club_data, {'players': "This is not a list"})

        result = get_top_league_players_by_value('Test League', 'TL1', num_players=10)
        
        self.assertEqual(len(result), 0) # Should be treated as zero players
        mock_get.assert_any_call("https://transfermarkt-api.fly.dev/clubs/123/players", headers=unittest.mock.ANY, timeout=unittest.mock.ANY)

    @patch('transfermarkt_scraper.requests.get')
    def test_scenario_6a_empty_dictionary_response(self, mock_get):
        self._setup_mock_get(mock_get, self.default_club_data, {})

        result = get_top_league_players_by_value('Test League', 'TL1', num_players=10)
        
        self.assertEqual(len(result), 0) # Should be treated as zero players
        mock_get.assert_any_call("https://transfermarkt-api.fly.dev/clubs/123/players", headers=unittest.mock.ANY, timeout=unittest.mock.ANY)

    @patch('transfermarkt_scraper.requests.get')
    def test_scenario_6b_dictionary_without_list_value(self, mock_get):
        self._setup_mock_get(mock_get, self.default_club_data, {'info': 'some info', 'count': 0, 'message': 'no players today'})

        result = get_top_league_players_by_value('Test League', 'TL1', num_players=10)
        
        self.assertEqual(len(result), 0) # Should be treated as zero players
        mock_get.assert_any_call("https://transfermarkt-api.fly.dev/clubs/123/players", headers=unittest.mock.ANY, timeout=unittest.mock.ANY)

    @patch('transfermarkt_scraper.requests.get')
    def test_player_data_missing_fields_and_zero_value(self, mock_get): # Renamed for clarity
        # Test with data that might cause issues in helper functions if not handled
        player_data = [
            {'id': '1', 'name': 'Player A', 'dateOfBirth': None, 'marketValue': None}, # None values for date and marketValue
            {'id': '2', 'name': 'Player B'}, # Missing keys for date and marketValue
            {'id': '3', 'name': 'Player C', 'dateOfBirth': 'invalid-date', 'marketValue': 'not-a-number'}, # Invalid values
            {'id': '4', 'name': 'Player D', 'dateOfBirth': '2000-01-01', 'marketValue': 0} # Market value of zero
        ]
        # Standard setup for successful club fetch, then player fetch with specific data
        self._setup_mock_get(mock_get, self.default_club_data, {'players': player_data})

        result = get_top_league_players_by_value('Test League', 'TL1', num_players=4)
        
        self.assertEqual(len(result), 4)
        
        # Player D: Valid dateOfBirth, Market value of 0. Should be first due to reverse sort.
        self.assertEqual(result[0]['Name'], 'Player D')
        self.assertIsInstance(result[0]['Age'], int) 
        self.assertEqual(result[0]['Market Value'], '€0') 
        self.assertEqual(result[0]['Market Value Int'], 0)

        # The order of Player A, B, C might vary as they all have 'None' market value (sorted as -inf)
        # For stable sort, their original relative order should be preserved.
        # Player A: None dateOfBirth, None marketValue
        player_a_data = next(p for p in result if p['Name'] == 'Player A')
        self.assertIsNone(player_a_data['Age']) 
        self.assertEqual(player_a_data['Market Value'], 'N/A') 
        self.assertIsNone(player_a_data['Market Value Int'])

        # Player B: Missing dateOfBirth, Missing marketValue
        player_b_data = next(p for p in result if p['Name'] == 'Player B')
        self.assertIsNone(player_b_data['Age']) 
        self.assertEqual(player_b_data['Market Value'], 'N/A') 
        self.assertIsNone(player_b_data['Market Value Int'])

        # Player C: Invalid dateOfBirth, Invalid marketValue
        player_c_data = next(p for p in result if p['Name'] == 'Player C')
        self.assertIsNone(player_c_data['Age']) 
        self.assertEqual(player_c_data['Market Value'], 'N/A') 
        self.assertIsNone(player_c_data['Market Value Int'])

    @patch('transfermarkt_scraper.requests.get')
    def test_club_fetching_fails_gracefully(self, mock_get):
        # Simulate requests.get raising an exception for all club-related calls
        mock_get.side_effect = requests.exceptions.RequestException("API unavailable for clubs")
        
        result = get_top_league_players_by_value('Test League', 'TL1', num_players=10)
        self.assertEqual(len(result), 0)
        
        # MAX_RETRIES is 3 in transfermarkt_scraper.py
        # Expect 3 calls for the primary clubs URL, then 3 calls for the fallback competition URL
        expected_calls = 3 + 3 
        self.assertEqual(mock_get.call_count, expected_calls)
        # Check the URLs were called as expected
        mock_get.assert_any_call(self.default_clubs_url, headers=unittest.mock.ANY, timeout=unittest.mock.ANY)
        mock_get.assert_any_call(self.default_competition_fallback_url, headers=unittest.mock.ANY, timeout=unittest.mock.ANY)


    @patch('transfermarkt_scraper.requests.get')
    def test_player_fetching_request_exception(self, mock_get):
        mock_club_response = MagicMock()
        mock_club_response.json.return_value = self.default_club_data
        mock_club_response.raise_for_status = MagicMock()
        mock_club_response.status_code = 200

        # Custom side_effect function for more control over responses based on URL
        def custom_side_effect(url, headers, timeout):
            if url == self.default_clubs_url:
                return mock_club_response
            elif url == self.default_players_url:
                raise requests.exceptions.RequestException("Player API unavailable")
            # Fallback for any other unexpected calls
            mock_unexpected = MagicMock()
            mock_unexpected.status_code = 404 
            mock_unexpected.raise_for_status.side_effect = requests.exceptions.HTTPError(f"Unexpected URL: {url}")
            return mock_unexpected

        mock_get.side_effect = custom_side_effect

        result = get_top_league_players_by_value('Test League', 'TL1', num_players=10)
        self.assertEqual(len(result), 0)
        
        # MAX_RETRIES is 3 in transfermarkt_scraper.py
        # Expected calls: 1 for clubs (success), MAX_RETRIES for players (all fail)
        expected_calls = 1 + 3
        self.assertEqual(mock_get.call_count, expected_calls)
        mock_get.assert_any_call(self.default_clubs_url, headers=unittest.mock.ANY, timeout=unittest.mock.ANY)
        mock_get.assert_any_call(self.default_players_url, headers=unittest.mock.ANY, timeout=unittest.mock.ANY)


if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)

# Note: LEAGUES is imported but not directly used in these specific unit tests.
# MINIMAL_PLAYER_DATA provides essential fields.
# The `make_request_with_retry` function is part of transfermarkt_scraper and is tested implicitly by these tests.
