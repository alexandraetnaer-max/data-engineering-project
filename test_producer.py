# =============================================================================
# UNIT TESTS FOR PRODUCER
# Tests the format and structure of sensor data payloads
# Run with: python -m pytest test_producer.py -v
# =============================================================================

import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
import requests


def get_sensor_data():
    """
    Standalone version of get_sensor_data() for testing purposes.
    Fetches current weather data from Open-Meteo API and returns
    a structured payload with health status.
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 52.52,
        "longitude": 13.41,
        "current": [
            "temperature_2m",
            "relative_humidity_2m",
            "wind_speed_10m"
        ],
        "timezone": "Europe/Berlin"
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    current = data["current"]
    return {
        "timestamp": current["time"],
        "temperature": current["temperature_2m"],
        "humidity": current["relative_humidity_2m"],
        "wind_speed": current["wind_speed_10m"],
        "location": "Berlin",
        "status": "ok",
        "fetched_at": datetime.now(datetime.UTC).isoformat()
    }


class TestGetSensorData(unittest.TestCase):
    """Tests for the get_sensor_data() function"""

    # Mock API response to avoid real HTTP calls during testing
    MOCK_API_RESPONSE = {
        "current": {
            "time": "2026-06-09T15:00",
            "temperature_2m": 19.5,
            "relative_humidity_2m": 48,
            "wind_speed_10m": 20.2
        }
    }

    @patch('requests.get')
    def test_payload_has_required_fields(self, mock_get):
        """Test that payload contains all required fields"""
        # Arrange: mock the API response
        mock_response = MagicMock()
        mock_response.json.return_value = self.MOCK_API_RESPONSE
        mock_get.return_value = mock_response

        # Act: call the function
        result = get_sensor_data()

        # Assert: check all required fields are present
        required_fields = ['timestamp', 'temperature', 'humidity',
                          'wind_speed', 'location', 'status', 'fetched_at']
        for field in required_fields:
            self.assertIn(field, result,
                         f"Missing required field: {field}")

    @patch('requests.get')
    def test_payload_values_correct_types(self, mock_get):
        """Test that payload values have correct data types"""
        # Arrange
        mock_response = MagicMock()
        mock_response.json.return_value = self.MOCK_API_RESPONSE
        mock_get.return_value = mock_response

        # Act
        result = get_sensor_data()

        # Assert: check data types
        self.assertIsInstance(result['temperature'], float,
                             "Temperature should be a float")
        self.assertIsInstance(result['humidity'], (int, float),
                             "Humidity should be a number")
        self.assertIsInstance(result['wind_speed'], float,
                             "Wind speed should be a float")
        self.assertIsInstance(result['timestamp'], str,
                             "Timestamp should be a string")

    @patch('requests.get')
    def test_payload_status_is_ok(self, mock_get):
        """Test that successful response has status ok"""
        # Arrange
        mock_response = MagicMock()
        mock_response.json.return_value = self.MOCK_API_RESPONSE
        mock_get.return_value = mock_response

        # Act
        result = get_sensor_data()

        # Assert
        self.assertEqual(result['status'], 'ok',
                        "Status should be ok for successful API call")

    @patch('requests.get')
    def test_payload_location_is_berlin(self, mock_get):
        """Test that location is set to Berlin"""
        # Arrange
        mock_response = MagicMock()
        mock_response.json.return_value = self.MOCK_API_RESPONSE
        mock_get.return_value = mock_response

        # Act
        result = get_sensor_data()

        # Assert
        self.assertEqual(result['location'], 'Berlin',
                        "Location should be Berlin")

    @patch('requests.get')
    def test_fetched_at_is_valid_timestamp(self, mock_get):
        """Test that fetched_at is a valid ISO format timestamp"""
        # Arrange
        mock_response = MagicMock()
        mock_response.json.return_value = self.MOCK_API_RESPONSE
        mock_get.return_value = mock_response

        # Act
        result = get_sensor_data()

        # Assert: fetched_at should be parseable as datetime
        try:
            datetime.fromisoformat(result['fetched_at'])
            valid = True
        except ValueError:
            valid = False
        self.assertTrue(valid,
                       "fetched_at should be valid ISO format timestamp")


if __name__ == '__main__':
    unittest.main()