#!/usr/bin/env python3
import requests
import ijson
import json
import io
from datetime import datetime

# GraphQL Server URL
GRAPHQL_ENDPOINT = "http://localhost:8000/graphql"


def run_graphql_query_stream(query: str, path: str):
    """
    Execute a GraphQL query in streaming mode using requests + ijson.
    Return a list of records found at the given 'path' in the JSON, e.g. 'data.netflixPlays.item'.
    """
    headers = {"Content-Type": "application/json"}
    # Use stream=True to allow streaming
    response = requests.post(
        GRAPHQL_ENDPOINT, json={"query": query}, headers=headers, stream=True
    )
    if response.status_code != 200:
        raise Exception(
            f"GraphQL query failed with status {response.status_code}: {response.text}"
        )

    records = []
    # Use ijson to stream-parse JSON from response.raw
    for item in ijson.items(response.raw, path):
        records.append(item)
    return records


def run_graphql_query(query: str) -> dict:
    """
    Execute a GraphQL query and return the JSON response.

    Raises an exception if the request fails.
    """
    headers = {"Content-Type": "application/json"}
    response = requests.post(GRAPHQL_ENDPOINT, json={"query": query}, headers=headers)
    if response.status_code != 200:
        raise Exception(
            f"GraphQL query failed with status {response.status_code}: {response.text}"
        )
    return response.json()


def run_and_sort_query(query: str) -> str:
    data = run_graphql_query(query)
    results = []
    for key, value in data.get("data", {}).items():
        if isinstance(value, list):
            results.extend(value)
        elif isinstance(value, dict):
            results.append(value)
    # Purge items missing a valid timestamp.
    results = [
        item for item in results if item.get("latitude") and item.get("longitude")
    ]
    # Sort by timestamp.
    try:
        sorted_results = sorted(
            results, key=lambda x: datetime.fromisoformat(x["timestamp"])
        )
    except Exception:
        sorted_results = sorted(results, key=lambda x: x["timestamp"])
    return json.dumps(sorted_results, indent=2)


def fetch_triplets() -> str:
    query = """
    query {
      pokemongoTriplets: allTriplets(database: "pokemongo") {
        timestamp
        latitude
        longitude
      }
      wazeTriplets: allTriplets(database: "waze") {
        timestamp
        latitude
        longitude
      }
      lyftTriplets: allTriplets(database: "lyft") {
        timestamp
        latitude
        longitude
      }
    }
    """
    return run_and_sort_query(query)


def count_triplets() -> dict:
    query = """
    query {
      pokemongoTriplets: countTriplets(database: "pokemongo")
      wazeTriplets: countTriplets(database: "waze")
      lyftTriplets: countTriplets(database: "lyft")
    }
    """
    return run_graphql_query(query)


def fetch_mediaplays_streaming() -> list:
    """
    Fetch netflixPlays from the server in streaming mode using ijson.
    Returns a Python list of items found at data.netflixPlays
    """
    query = """
        query {
          allViewingActivities {
            _id
            Profile_Name
            Start_Time
            Duration
            Title
            Bookmark
          }
        }
    """
    # ijson path to the array is 'data.netflixPlays.item'
    results = run_graphql_query_stream(query, "data.allViewingActivities.item")
    return results


def fetch_mediaplays() -> dict:
    """
    Fetch mediaplays from netflix (non-streaming version) if you prefer.
    Returns the entire JSON dictionary.
    """
    query = """
    """
    return run_graphql_query(query)


if __name__ == "__main__":
    # Example: streaming fetch for Netflix plays
    records = fetch_mediaplays_streaming()
    print(json.dumps(records, indent=2))
