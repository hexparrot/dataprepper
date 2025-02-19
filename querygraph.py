#!/usr/bin/env python3
import requests
import json
from datetime import datetime

# GraphQL Server URL
GRAPHQL_ENDPOINT = "http://localhost:8000/graphql"


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


def run_and_sort_query(query: str) -> list:
    """
    Run the provided GraphQL query and return a combined list of results
    sorted by the 'timestamp' field.

    This function searches the returned data for any keys mapping to lists,
    merges them, filters out records missing 'timestamp', and then sorts the list.
    """
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
    # Sort by timestamp, converting to datetime when possible.
    try:
        sorted_results = sorted(
            results, key=lambda x: datetime.fromisoformat(x["timestamp"])
        )
    except Exception:
        sorted_results = sorted(results, key=lambda x: x["timestamp"])
    return json.dumps(sorted_results, indent=2)


def fetch_triplets() -> list:
    """
    Fetch triplets for pokemongo, waze, and lyft using a GraphQL query.

    Returns:
        A list of triplet records sorted by timestamp.
    """
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


def count_triplets() -> list:
    """
    Count triplets for pokemongo, waze, and lyft using a GraphQL query.

    Returns:
        A count of triplet records
    """
    query = """
    query {
      pokemongoTriplets: countTriplets(database: "pokemongo")
      wazeTriplets: countTriplets(database: "waze")
      lyftTriplets: countTriplets(database: "lyft")
    }
    """
    return run_graphql_query(query)


def fetch_mediaplays() -> list:
    """
    Fetch mediaplays from spotify

    Returns:
        A list of mediaplays, timestamped
    """
    query = """
    query {
      spotifyPlays: allMediaPlays(database: "spotify", limit: 100) {
        timestamp
        media
        duration
      }
    }
    """
    query = """
    query {
      netflixPlays: allMediaPlays(database: "netflix", limit: 100) {
        timestamp
        media
        duration
      }
    }
    """
    return run_graphql_query(query)


def count_mediaplays() -> list:
    """
    Count mediaplays among streaming services

    Returns:
        A count of mediaplay records
    """
    query = """
    query {
      spotifyMediaPlays: countMediaPlays
    }
    """
    return run_graphql_query(query)


if __name__ == "__main__":
    # Example usage: fetch triplets and print the total count and JSON output.
    # retval = fetch_triplets()
    # print(f"Total triplets fetched: {len(triplets)}")

    # not fully functional; not counting triplets but the unprocessed document count
    # retval = count_triplets()

    retval = fetch_mediaplays()
    # retval = count_mediaplays()

    print(retval)
