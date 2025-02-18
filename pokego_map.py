import requests
import pandas as pd
import h3
import folium
import branca.colormap as cm
from datetime import datetime
import json

# 🌐 GraphQL Server URL
GRAPHQL_ENDPOINT = "http://localhost:8000/graphql"

# 📡 GraphQL Query to fetch all triplets
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

# Fetch Data
response = requests.post(
    GRAPHQL_ENDPOINT,
    json={"query": query},
    headers={"Content-Type": "application/json"},
)

if response.status_code == 200:
    data = response.json()
    triplets = data["data"]
else:
    print(f"Error: {response.status_code}, {response.text}")
    exit()

# 🕒 Combine all triplets into a single list
all_triplets = []
# Combine triplets from Waze
if "wazeTriplets" in triplets:
    all_triplets.extend(triplets["wazeTriplets"])

# Combine triplets from Lyft
if "lyftTriplets" in triplets:
    all_triplets.extend(triplets["lyftTriplets"])

# Combine triplets from Pokemongo
if "pokemongoTriplets" in triplets:
    all_triplets.extend(triplets["pokemongoTriplets"])

# Purge invalids
all_triplets = [
    i
    for i in all_triplets
    if all([i.get("timestamp"), i.get("latitude"), i.get("longitude")])
]

# Check if we have all triplets in one list
print(f"Total triplets fetched: {len(all_triplets)}")

# 🕒 Convert timestamps & sort
df = pd.DataFrame(all_triplets)

# Check the DataFrame structure to ensure everything is valid
print(f"DataFrame structure: {df.head()}")

# Handle any invalid or missing timestamps
df["timestamp"] = pd.to_datetime(
    df["timestamp"], errors="coerce"
)  # Ensure datetime format
df = df.dropna(subset=["timestamp"])  # Drop rows with invalid timestamps

# Sort by timestamp
df = df.sort_values("timestamp")

# 🎯 H3 Resolution for Clustering
resolution = 6  # Higher = smaller hexes
df["h3_index"] = df.apply(
    lambda row: h3.latlng_to_cell(row["latitude"], row["longitude"], resolution), axis=1
)

# 🔢 Count Occurrences in Each Hex Bin
hex_counts = df["h3_index"].value_counts().to_dict()

# 🌍 Initialize Folium Map
m = folium.Map(location=[df["latitude"].mean(), df["longitude"].mean()], zoom_start=4)

# 🎨 Define Color Scale
colormap = cm.LinearColormap(
    colors=["blue", "green", "yellow", "red"], vmin=0, vmax=max(hex_counts.values())
)

# 🔲 Add Hexagons to Map
for hex_id, count in hex_counts.items():
    hex_boundary = h3.cell_to_boundary(hex_id)  # Get hexagon shape
    hex_boundary = [(lat, lon) for lat, lon in hex_boundary]  # Convert format

    folium.Polygon(
        locations=hex_boundary,
        color=colormap(count),
        fill=True,
        fill_opacity=0.6,
        tooltip=f"Count: {count}",
    ).add_to(m)

# 🌈 Add Color Legend
colormap.caption = "Density of Events"
m.add_child(colormap)

# Set bounds to restrict infinite scrolling
bounds = [
    [df["latitude"].min(), df["longitude"].min()],
    [df["latitude"].max(), df["longitude"].max()],
]
m.fit_bounds(bounds)

# 📌 Save & Show Map
m.save("animated_hexbin_map.html")
print(
    "✅ Interactive map saved as 'animated_hexbin_map.html'. Open in a browser to view!"
)
