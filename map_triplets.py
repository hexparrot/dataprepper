import subprocess
import json
import pandas as pd
import h3
import folium
import branca.colormap as cm

# Execute "./querygraph.py" and load its JSON output
try:
    output = subprocess.check_output(["./querygraph.py"], text=True)
    data = json.loads(output)
except Exception as e:
    print(f"Error executing querygraph.py: {e}")
    exit(1)

# Assume the data is a single list of sibling records.
# If the JSON is wrapped in a {"data": ...} object, extract that.
if isinstance(data, dict) and "data" in data:
    all_triplets = data["data"]
else:
    all_triplets = data

print(f"Total triplets fetched: {len(all_triplets)}")

# Convert the list to a DataFrame
df = pd.DataFrame(all_triplets)
print(f"DataFrame structure:\n{df.head()}")

# Calculate H3 index for each record at the chosen resolution
resolution = 6  # Higher resolution produces smaller hexagons
df["h3_index"] = df.apply(
    lambda row: h3.latlng_to_cell(row["latitude"], row["longitude"], resolution), axis=1
)

# Count occurrences in each hexagon
hex_counts = df["h3_index"].value_counts().to_dict()

# Initialize Folium map centered on the mean latitude and longitude
m = folium.Map(location=[df["latitude"].mean(), df["longitude"].mean()], zoom_start=4)

# Define a color scale for the hexagon density
colormap = cm.LinearColormap(
    colors=["blue", "green", "yellow", "red"], vmin=0, vmax=max(hex_counts.values())
)

# Add hexagons to the map
for hex_id, count in hex_counts.items():
    # Get the hexagon's boundary coordinates
    hex_boundary = h3.cell_to_boundary(hex_id)
    hex_boundary = [
        (lat, lon) for lat, lon in hex_boundary
    ]  # Convert to list of (lat, lon) tuples

    folium.Polygon(
        locations=hex_boundary,
        color=colormap(count),
        fill=True,
        fill_opacity=0.6,
        tooltip=f"Count: {count}",
    ).add_to(m)

# Add the color legend to the map
colormap.caption = "Density of Events"
m.add_child(colormap)

# Fit the map bounds to the data
bounds = [
    [df["latitude"].min(), df["longitude"].min()],
    [df["latitude"].max(), df["longitude"].max()],
]
m.fit_bounds(bounds)

# Save the map as an HTML file
m.save("animated_hexbin_map.html")
print(
    "✅ Interactive map saved as 'animated_hexbin_map.html'. Open in a browser to view!"
)
