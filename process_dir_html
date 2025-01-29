#!/bin/bash

# Directories
INPUT_DIR=$1
OUTPUT_DIR=$2

# Check input arguments
if [[ -z "$INPUT_DIR" || -z "$OUTPUT_DIR" ]]; then
  echo "Usage: $0 <input_dir> <output_dir>"
  exit 1
fi

# Create the output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Declare an associative array to track conversation counts
declare -A convo_count

# Process each .htm or .html file in the input directory recursively
find "$INPUT_DIR" -type f \( -iname '*.htm' -o -iname '*.html' \) -print0 | while IFS= read -r -d '' file; do
  echo "==========================================="
  echo "Processing: $file"

  # Extract parent directory as recipient
  parent_dir=$(basename "$(dirname "$file")")
  recipient=${parent_dir:-unknown}

  # Extract date from filename or use default
  filename=$(basename "$file")
  date_match=$(echo "$filename" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}')
  date_match=${date_match:-1970-01-01}

  # Generate unique output file name using recipient and date
  key="${recipient}-${date_match}"
  current_count=${convo_count["$key"]:-0}
  convo_label=$(printf "%02d" "$current_count")
  convo_count["$key"]=$((current_count + 1))

  output_file="$OUTPUT_DIR/${recipient}-${date_match}_${convo_label}.json"

  # Use parse_html.py as a pipe to process the file
  if cat "$file" | ./parse_html.py "$date_match" > "$output_file"; then
    echo " -> Successfully processed: $file"
    echo " -> Output written to: $output_file"
  else
    echo " -> Failed to process: $file"
    rm -f "$output_file"  # Remove incomplete output on failure
  fi

  echo "-------------------------------------------"
done

echo "==========================================="
echo "All .htm and .html files have been processed."
echo "Output files are in '$OUTPUT_DIR'"
echo "==========================================="

