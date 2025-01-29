#!/bin/bash

# Display usage information if the required arguments are not provided
if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <input_directory> <output_directory> [filter_length]"
    echo "  <input_directory>   Directory containing JSON files to process."
    echo "  <output_directory>  Directory where processed files will be saved."
    echo "  [filter_length]     Minimum message length to retain (default: 80)."
    exit 1
fi

# Input arguments
INPUT_DIR="$1"
OUTPUT_DIR="$2"
FILTER_LENGTH="${3:-80}"  # Default to 80 if not provided
KEPT_AUTHORS="me"  # Example: "someusername,1234567,example@example.com"
FIELDS_TO_REMOVE=""  # Example: "timestamp,convo_id,sequence_id"

# Ensure the output directory exists
if ! mkdir -p "$OUTPUT_DIR"; then
    echo "Error: Failed to create output directory: $OUTPUT_DIR"
    exit 1
fi

# Process each JSON file in the input directory
for json_file in "$INPUT_DIR"/*.json; do
    if [[ ! -f "$json_file" ]]; then
        echo "No JSON files found in the input directory: $INPUT_DIR"
        break
    fi

    # Extract the base filename and define the output file path
    base_name="$(basename "$json_file")"
    output_file="$OUTPUT_DIR/$base_name"

    # Process the file
    #
    # - Normalize author names to ASCII, lowercase, and remove whitespace
    # - Drop conversations consisting of a single message
    # - Merge specified authors into "user"
    # - Convert newlines to spaces for continuity
    # - Remove messages shorter than the specified length
    if cat "$json_file" \
        | ./pipes/rewrite_author_norm.py \
        | ./pipes/drop_single_convo.py \
        | ./pipes/rewrite_author_merge.py "$KEPT_AUTHORS" user \
        | ./pipes/rewrite_newlines.py \
        | ./pipes/drop_short_messages.py "$FILTER_LENGTH" \
        > "$output_file"; then
        echo "Processed: $base_name"
    else
        echo "Failed to process: $base_name"
        rm -f "$output_file"  # Remove any partial output if processing fails
    fi

done

# Completion message
echo "Processing complete. Files saved to: $OUTPUT_DIR"

