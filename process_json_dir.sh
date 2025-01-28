#!/bin/bash

# Syntax usage information
if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <input_directory> <output_directory> [filter_length] [kept_authors] [fields_to_remove]"
    echo "  <input_directory>   Directory containing JSON files to process."
    echo "  <output_directory>  Directory where processed files will be saved."
    exit 1
fi

# Input arguments
INPUT_DIR=$1
OUTPUT_DIR=$2
FILTER_LENGTH=${3:-80}  # Default to 80 if not provided
KEPT_AUTHORS="me" # example: "someusername,1234567,example@example.com"
FIELDS_TO_REMOVE="" # example: "timestamp,convo_id,sequence_id"

# Create the output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR" || { echo "Failed to create output directory: $OUTPUT_DIR"; exit 1; }

# Process each JSON file in the input directory
for json_file in "$INPUT_DIR"/*.json; do
    if [[ ! -f "$json_file" ]]; then
        echo "No JSON files found in the input directory: $INPUT_DIR"
        break
    fi

    # Get the base name of the file
    base_name=$(basename "$json_file")
    output_file="$OUTPUT_DIR/$base_name"

    # Process the file
    #
    # - normalizes names to ascii chars, lower case, whitespace removed
    # - drops conversations consisting of a single message
    # - combine all kept authors into "user"
    # - change newlines to spaces for training continuity
    # - prune out messages < X length
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
        # Optional: Remove partial output if the process fails
        rm -f "$output_file"
    fi
done

# Summary message
echo "Processing complete. Processed files saved to: $OUTPUT_DIR"
