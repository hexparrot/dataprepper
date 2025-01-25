#!/bin/bash

# Directories
INPUT_DIR=$1
OUTPUT_DIR=$2
FILTER_LENGTH=60
KEPT_AUTHORS=hexparrot
FIELDS_TO_REMOVE=timestamp,convo_id,sequence_id

# Create the output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Process each JSON file in the input directory
for json_file in "$INPUT_DIR"/*.json; do
    # Get the base name of the file
    base_name=$(basename "$json_file")
    output_file="$OUTPUT_DIR/$base_name"

        #| ./pipes/rewrite_user_assistant.py "$KEPT_AUTHORS" \
    # Process the file
    if cat "$json_file" \
        | ./pipes/rewrite_author_norm.py \
        | ./pipes/rewrite_omit_fields.py "$FIELDS_TO_REMOVE" \
        | ./pipes/drop_short_messages.py "$FILTER_LENGTH" \
        | ./pipes/rewrite_author_merge.py "$KEPT_AUTHORS" user \
        | ./pipes/rewrite_newlines.py \
        > "$output_file"; then
        echo "Processed: $base_name"
    else
        echo "Failed to process: $base_name"
        # Optional: Remove partial output if the process fails
        rm -f "$output_file"
    fi
done
