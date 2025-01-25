#!/usr/bin/env python3
import os
import sys
import json
from transformers import GPT2Tokenizer

"""
This script processes a directory of JSON objects and formats them into text suitable
for GPT training. A single string argument specifies the "user" role, while all other
roles are relabeled as "assistant".
"""

# Constants
DEFAULT_MAX_TOKENS = 1024
DEFAULT_OUTPUT_FILE = "gpt_txt.out"
END_OF_TEXT = "<|endoftext|>\n"


def format_conversation(messages, user_role):
    """
    Format a conversation into the GPT training format.
    Any role matching the user_role will be labeled as "user", and all others as "assistant".
    """
    formatted = ""
    for msg in messages:
        role = "user" if msg["author"] == user_role else "assistant"
        formatted += f"{role}: {msg['message']}\n"
    formatted += END_OF_TEXT
    return formatted


def count_tokens(text, tokenizer):
    """
    Count the number of tokens in a text using the GPT tokenizer.
    """
    return len(tokenizer.encode(text))


def split_and_write(messages, outfile, tokenizer, max_tokens, user_role):
    """
    Split a long conversation into smaller chunks, each within the token limit, and write them to the output file.
    """
    chunk = ""
    for message in messages:
        role = "user" if message["author"] == user_role else "assistant"
        temp_chunk = chunk + f"{role}: {message['message']}\n"
        if count_tokens(temp_chunk, tokenizer) > max_tokens:
            chunk += END_OF_TEXT
            outfile.write(chunk)
            chunk = f"{role}: {message['message']}\n"
        else:
            chunk = temp_chunk

    # Write any remaining chunk
    if chunk.strip():
        chunk += END_OF_TEXT
        outfile.write(chunk)


def process_file(filepath, outfile, tokenizer, max_tokens, user_role):
    """
    Process a single JSON file, format conversations, and write to the output file.
    """
    try:
        with open(filepath, "r") as infile:
            data = json.load(infile)

            if isinstance(data, list):
                # Handle case where the JSON is a list of messages
                messages = data
            else:
                raise ValueError(f"Unsupported JSON structure in {filepath}")

            # Format and tokenize
            formatted_conversation = format_conversation(messages, user_role)
            token_count = count_tokens(formatted_conversation, tokenizer)

            if token_count <= max_tokens:
                outfile.write(formatted_conversation)
            else:
                print(
                    f"Warning: File {filepath} exceeds {max_tokens} tokens. Splitting.",
                    file=sys.stderr,
                )
                split_and_write(messages, outfile, tokenizer, max_tokens, user_role)

            print(f"Processed: {filepath}", file=sys.stderr)

    except json.JSONDecodeError:
        print(f"Error: {filepath} is not a valid JSON file.", file=sys.stderr)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)


def process_directory(input_dir, output_file, user_role, max_tokens=DEFAULT_MAX_TOKENS):
    """
    Process all JSON files in the input directory and concatenate them into a single training file.
    """
    if not os.path.isdir(input_dir):
        print(f"Error: {input_dir} is not a valid directory.", file=sys.stderr)
        sys.exit(1)

    tokenizer = GPT2Tokenizer.from_pretrained("gpt2")

    with open(output_file, "w") as outfile:
        for filename in os.listdir(input_dir):
            if filename.endswith(".json"):
                filepath = os.path.join(input_dir, filename)
                process_file(filepath, outfile, tokenizer, max_tokens, user_role)


if __name__ == "__main__":
    # Check for required arguments
    if len(sys.argv) < 3:
        print(
            "Usage: role_json_to_txt.py <input_dir> <user_role> [<output_file>]",
            file=sys.stderr,
        )
        sys.exit(1)

    input_dir = sys.argv[1]
    user_role = sys.argv[2]
    output_file = sys.argv[3] if len(sys.argv) > 3 else DEFAULT_OUTPUT_FILE

    process_directory(input_dir, output_file, user_role)
