#!/usr/bin/env python3
import os
import tarfile
import zipfile
from collections import deque


def remove_empty_dirs(directory):
    """Recursively remove empty directories."""
    for root, dirs, files in os.walk(directory, topdown=False):
        for d in dirs:
            dir_path = os.path.join(root, d)
            if not os.listdir(dir_path):  # Directory is empty
                os.rmdir(dir_path)


def extract_nested_archives(directory, processed_files):
    """Find and extract nested archives, remove them after successful extraction, and avoid infinite loops."""
    queue = deque([directory])

    while queue:
        current_dir = queue.popleft()
        for root, _, files in os.walk(current_dir):
            for file in files:
                file_path = os.path.join(root, file)
                if file_path in processed_files:
                    continue  # Skip already processed files

                try:
                    if file.endswith((".tar.gz", ".tgz")):
                        with tarfile.open(file_path, "r:gz") as tar:
                            tar.extractall(path=root)
                        processed_files.add(file_path)
                        queue.append(root)
                        os.remove(file_path)  # Remove after successful extraction
                    elif file.endswith(".tar.xz"):
                        with tarfile.open(file_path, "r:xz") as tar:
                            tar.extractall(path=root)
                        processed_files.add(file_path)
                        queue.append(root)
                        os.remove(file_path)  # Remove after successful extraction
                    elif file.endswith(".zip"):
                        with zipfile.ZipFile(file_path, "r") as zip_ref:
                            if zip_ref.testzip() is None:  # Check for file corruption
                                zip_ref.extractall(root)
                                processed_files.add(file_path)
                                queue.append(root)
                                os.remove(
                                    file_path
                                )  # Remove after successful extraction
                            else:
                                print(f"Skipping corrupted ZIP file: {file_path}")
                                os.rename(file_path, file_path + ".bad")
                except (tarfile.TarError, zipfile.BadZipFile, OSError) as e:
                    print(f"Error extracting {file_path}: {e}")
                    os.rename(file_path, file_path + ".bad")


def extract_compressed_files(compressed_root, raw_root):
    """Extracts compressed files from compressed directories to raw directories without creating extra subdirectories."""
    categories = os.listdir(compressed_root)
    processed_files = set()

    for category in categories:
        compressed_dir = os.path.join(compressed_root, category)
        raw_dir = os.path.join(raw_root, category)

        if not os.path.exists(compressed_dir) or not os.path.isdir(compressed_dir):
            continue

        if not os.path.exists(raw_dir):
            os.makedirs(raw_dir)

        for item in os.listdir(compressed_dir):
            item_path = os.path.join(compressed_dir, item)

            if item_path in processed_files:
                continue  # Skip already processed files

            try:
                if os.path.isfile(item_path):
                    if item.endswith((".tar.gz", ".tgz")):
                        with tarfile.open(item_path, "r:gz") as tar:
                            tar.extractall(path=raw_dir)
                        processed_files.add(item_path)
                    elif item.endswith(".tar.xz"):
                        with tarfile.open(item_path, "r:xz") as tar:
                            tar.extractall(path=raw_dir)
                        processed_files.add(item_path)
                    elif item.endswith(".zip"):
                        with zipfile.ZipFile(item_path, "r") as zip_ref:
                            if zip_ref.testzip() is None:  # Check for file corruption
                                zip_ref.extractall(raw_dir)
                                processed_files.add(item_path)
                            else:
                                print(f"Skipping corrupted ZIP file: {item_path}")
                                os.rename(item_path, item_path + ".bad")
            except (tarfile.TarError, zipfile.BadZipFile, OSError) as e:
                print(f"Error extracting {item_path}: {e}")
                os.rename(item_path, item_path + ".bad")

        extract_nested_archives(raw_dir, processed_files)
        remove_empty_dirs(raw_dir)


if __name__ == "__main__":
    compressed_root = "userdata/compressed"
    raw_root = "userdata/raw"
    extract_compressed_files(compressed_root, raw_root)
