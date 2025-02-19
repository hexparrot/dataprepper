#!/usr/bin/env python3
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import ijson

from datetime import datetime


# Convert duration (HH:MM:SS) to seconds
def convert_duration_to_seconds(duration):
    try:
        h, m, s = map(int, duration.split(":"))
        return h * 3600 + m * 60 + s
    except:
        return np.nan


# Convert bookmark time to seconds
def convert_bookmark_to_seconds(bookmark):
    try:
        h, m, s = map(int, bookmark.split(":"))
        return h * 3600 + m * 60 + s
    except:
        return np.nan


# Read JSON array of objects from stdin with ijson
# and convert them into a pandas DataFrame.
def load_data_stdin():
    records = []
    for item in ijson.items(sys.stdin, "item"):
        records.append(item)
    df = pd.DataFrame(records)
    return df


# Clean and preprocess data
def clean_data(df):
    # Convert Start_Time to datetime
    df["Start_Time"] = pd.to_datetime(df["Start_Time"], errors="coerce")
    # Convert Duration and Bookmark to numeric seconds
    df["Duration_Seconds"] = df["Duration"].apply(convert_duration_to_seconds)
    df["Bookmark_Seconds"] = df["Bookmark"].apply(convert_bookmark_to_seconds)
    # Mark completion if Duration >= Bookmark
    df["Completed"] = df["Duration_Seconds"] >= df["Bookmark_Seconds"]
    # Extract hour and day of week
    df["Hour_Watched"] = df["Start_Time"].dt.hour
    df["Day_Of_Week"] = df["Start_Time"].dt.day_name()
    return df


# Analyze Completion Rates
def plot_completion_rates(df):
    completion_rates = (
        df.groupby("Title")["Completed"].mean().sort_values(ascending=False)
    )

    plt.figure(figsize=(10, 5))
    completion_rates[:15].plot(kind="bar", color="blue")
    plt.title("Top 15 Shows with Highest Completion Rates")
    plt.xlabel("Show Title")
    plt.ylabel("Completion Rate")
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.show()


# Analyze Rewatch Rates
def plot_rewatch_rates(df):
    rewatch_counts = (
        df.groupby("Title")["Profile_Name"].count().sort_values(ascending=False)
    )

    plt.figure(figsize=(10, 5))
    rewatch_counts[:15].plot(kind="bar", color="red")
    plt.title("Top 15 Most Rewatched Shows")
    plt.xlabel("Show Title")
    plt.ylabel("Rewatch Count")
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.show()


# Analyze Viewing Trends by Time of Day
def plot_time_of_day_trends(df):
    plt.figure(figsize=(10, 5))
    sns.histplot(df["Hour_Watched"].dropna(), bins=24, kde=True, color="green")
    plt.title("Viewing Distribution by Hour of the Day")
    plt.xlabel("Hour of the Day")
    plt.ylabel("View Count")
    plt.tight_layout()
    plt.show()


# Analyze Drop-Off Points
def plot_drop_off_points(df):
    plt.figure(figsize=(10, 5))
    sns.histplot(df["Bookmark_Seconds"].dropna(), bins=50, kde=True, color="purple")
    plt.title("Common Drop-Off Points in Viewing")
    plt.xlabel("Time Watched (Seconds)")
    plt.ylabel("Frequency")
    plt.tight_layout()
    plt.show()


# Main function
if __name__ == "__main__":
    df = load_data_stdin()
    df = clean_data(df)
    print(df.head())  # Quick look at the cleaned data

    # Example usage
    plot_completion_rates(df)
    plot_rewatch_rates(df)
    plot_time_of_day_trends(df)
    plot_drop_off_points(df)
