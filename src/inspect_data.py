import pandas as pd

# Path to the raw AFL player stats file
file_path = "data/raw/afl_player_stats_2023_2024.csv"

# Load the data
stats = pd.read_csv(file_path)

# Basic inspection
print("File Overview:")
print(stats.info())

print("\nFirst 5 Rows of the Data:")
print(stats.head())

print("\nSummary of Missing Values:")
print(stats.isnull().sum())
