import pandas as pd

# Load the processed AFL data
file_path = "../data/raw/afl_player_stats_2023_2024.csv"
data = pd.read_csv(file_path)

# Combine player name columns for readability
data["player_name"] = data["player.givenName"] + " " + data["player.surname"]

# Show a sample of data
print("Sample data preview:")
print(data[["providerId", "team.name", "player_name", "goals", "disposals"]].head(10))

# Verify specific game and player data
game_id = "CD_M20240140002"  # Replace with a valid providerId
player_name = "Callum Ah Chee"  # Replace with an actual player name

# Filter data
sample_data = data[
    (data["providerId"] == game_id) & (data["player_name"] == player_name)
]

print(f"\nData for {player_name} in game {game_id}:")
print(sample_data)
