import pandas as pd

# File paths
file_path = "../data/raw/afl_player_stats_2023_2024.csv"
output_path = "../data/processed/afl_player_stats_filtered.csv"

# Load the data
stats = pd.read_csv(file_path)

# Select relevant metrics for analysis
relevant_columns = [
    "providerId",
    "utcStartTime",
    "team.name",
    "home.team.name",
    "away.team.name",
    "player.givenName",
    "player.surname",
    "goals",
    "behinds",
    "goalAccuracy",
    "goalAssists",
    "disposals",
    "kicks",
    "handballs",
    "contestedPossessions",
    "uncontestedPossessions",
    "totalPossessions",
    "tackles",
    "intercepts",
    "rebound50s",
    "disposalEfficiency",
    "clangers",
    "metresGained",
    "scoreInvolvements",
    "bounces",
]

# Filter the data
stats_filtered = stats[relevant_columns]


# Function to filter stats by game and opponents
def filter_stats_by_game(data, game_id=None, opponent=None):
    filtered = data
    if game_id:
        filtered = filtered[filtered["providerId"] == game_id]
    if opponent:
        filtered = filtered[
            (filtered["home.team.name"] == opponent)
            | (filtered["away.team.name"] == opponent)
        ]
    return filtered


# Example: Filter for a specific game and opponent
game_id = "CD_M20240140002"  # Example game ID
opponent_team = "Brisbane Lions"  # Example opponent team

filtered_data = filter_stats_by_game(
    stats_filtered, game_id=game_id, opponent=opponent_team
)

# Display the filtered data
print("Filtered Data:")
print(filtered_data.head())

# Save filtered data to file
filtered_data.to_csv(output_path, index=False)
print(f"Filtered data saved to {output_path}")
