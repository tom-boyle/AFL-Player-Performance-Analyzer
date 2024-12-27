from pathlib import Path
import pandas as pd

# Load the processed AFL data
file_path = Path("../data/processed/afl_player_stats_filtered.csv")
data = pd.read_csv(file_path, encoding='utf-8')

# Combine player names for readability
data["player_name"] = data["player.givenName"] + " " + data["player.surname"]

# Top players by goals, disposals, and metres gained
top_goals = (
    data.groupby("player_name")["goals"].sum().sort_values(ascending=False).head(10)
)
top_disposals = (
    data.groupby("player_name")["disposals"].sum().sort_values(ascending=False).head(10)
)
top_metres = (
    data.groupby("player_name")["metresGained"]
    .sum()
    .sort_values(ascending=False)
    .head(10)
)

# Display summaries
print("Top 10 Players - Goals:\n", top_goals)
print("\nTop 10 Players - Disposals:\n", top_disposals)
print("\nTop 10 Players - Metres Gained:\n", top_metres)

# Save summaries to a CSV file
output_path = "../data/summary/core_metrics_summary.csv"
top_metrics = pd.DataFrame(
    {
        "Top Goals": top_goals,
        "Top Disposals": top_disposals,
        "Top Metres Gained": top_metres,
    }
).fillna(0)

top_metrics.to_csv(output_path)
print(f"\nSummary saved to {output_path}")
