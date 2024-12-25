import pandas as pd
import logging
from pathlib import Path
from typing import Optional, List, Union

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AFLStatsProcessor:
    RELEVANT_COLUMNS = [
        "providerId", "utcStartTime", "team.name",
        "home.team.name", "away.team.name",
        "player.givenName", "player.surname",
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

    def __init__(self, input_path: Union[str, Path], output_path: Union[str, Path]):
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.stats_df: Optional[pd.DataFrame] = None

    def load_data(self) -> None:
        """Load and validate data"""
        try:
            logger.info(f"Loading data from {self.input_path}...")
            self.stats_df = pd.read_csv(self.input_path)
            
            # Validate required columns
            missing_cols = set(self.RELEVANT_COLUMNS) - set(self.stats_df.columns)
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            # Handle missing values
            logger.info("Processing missing values...")
            self.handle_missing_values()
            
            # Remove duplicates
            logger.info("Removing duplicates...")
            self.remove_duplicates()
                
        except FileNotFoundError:
            logger.error(f"Input file not found: {self.input_path}")
            raise
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise

    def handle_missing_values(self) -> None:
        """Handle missing values"""
        if self.stats_df is None:
            raise ValueError("Data not loaded")

        # Record initial row count
        initial_rows = len(self.stats_df)

        # Fill numeric columns with 0
        numeric_columns = self.stats_df.select_dtypes(include=['float64', 'int64']).columns
        self.stats_df[numeric_columns] = self.stats_df[numeric_columns].fillna(0)

        # Fill string columns with 'Unknown'
        string_columns = self.stats_df.select_dtypes(include=['object']).columns
        self.stats_df[string_columns] = self.stats_df[string_columns].fillna('Unknown')

        # Remove rows where all key columns are empty
        key_columns = ['providerId', 'player.givenName', 'player.surname']
        self.stats_df.dropna(subset=key_columns, how='all', inplace=True)

        # Log processing results
        rows_removed = initial_rows - len(self.stats_df)
        if rows_removed > 0:
            logger.info(f"Missing value processing complete, removed {rows_removed} rows")

    def remove_duplicates(self) -> None:
        """Remove duplicate records"""
        if self.stats_df is None:
            raise ValueError("Data not loaded")

        # Record initial row count
        initial_rows = len(self.stats_df)

        # Remove duplicates based on key columns
        duplicate_columns = [
            'providerId',
            'player.givenName',
            'player.surname',
            'utcStartTime'
        ]
        
        self.stats_df.drop_duplicates(
            subset=duplicate_columns,
            keep='first',
            inplace=True
        )

        # Log deduplication results
        rows_removed = initial_rows - len(self.stats_df)
        if rows_removed > 0:
            logger.info(f"Deduplication complete, removed {rows_removed} duplicate records")

    def filter_stats_by_game(self, 
                           game_id: Optional[str] = None, 
                           opponent: Optional[str] = None) -> pd.DataFrame:
        """Filter data by game ID and opponent"""
        if self.stats_df is None:
            raise ValueError("Please load data first")

        filtered = self.stats_df[self.RELEVANT_COLUMNS]
        
        if game_id:
            filtered = filtered[filtered["providerId"] == game_id]
            logger.info(f"Filtering by game ID: {game_id}")
            
        if opponent:
            filtered = filtered[
                (filtered["home.team.name"] == opponent) |
                (filtered["away.team.name"] == opponent)
            ]
            logger.info(f"Filtering by opponent team: {opponent}")
            
        return filtered

    def save_filtered_data(self, filtered_data: pd.DataFrame) -> None:
        """Save filtered data"""
        try:
            # Ensure output directory exists
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            
            filtered_data.to_csv(self.output_path, index=False)
            logger.info(f"Data saved to: {self.output_path}")
            
        except Exception as e:
            logger.error(f"Error saving data: {str(e)}")
            raise

def main():
    # File paths
    input_path = Path("../data/raw/afl_player_stats_2023_2024.csv")
    output_path = Path("../data/processed/afl_player_stats_filtered.csv")

    try:
        # Initialize processor
        processor = AFLStatsProcessor(input_path, output_path)
        
        # Load data
        processor.load_data()
        
        # Filter data
        filtered_data = processor.filter_stats_by_game(
            game_id="CD_M20240140002",
            opponent="Brisbane Lions"
        )
        
        # Save results
        processor.save_filtered_data(filtered_data)
        
    except Exception as e:
        logger.error(f"Error during processing: {str(e)}")
        raise

if __name__ == "__main__":
    main()
