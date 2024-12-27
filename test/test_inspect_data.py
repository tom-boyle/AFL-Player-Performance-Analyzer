import pytest
import pandas as pd
import tempfile
from pathlib import Path
import sys
from pathlib import Path

# Add project root directory to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.inspect_data import AFLStatsProcessor

@pytest.fixture
def sample_data():
    """Create test data containing all required columns"""
    return pd.DataFrame({
        'providerId': ['CD_M20240140002', 'CD_M20240140002', 'CD_M20240140003', 'CD_M20240140004', None],
        'utcStartTime': ['2024-01-01', '2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04'],
        'team.name': ['Brisbane Lions', 'Brisbane Lions', 'Carlton', 'Collingwood', None],
        'home.team.name': ['Brisbane Lions', 'Brisbane Lions', 'Carlton', 'Collingwood', 'Essendon'],
        'away.team.name': ['Carlton', 'Carlton', 'Collingwood', 'Essendon', 'Fremantle'],
        'player.givenName': ['John', 'John', 'Mike', 'Tom', None],
        'player.surname': ['Doe', 'Doe', 'Smith', 'Brown', None],
        'goals': [1, 1, 2, None, 4],
        'behinds': [2, 2, None, 4, 5],
        'goalAccuracy': [0.5, 0.5, 0.7, None, 0.8],
        'goalAssists': [1, 1, 2, None, 3],
        'disposals': [10, 10, None, 15, 16],
        'kicks': [5, 5, 6, None, 8],
        'handballs': [5, 5, 6, None, 8],
        'contestedPossessions': [4, 4, 5, None, 7],
        'uncontestedPossessions': [6, 6, 7, None, 9],
        'totalPossessions': [10, 10, 12, None, 16],
        'tackles': [3, 3, 4, None, 5],
        'intercepts': [2, 2, 3, None, 4],
        'rebound50s': [1, 1, 2, None, 3],
        'disposalEfficiency': [75.0, 75.0, 80.0, None, 85.0],
        'clangers': [2, 2, 3, None, 4],
        'metresGained': [300, 300, 350, None, 400],
        'scoreInvolvements': [4, 4, 5, None, 6],
        'bounces': [1, 1, 2, None, 3]
    })

@pytest.fixture
def temp_csv_file(sample_data):
    """Create temporary CSV file"""
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
        sample_data.to_csv(tmp.name, index=False)
        return Path(tmp.name)

class TestAFLStatsProcessor:
    
    def test_load_data(self, temp_csv_file):
        """Test data loading functionality"""
        output_path = Path("test_output.csv")
        processor = AFLStatsProcessor(temp_csv_file, output_path)
        processor.load_data()
        
        assert processor.stats_df is not None
        assert len(processor.stats_df) > 0
        assert all(col in processor.stats_df.columns for col in processor.RELEVANT_COLUMNS)
        
    def test_handle_missing_values(self, temp_csv_file):
        """Test missing value handling functionality"""
        output_path = Path("test_output.csv")
        processor = AFLStatsProcessor(temp_csv_file, output_path)
        processor.load_data()
        
        # Verify if numeric columns are filled with 0
        numeric_cols = ['goals', 'behinds', 'goalAccuracy', 'disposals', 'kicks', 
                       'handballs', 'contestedPossessions', 'uncontestedPossessions', 
                       'totalPossessions', 'tackles', 'intercepts', 'rebound50s', 
                       'disposalEfficiency', 'clangers', 'metresGained', 
                       'scoreInvolvements', 'bounces']
        
        for col in numeric_cols:
            assert processor.stats_df[col].isna().sum() == 0
        
        # Verify if string columns are filled with 'Unknown'
        string_cols = ['team.name', 'player.givenName', 'player.surname']
        for col in string_cols:
            assert processor.stats_df[col].isna().sum() == 0
            assert 'Unknown' in processor.stats_df[col].values
        
    def test_remove_duplicates(self, temp_csv_file):
        """Test deduplication functionality"""
        output_path = Path("test_output.csv")
        processor = AFLStatsProcessor(temp_csv_file, output_path)
        processor.load_data()
        
        # Verify if duplicate rows are removed
        duplicate_count = processor.stats_df.duplicated(
            subset=['providerId', 'player.givenName', 'player.surname', 'utcStartTime']
        ).sum()
        assert duplicate_count == 0
        
    def test_filter_stats_by_game(self, temp_csv_file):
        """Test data filtering functionality"""
        output_path = Path("test_output.csv")
        processor = AFLStatsProcessor(temp_csv_file, output_path)
        processor.load_data()
        
        # Test filtering by game ID
        filtered_data = processor.filter_stats_by_game(game_id="CD_M20240140002")
        assert len(filtered_data) > 0
        assert all(filtered_data['providerId'].astype(str) == 'CD_M20240140002')
        
        # Test filtering by opponent
        filtered_data = processor.filter_stats_by_game(opponent='Carlton')
        assert len(filtered_data) > 0
        assert any((filtered_data['home.team.name'] == 'Carlton') | 
                  (filtered_data['away.team.name'] == 'Carlton'))
    
    def test_save_filtered_data(self, temp_csv_file, tmp_path):
        """Test data saving functionality"""
        output_path = tmp_path / "test_output.csv"
        processor = AFLStatsProcessor(temp_csv_file, output_path)
        processor.load_data()
        
        filtered_data = processor.filter_stats_by_game(game_id='CD_M20240140002')
        processor.save_filtered_data(filtered_data)
        
        # Verify if file is created
        assert output_path.exists()
        
        # Verify if saved data is correct
        saved_data = pd.read_csv(output_path)
        assert len(saved_data) == len(filtered_data)
        assert all(col in saved_data.columns for col in processor.RELEVANT_COLUMNS)
    
    def test_invalid_file_path(self):
        """Test invalid file path handling"""
        with pytest.raises(FileNotFoundError):
            processor = AFLStatsProcessor(
                Path("non_existent_file.csv"), 
                Path("test_output.csv")
            )
            processor.load_data()

def test_cleanup(tmp_path):
    """Clean up temporary files after testing"""
    output_file = tmp_path / "test_output.csv"
    if output_file.exists():
        output_file.unlink()