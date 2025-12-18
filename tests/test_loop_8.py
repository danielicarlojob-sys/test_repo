import os
import pandas as pd
from src.utils.log_file import debug_info
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', None)     # Show all rows
pd.set_option('display.max_colwidth', None) # Show full column width
pd.set_option('display.width', 0)           # Auto-adjust display width
import numpy as np
import pytest

from src.Loop_8_Summary_Stats import Loop_8_Summary_Stats

class TestLoop8SummaryStats:
    @pytest.fixture(autouse=True)
    def _setup_tmpdir(self, tmp_path, monkeypatch):
        """
        Auto-used fixture for each test:
        - Create Fleetstore_Data/ directory
        - Change working directory to tmp_path
        """
        self.tmpdir = tmp_path
        os.makedirs(self.tmpdir / "Fleetstore_Data", exist_ok=True)
        monkeypatch.chdir(self.tmpdir)

    def test_basic_computation(self):
        """
        Test that summary statistics (mean, max, fractions) are computed correctly
        for a small dataframe with valid IPC/HPC shift data.
        """
        df = pd.DataFrame({
            "operator": ['Op1', 'Op1', 'Op1', 'Op1'],
            "ESN": [1, 1, 1, 1],
            "DSCID":    [52, 52, 52, 52],
            "ACID": ["AC1", "AC1", "AC1", "AC1"],
            "NEW_FLAG": [1, 1, 1, 1],
            "reportdatetime": pd.date_range(start='2023-01-01', end='2023-01-31', periods=4),
            "ENGPOS":   [1, 1, 1, 1],
            'VAR1_IDENTIFIER50': ['IPC ETA', 'IPC ETA', 'IPC ETA', 'IPC ETA'], 
            'VAR2_IDENTIFIER50': ['HPC ETA', 'HPC ETA', 'HPC ETA', 'HPC ETA'],
            'VAR3_IDENTIFIER50': ['XRATE_3', 'XRATE_3', 'XRATE_3', 'XRATE_3'],
            "IPC_DAMAGE_SHIFT50": [0.1, 0.5, 0.6, 0.9],
            "HPC_DAMAGE_SHIFT50": [0.2, 0.4, 0.8, 1.0],
        })

        result = Loop_8_Summary_Stats(df, lag_list=[50], save_csv=True, flight_phase="test")

        # Check that rolling max/mean columns exist
        assert "IPC_MAX50" in result.columns
        assert "IPC_MEAN50" in result.columns
        assert any(col.startswith("IPC_FRACTION_GT") for col in result.columns)

        # Check that the rolling max/mean values are correct
        assert result["IPC_MAX50"].max() == pytest.approx(0.9)
        # Rolling mean for the newest row (descending sorted) equals the mean of all
        assert result["IPC_MEAN50"].iloc[3] == pytest.approx(np.mean([0.1, 0.5, 0.6, 0.9]))

        # Check CSV file was created
        expected_file = self.tmpdir / "Fleetstore_Data" / "LOOP_8_test.csv"
        assert expected_file.exists()

    def test_handles_no_new_data(self):
        """
        Ensure the function does not crash and returns original df when no NEW_FLAG=1 rows are present.
        """
        df = pd.DataFrame({
            "ESN": [1, 1],
            "NEW_FLAG": [0, 0],
            "reportdatetime": pd.date_range("2023-01-01", periods=2),
            "IPC_DAMAGE_SHIFT50": [0.2, 0.4],
            "HPC_DAMAGE_SHIFT50": [0.3, 0.6],
        })

        result = Loop_8_Summary_Stats(df, lag_list=[50], save_csv=True, flight_phase="nonew")

        # Ensure no crash and original data is returned
        assert result.shape[0] == 2
        # Columns should match original dataframe (no rolling stats added)
        assert set(result.columns) == set(df.columns)

    def test_csv_is_saved_to_correct_path(self):
        """
        Verify that the function writes the results CSV file into Fleetstore_Data.
        """
        df = pd.DataFrame({
            "operator": ['Op1', 'Op1'],
            "ESN": [1, 1],
            "ENGPOS":   [1, 1],
            "DSCID":    [52, 52],
            "ACID": ["AC1", "AC1"],
            "NEW_FLAG": [1, 1],
            "reportdatetime": pd.date_range("2023-01-01", periods=2),            
            'VAR1_IDENTIFIER50': ['IPC ETA', 'IPC ETA'], 
            'VAR2_IDENTIFIER50': ['HPC ETA', 'HPC ETA'],
            'VAR3_IDENTIFIER50': ['XRATE_3', 'XRATE_3'],
            "IPC_DAMAGE_SHIFT50": [0.5, 0.6],
            "HPC_DAMAGE_SHIFT50": [0.2, 0.3],

        })

        Loop_8_Summary_Stats(df, lag_list=[50], save_csv=True, flight_phase="csvtest")

        expected_path = self.tmpdir / "Fleetstore_Data" / "LOOP_8_csvtest.csv"
        assert expected_path.is_file()

        saved_df = pd.read_csv(expected_path)
        assert not saved_df.empty

