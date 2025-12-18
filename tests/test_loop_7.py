import pandas as pd
from src.utils.log_file import debug_info
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', None)     # Show all rows
pd.set_option('display.max_colwidth', None) # Show full column width
pd.set_option('display.width', 0)           # Auto-adjust display width

# import pytest
from src.Loop_7_IPC_HPC_PerfShift import Loop_7_IPC_HPC_PerfShift

# ------------------------------
# Happy Path Tests
# ------------------------------


class TestLoop7HappyPath:
    def test_basic_assignment_single_lag(self):
        df = pd.DataFrame({
            'NEW_FLAG': [1, 1, 0],
            "reportdatetime": pd.date_range(start='2025-01-01', end='2025-01-31', periods=3),

            'VAR1_IDENTIFIER50': ["IPC ETA", "HPC ETA", "IPC ETA"],
            'VAR1_SHIFT50': [10, 20, 30],
            'VAR2_IDENTIFIER50': [0, 0, 0],
            'VAR2_SHIFT50': [0, 0, 0],
            'VAR3_IDENTIFIER50': [0, 0, 0],
            'VAR3_SHIFT50': [0, 0, 0],
        })

        result = Loop_7_IPC_HPC_PerfShift(df, lag_list=[50], save_csv=False)

        # Columns should exist
        assert 'IPC_DAMAGE_SHIFT50' in result.columns
        assert 'HPC_DAMAGE_SHIFT50' in result.columns

        # IPC assigned correctly ETA VALUES NEGATED
        assert result.loc[0, 'IPC_DAMAGE_SHIFT50'] == -10
        # HPC assigned correctly ETA VALUES NEGATED
        assert result.loc[1, 'HPC_DAMAGE_SHIFT50'] == -20
        # Old row untouched
        assert pd.isna(result.loc[2, 'IPC_DAMAGE_SHIFT50'])
        assert pd.isna(result.loc[2, 'HPC_DAMAGE_SHIFT50'])

    def test_basic_assignment_multiple_lags(self):
        df = pd.DataFrame({
            'NEW_FLAG': [1, 1],
            "reportdatetime": pd.date_range(start='2025-01-31', end='2025-01-01', periods=2),
            
            'VAR1_IDENTIFIER50': ["IPC ETA", "HPC ETA"],
            'VAR1_SHIFT50': [5, 6],
            'VAR1_IDENTIFIER100': ["HPC ETA","IPC ETA"],
            'VAR1_SHIFT100': [15, 16],
            'VAR2_IDENTIFIER50': ["XRATE_0", "XRATE_0"],
            'VAR2_SHIFT50': [0, 0],
            'VAR2_IDENTIFIER100': ["XRATE_0", "XRATE_0"],
            'VAR2_SHIFT100': [0, 0],
            'VAR3_IDENTIFIER50': ["XRATE_0", "XRATE_0"],
            'VAR3_SHIFT50': [0, 0],
            'VAR3_IDENTIFIER100': ["XRATE_0", "XRATE_0"],
            'VAR3_SHIFT100': [0, 0],
        })

        result = Loop_7_IPC_HPC_PerfShift(df, lag_list=[50, 100], save_csv=False)
        print(f"{debug_info()} -->result:\n {result}")
        # Check both lag columns exist
        for lag in [50, 100]:
            assert f'IPC_DAMAGE_SHIFT{lag}' in result.columns
            assert f'HPC_DAMAGE_SHIFT{lag}' in result.columns

        # Verify correct assignment for lag 50  ETA VALUES NEGATED
        assert result.loc[1, 'IPC_DAMAGE_SHIFT50'] == -5
        assert result.loc[0, 'HPC_DAMAGE_SHIFT50'] == -6

        # Verify correct assignment for lag 100 ETA VALUES NEGATED
        assert result.loc[1, 'HPC_DAMAGE_SHIFT100'] == -15
        assert result.loc[0, 'IPC_DAMAGE_SHIFT100'] == -16

# ------------------------------
# Edge Case Tests
# ------------------------------


class TestLoop7EdgeCases:
    def test_no_new_rows(self):
        df = pd.DataFrame({
            'NEW_FLAG': [0, 0],
            "reportdatetime": pd.date_range(start='2025-01-01', end='2025-01-31', periods=2),

            'VAR1_IDENTIFIER50': [1, 2],
            'VAR1_SHIFT50': [10, 20],
            'VAR2_IDENTIFIER50': [0, 0],
            'VAR2_SHIFT50': [0, 0],
            'VAR3_IDENTIFIER50': [0, 0],
            'VAR3_SHIFT50': [0, 0],
        })

        result = Loop_7_IPC_HPC_PerfShift(df, lag_list=[50], save_csv=False)

        # Columns should exist, but no assignments
        assert 'IPC_DAMAGE_SHIFT50' in result.columns
        assert 'HPC_DAMAGE_SHIFT50' in result.columns
        assert pd.isna(result.loc[0, 'IPC_DAMAGE_SHIFT50'])
        assert pd.isna(result.loc[1, 'HPC_DAMAGE_SHIFT50'])

    def test_unexpected_identifier_values(self):
        df = pd.DataFrame({
            'NEW_FLAG': [1, 1],
            "reportdatetime": pd.date_range(start='2025-01-31', end='2025-01-01', periods=2),

            'VAR1_IDENTIFIER50': [99, 100],  # Invalid identifiers
            'VAR1_SHIFT50': [10, 20],
            'VAR2_IDENTIFIER50': [0, 0],
            'VAR2_SHIFT50': [0, 0],
            'VAR3_IDENTIFIER50': [0, 0],
            'VAR3_SHIFT50': [0, 0],
        })

        result = Loop_7_IPC_HPC_PerfShift(df, lag_list=[50], save_csv=False)

        # IPC/HPC should remain NaN for invalid identifiers
        assert pd.isna(result.loc[0, 'IPC_DAMAGE_SHIFT50'])
        assert pd.isna(result.loc[0, 'HPC_DAMAGE_SHIFT50'])
        assert pd.isna(result.loc[1, 'IPC_DAMAGE_SHIFT50'])
        assert pd.isna(result.loc[1, 'HPC_DAMAGE_SHIFT50'])

# ------------------------------
# CSV Output Tests
# ------------------------------


class TestLoop7CSVOutput:
    def test_save_csv_disabled(self, tmp_path, monkeypatch):
        df = pd.DataFrame({
            'NEW_FLAG': [1],
            "reportdatetime": pd.date_range(start='2025-01-01', end='2025-01-31', periods=1),

            'VAR1_IDENTIFIER50': ["IPC ETA"],
            'VAR1_SHIFT50': [42],
            'VAR2_IDENTIFIER50': ["XRATE_0"],
            'VAR2_SHIFT50': [0],
            'VAR3_IDENTIFIER50': ["XRATE_0"],
            'VAR3_SHIFT50': [0],
        })

        # Suppress print
        monkeypatch.setattr("builtins.print", lambda x: None)

        result = Loop_7_IPC_HPC_PerfShift(df, lag_list=[50], save_csv=False)
        # Ensure assignment happened ETA VALUES NEGATED
        assert result.loc[0, 'IPC_DAMAGE_SHIFT50'] == -42
        # No file created (tmp_path not used in this test since save_csv=False)
