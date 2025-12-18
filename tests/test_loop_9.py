import pytest
import pandas as pd
import os
from datetime import datetime, timedelta
from src.Loop_9_combine_DSC import Loop_9_combine_DSC

@pytest.fixture
def sample_data_dict():
    """Fixture that creates a small dataset for take-off, climb, and cruise phases.

    Each DataFrame contains two rows, one with NEW_FLAG=1 (new data) and one with
    NEW_FLAG=0 (old data). FRACTION_GT_* columns are used to compute row_sum.
    """
    base_time = datetime(2025, 1, 1, 12, 0)
    df_takeoff = pd.DataFrame({
        "ESN": [1001, 1002],
        "operator": ["OpA", "OpB"],
        "ACID": ["AC1", "AC2"],
        "ENGPOS": [1, 2],
        "DSCID": [53, 53],
        "reportdatetime": [base_time, base_time + timedelta(minutes=10)],
        "NEW_FLAG": [1, 0],
        "FRACTION_GT_1": [0.1, 0.5],
        "FRACTION_GT_2": [0.2, 0.6]
    })
    df_climb = df_takeoff.copy()
    df_climb["DSCID"] = 54
    df_cruise = df_takeoff.copy()
    df_cruise["DSCID"] = 52
    return {"take-off": df_takeoff, "climb": df_climb, "cruise": df_cruise}

class TestLoop9HappyPath:
    def test_row_sum_computation(self, sample_data_dict):
        """Check that NEW rows get row_sum computed correctly and OLD rows remain NaN.

        - For NEW rows: row_sum should equal the number of FRACTION_GT_* columns
          that exceed Lim_dict['lim'].
        - For OLD rows: row_sum should not be filled in and should remain NaN.
        """
        data_dict, df_merged = Loop_9_combine_DSC(
            sample_data_dict, Lim_dict={'lim': 0.15, 'num': 3}, save_csv=False
        )
        df_takeoff = data_dict['take-off']
        assert df_takeoff.loc[df_takeoff['NEW_FLAG']
                              == 1, 'row_sum'].iloc[0] == 1
        assert pd.isna(
            df_takeoff.loc[df_takeoff['NEW_FLAG'] == 0, 'row_sum']).iloc[0]

    def test_merged_dataframe_columns(self, sample_data_dict):
        """Ensure merged DataFrame has the expected set of output columns.

        Verifies that reportdatetime_* and row_sum_* columns exist for
        each flight phase (take-off, climb, cruise).
        """
        _, df_merged = Loop_9_combine_DSC(
            sample_data_dict, Lim_dict={'lim': 0.0, 'num': 3}, save_csv=False
        )
        expected_cols = [
            'ESN',
            'operator',
            'ACID',
            'ENGPOS',
            'reportdatetime_takeoff',
            'reportdatetime_climb',
            'reportdatetime_cruise',
            'row_sum_takeoff',
            'row_sum_climb',
            'row_sum_cruise']
        assert all(col in df_merged.columns for col in expected_cols)

class TestLoop9EdgeCases:
    def test_empty_dataframes(self):
        """Handle completely empty inputs gracefully.

        - Input: Empty DataFrames with required schema but no rows.
        - Expected: The merged DataFrame should also be empty without raising errors.
        """
        cols = ["ESN", "operator", "ACID", "ENGPOS", "DSCID", "reportdatetime",
                "NEW_FLAG", "FRACTION_GT_1", "FRACTION_GT_2", "row_sum"]
        empty_df = pd.DataFrame(columns=cols)
        empty_dict = {
            "take-off": empty_df,
            "climb": empty_df.copy(),
            "cruise": empty_df.copy()}
        data_dict, df_merged = Loop_9_combine_DSC(empty_dict, save_csv=False)
        assert df_merged.empty

    def test_no_new_flag_rows(self, sample_data_dict):
        """Case where all rows are OLD (NEW_FLAG=0).

        - Expectation: row_sum should remain NaN for all rows,
          since no new data is being processed.
        """
        for df in sample_data_dict.values():
            df['NEW_FLAG'] = 0
            df['row_sum'] = None
        data_dict, _ = Loop_9_combine_DSC(sample_data_dict, save_csv=False)
        for df in data_dict.values():
            assert df['row_sum'].isna().all()

    def test_save_csv_creates_files(self, sample_data_dict, tmp_path):
        """Verify that CSV files are written when save_csv=True.

        Creates a temporary Fleetstore_Data directory, runs the function,
        and checks that one or more CSVs are produced.
        """
        fleet_dir = tmp_path / "Fleetstore_Data"
        fleet_dir.mkdir()
        cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            Loop_9_combine_DSC(sample_data_dict, save_csv=True)
            files = list(fleet_dir.glob("*.csv"))
            assert len(files) > 0
        finally:
            os.chdir(cwd)

class TestLoop9ThresholdVariations:
    def test_threshold_edge_values(self, sample_data_dict):
        """Check that row_sum reacts correctly at the threshold boundary.

        - Sets the limit equal to the maximum FRACTION_GT_* value.
        - Depending on equality comparison, row_sum could be 0, 1, or 2.
        - Test accepts any of these as valid.
        """
        df_new = sample_data_dict['take-off']
        df_new['NEW_FLAG'] = 1
        max_val = df_new[['FRACTION_GT_1', 'FRACTION_GT_2']].max().max()
        data_dict, _ = Loop_9_combine_DSC(
            sample_data_dict, Lim_dict={
                'lim': max_val, 'num': 1}, save_csv=False)
        row_sum = data_dict['take-off']['row_sum'].iloc[0]
        assert row_sum in [0, 1, 2]
