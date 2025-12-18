import pandas as pd
from src.utils.log_file import debug_info

# show full DataFrame when printing
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)
from src.Loop_3_flag_sv_and_eng_change_v1 import Loop_3_flag_sv_and_eng_change


class TestHappyPath:
    """
    Tests for correct behavior when valid input data is provided.
    """

    def test_first_row_flagged_as_sv(self):
        """
        The very first row for an ESN should always be flagged as a Shop Visit (FlagSV=1).
        Sister change should be 0 for the first row.
        """
        df = pd.DataFrame({
            "ESN": [1],
            "NEW_FLAG": [1],
            "reportdatetime": [1],
            "SISTER_ESN": [100],
            "days_since_prev": [0],
        })

        df_out = Loop_3_flag_sv_and_eng_change(
            df, flight_phase="cruise", DebugOption=0)
        assert df_out.loc[0, "FlagSV"] == 1
        assert df_out.loc[0, "FlagSisChg"] == 0

    def test_sister_change_detected(self):
        """
        If the sister ESN changes between two consecutive rows, FlagSisChg should be 1.
        """
        n = 2
        start_date = pd.to_datetime("2025-01-01")
        end_date = pd.to_datetime("2025-01-02")
        date_list = pd.date_range(start=start_date, end=end_date, periods=n).strftime("%Y-%m-%d %H:%M:%S")
        
        df = pd.DataFrame({
            "ESN": [75001, 75001],
            "NEW_FLAG": [1, 1],
            "reportdatetime": date_list,
            "SISTER_ESN": [75100, 75200],
            "days_since_prev": [0, 1],
        })

        df_out = Loop_3_flag_sv_and_eng_change(
            df, flight_phase="climb", DebugOption=0)
        
        print(f"{debug_info()}---> df_out:\n {df_out}")

        assert df_out.loc[df_out.index[0],
                          "FlagSV"] == 1  # first row always SV
        assert df_out.loc[df_out.index[1], "FlagSisChg"] == 1


class TestEdgeCases:
    """
    Tests for special or problematic inputs.
    """

    def test_large_time_gap_flags_sv(self):
        """
        A gap larger than min_sv_dur should cause a new Shop Visit flag.
        """
        df = pd.DataFrame({
            "ESN": [1, 1],
            "NEW_FLAG": [1, 1],
            "reportdatetime": [1, 50],
            "SISTER_ESN": [100, 100],
            "days_since_prev": [0, 45],  # > 40 (default min_sv_dur)
        })

        df_out = Loop_3_flag_sv_and_eng_change(
            df, flight_phase="descent", DebugOption=0)
        assert df_out.loc[df_out.index[1], "FlagSV"] == 1

    def test_small_time_gap_does_not_flag_sv(self):
        """
        A gap smaller than min_sv_dur should not trigger a new Shop Visit flag.
        """
        df = pd.DataFrame({
            "ESN": [1, 1],
            "NEW_FLAG": [1, 1],
            "reportdatetime": [1, 10],
            "SISTER_ESN": [100, 100],
            "days_since_prev": [0, 5],
        })

        df_out = Loop_3_flag_sv_and_eng_change(
            df, flight_phase="descent", DebugOption=0)
        assert df_out.loc[1, "FlagSV"] == 0

    def test_new_flag_zero_rows_ignored(self):
        """
        Rows where NEW_FLAG=0 should not be processed but still appear in output.
        They should have NaN in new columns, since they are untouched.
        """
        import numpy as np
        n = 2
        start_date = pd.to_datetime("2025-01-01")
        end_date = pd.to_datetime("2025-01-02")
        date_list = pd.date_range(start=start_date, end=end_date, periods=n).strftime("%Y-%m-%d %H:%M:%S")
        
        df = pd.DataFrame({
            "ESN": [1, 1],
            "NEW_FLAG": [0, 1],
            "reportdatetime": date_list,
            "SISTER_ESN": [100, 100],
            "days_since_prev": [0, 1],
        })

        df_out = Loop_3_flag_sv_and_eng_change(
            df, flight_phase="cruise", DebugOption=0)
        print(f"{debug_info()}---> df_out:\n {df_out}")

        # First row NEW_FLAG=0 should keep FlagSV/FlagSisChg as NaN
        assert np.isnan(df_out.loc[df_out.index[0], "FlagSV"])
        assert np.isnan(df_out.loc[df_out.index[0], "FlagSisChg"])

        # Second row (NEW_FLAG=1) should still be processed normally
        assert df_out.loc[df_out.index[1], "FlagSV"] in [0, 1]
        assert df_out.loc[df_out.index[1], "FlagSisChg"] in [0, 1]

    def test_empty_dataframe_returns_empty(self):
        """
        If the input DataFrame is empty, the output should also be empty.
        """
        df = pd.DataFrame(
            columns=[
                "ESN",
                "NEW_FLAG",
                "reportdatetime",
                "SISTER_ESN",
                "days_since_prev"])
        df_out = Loop_3_flag_sv_and_eng_change(
            df, flight_phase="cruise", DebugOption=0)
        assert df_out.empty


class TestDebugOption:
    """
    Tests related to the DebugOption flag that controls CSV saving.
    """

    def test_debugoption_creates_csv(self, tmp_path, monkeypatch):
        """
        DebugOption=1 should save a CSV file to Fleetstore_Data.
        """
        df = pd.DataFrame({
            "ESN": [1, 1],
            "NEW_FLAG": [1, 1],
            "reportdatetime": [1, 2],
            "SISTER_ESN": [100, 100],
            "days_since_prev": [0, 1],
        })

        # Run inside a temporary directory
        monkeypatch.chdir(tmp_path)

        df_out = Loop_3_flag_sv_and_eng_change(
            df, flight_phase="cruise", DebugOption=1)

        fleetstore_dir = tmp_path / "Fleetstore_Data"
        saved_file = fleetstore_dir / "LOOP_3_cruise.csv"

        # Directory and file should exist
        assert isinstance(df_out, pd.DataFrame)
        assert saved_file.exists()
