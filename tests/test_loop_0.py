import pandas as pd
import numpy as np
import pytest

from src.Loop_0_Calculate_Deltas_v1 import Loop_0_delta_calc


class TestHappyPath:
    """
    Tests that verify correct behavior when valid input data is provided.
    """

    def test_correct_delta_calculation(self):
        """
        Ensure deltas are computed correctly when all nominal values are non-zero.
        """
        df = pd.DataFrame({
            "ESN": [1],
            "reportdatetime": ["2025-01-01"],
            "ACID": ["AC123"],
            "ENGPOS": [1],
            "NEW_FLAG": [1],   # Required by decorator
            "P25__PSI": [110.0],
            "PS26S__NOM_PSI": [100.0],   # baseline
            "T25__DEGC": [210.0],
            "TS25S__NOM_K": [200.0],
            "P30__PSI": [305.0],
            "PS30S__NOM_PSI": [300.0],
            "T30__DEGC": [405.0],
            "TS30S__NOM_K": [400.0],
            "TGTU_A__DEGC": [600.0],
            "TGTS__NOM_K": [500.0],
            "NL__PC": [101.0],
            "NL__NOM_PC": [100.0],
            "NI__PC": [202.0],
            "NI__NOM_PC": [200.0],
            "NH__PC": [303.0],
            "NH__NOM_PC": [300.0],
            "FF__LBHR": [505.0],
            "FF__NOM_LBHR": [500.0],
            "PS160__PSI": [410.0],
            "P135S__NOM_PSI": [400.0],
        })

        df_out = Loop_0_delta_calc(df, flight_phase="cruise", DebugOption=0)

        assert df_out["PS26__DEL_PC"].iloc[0] == 10.0
        assert df_out["T25__DEL_PC"].iloc[0] == 5.0
        assert df_out["P30__DEL_PC"].iloc[0] == pytest.approx(
            1.66667, rel=1e-5)
        assert "TGTU__DEL_PC" in df_out.columns


class TestEdgeCases:
    """
    Tests that verify how the function handles problematic or boundary input cases.
    """

    def test_division_by_zero_nominal(self):
        df = pd.DataFrame({
            "ESN": [1],
            "reportdatetime": ["2025-01-01"],
            "ACID": ["AC123"],
            "ENGPOS": [1],
            "NEW_FLAG": [1],
            "P25__PSI": [50.0],
            "PS26S__NOM_PSI": [0.0],  # division by zero
            "T25__DEGC": [np.nan],
            "TS25S__NOM_K": [200.0],
            "P30__PSI": [305.0],
            "PS30S__NOM_PSI": [300.0],
            "T30__DEGC": [405.0],
            "TS30S__NOM_K": [400.0],
            "TGTU_A__DEGC": [600.0],
            "TGTS__NOM_K": [500.0],
            "NL__PC": [101.0],
            "NL__NOM_PC": [100.0],
            "NI__PC": [202.0],
            "NI__NOM_PC": [200.0],
            "NH__PC": [303.0],
            "NH__NOM_PC": [300.0],
            "FF__LBHR": [505.0],
            "FF__NOM_LBHR": [500.0],
            "PS160__PSI": [410.0],
            "P135S__NOM_PSI": [400.0],
        })

        df_out = Loop_0_delta_calc(df, flight_phase="cruise", DebugOption=0)
        assert np.isinf(
            df_out["PS26__DEL_PC"].iloc[0]) or np.isnan(
            df_out["PS26__DEL_PC"].iloc[0])

    def test_missing_columns_raises(self):
        df = pd.DataFrame({
            "ESN": [1],
            "NEW_FLAG": [1],
            "P25__PSI": [100.0],
        })
        with pytest.raises(KeyError):
            Loop_0_delta_calc(df, flight_phase="climb", DebugOption=0)

    def test_empty_dataframe(self):
        """
        If input DataFrame is empty but has the full required schema,
        the function should return an empty DataFrame.
        This mimics the realistic pipeline where upstream filtering ensures
        all required pairs are present, but the dataset may be empty.
        """
        df = pd.DataFrame({
            "ESN": pd.Series(dtype="int64"),
            "NEW_FLAG": pd.Series(dtype="int64"),
            "reportdatetime": pd.Series(dtype="datetime64[ns]"),
            "ACID": pd.Series(dtype="string"),
            "ENGPOS": pd.Series(dtype="int64"),
            "P25__PSI": pd.Series(dtype="float64"),
            "PS26S__NOM_PSI": pd.Series(dtype="float64"),
            "T25__DEGC": pd.Series(dtype="float64"),
            "TS25S__NOM_K": pd.Series(dtype="float64"),
            "P30__PSI": pd.Series(dtype="float64"),
            "PS30S__NOM_PSI": pd.Series(dtype="float64"),
            "T30__DEGC": pd.Series(dtype="float64"),
            "TS30S__NOM_K": pd.Series(dtype="float64"),
            "TGTU_A__DEGC": pd.Series(dtype="float64"),
            "TGTS__NOM_K": pd.Series(dtype="float64"),
            "NL__PC": pd.Series(dtype="float64"),
            "NL__NOM_PC": pd.Series(dtype="float64"),
            "NI__PC": pd.Series(dtype="float64"),
            "NI__NOM_PC": pd.Series(dtype="float64"),
            "NH__PC": pd.Series(dtype="float64"),
            "NH__NOM_PC": pd.Series(dtype="float64"),
            "FF__LBHR": pd.Series(dtype="float64"),
            "FF__NOM_LBHR": pd.Series(dtype="float64"),
            "PS160__PSI": pd.Series(dtype="float64"),
            "P135S__NOM_PSI": pd.Series(dtype="float64"),
        })

        df_out_temp = Loop_0_delta_calc(
            df, flight_phase="Cruise", DebugOption=0)
        assert df_out_temp.empty

    def test_with_nan_values(self):
        df = pd.DataFrame({
            "ESN": [1],
            "reportdatetime": ["2025-01-01"],
            "ACID": ["AC123"],
            "ENGPOS": [1],
            "NEW_FLAG": [1],
            "P25__PSI": [np.nan],
            "PS26S__NOM_PSI": [100.0],
            "T25__DEGC": [210.0],
            "TS25S__NOM_K": [200.0],
            "P30__PSI": [305.0],
            "PS30S__NOM_PSI": [300.0],
            "T30__DEGC": [405.0],
            "TS30S__NOM_K": [400.0],
            "TGTU_A__DEGC": [600.0],
            "TGTS__NOM_K": [500.0],
            "NL__PC": [101.0],
            "NL__NOM_PC": [100.0],
            "NI__PC": [202.0],
            "NI__NOM_PC": [200.0],
            "NH__PC": [303.0],
            "NH__NOM_PC": [300.0],
            "FF__LBHR": [505.0],
            "FF__NOM_LBHR": [500.0],
            "PS160__PSI": [410.0],
            "P135S__NOM_PSI": [400.0],
        })

        df_out = Loop_0_delta_calc(df, flight_phase="cruise", DebugOption=0)
        assert pd.isna(df_out["PS26__DEL_PC"].iloc[0])


class TestDebugOption:
    """
    Tests related to the DebugOption parameter, which saves a CSV file for traceability.
    """

    def test_debugoption_creates_csv(self, tmp_path, monkeypatch):
        df = pd.DataFrame({
            "ESN": [1],
            "reportdatetime": ["2025-01-01"],
            "ACID": ["AC123"],
            "ENGPOS": [1],
            "NEW_FLAG": [1],
            "P25__PSI": [110.0],
            "PS26S__NOM_PSI": [100.0],
            "T25__DEGC": [210.0],
            "TS25S__NOM_K": [200.0],
            "P30__PSI": [305.0],
            "PS30S__NOM_PSI": [300.0],
            "T30__DEGC": [405.0],
            "TS30S__NOM_K": [400.0],
            "TGTU_A__DEGC": [600.0],
            "TGTS__NOM_K": [500.0],
            "NL__PC": [101.0],
            "NL__NOM_PC": [100.0],
            "NI__PC": [202.0],
            "NI__NOM_PC": [200.0],
            "NH__PC": [303.0],
            "NH__NOM_PC": [300.0],
            "FF__LBHR": [505.0],
            "FF__NOM_LBHR": [500.0],
            "PS160__PSI": [410.0],
            "P135S__NOM_PSI": [400.0],
        })

        monkeypatch.chdir(tmp_path)
        df_out = Loop_0_delta_calc(df, flight_phase="cruise", DebugOption=1)

        fleetstore_dir = tmp_path / "Fleetstore_Data"
        saved_file = fleetstore_dir / "LOOP_0_cruise.csv"

        assert isinstance(df_out, pd.DataFrame)
        assert saved_file.exists()


class TestProcessOnlyNew:
    """
    Tests specifically for the process_only_new decorator applied to Loop_0_delta_calc.
    """

    def test_old_rows_unmodified_new_rows_processed(self):
        """
        Rows with NEW_FLAG=0 should remain untouched, while rows with NEW_FLAG=1 are processed.
        """
        df = pd.DataFrame({
            "ESN": [1, 2],
            "reportdatetime": ["2025-01-01", "2025-01-02"],
            "ACID": ["AC123", "AC123"],
            "ENGPOS": [1, 1],
            "NEW_FLAG": [0, 1],
            "P25__PSI": [999.0, 110.0],
            "PS26S__NOM_PSI": [999.0, 100.0],
            "T25__DEGC": [999.0, 210.0],
            "TS25S__NOM_K": [999.0, 200.0],
            "P30__PSI": [999.0, 305.0],
            "PS30S__NOM_PSI": [999.0, 300.0],
            "T30__DEGC": [999.0, 405.0],
            "TS30S__NOM_K": [999.0, 400.0],
            "TGTU_A__DEGC": [999.0, 600.0],
            "TGTS__NOM_K": [999.0, 500.0],
            "NL__PC": [999.0, 101.0],
            "NL__NOM_PC": [999.0, 100.0],
            "NI__PC": [999.0, 202.0],
            "NI__NOM_PC": [999.0, 200.0],
            "NH__PC": [999.0, 303.0],
            "NH__NOM_PC": [999.0, 300.0],
            "FF__LBHR": [999.0, 505.0],
            "FF__NOM_LBHR": [999.0, 500.0],
            "PS160__PSI": [999.0, 410.0],
            "P135S__NOM_PSI": [999.0, 400.0],
        })

        df_out = Loop_0_delta_calc(df, flight_phase="cruise", DebugOption=0)

        # Old row should not have computed deltas (remain NaN)
        assert pd.isna(df_out.loc[0, "PS26__DEL_PC"])
        # New row should be processed correctly
        assert df_out.loc[1, "PS26__DEL_PC"] == 10.0
