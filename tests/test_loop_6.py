# tests/test_loop_6_refactored.py
import pytest
import pandas as pd
import numpy as np
from src.Loop_6_fit_signatures import _init_worker, process_row, Loop_6_fit_signatures

# =========================
# Fixtures
# =========================


@pytest.fixture
def sample_df():
    """
    Sample DataFrame with NEW_FLAG == 1 and all required observation columns.
    """
    data = {
        
        "NEW_FLAG": [1, 1],
        "reportdatetime": pd.date_range(start='2025-01-31', end='2025-01-01', periods=2),
        'PS26__DEL_PC_E2E_MAV_NO_STEPS_LAG_': [1.0, 0.5],
        'T25__DEL_PC_E2E_MAV_NO_STEPS_LAG_': [0.5, 0.2],
        'P30__DEL_PC_E2E_MAV_NO_STEPS_LAG_': [0.2, 0.1],
        'T30__DEL_PC_E2E_MAV_NO_STEPS_LAG_': [0.3, 0.3],
        'TGTU__DEL_PC_E2E_MAV_NO_STEPS_LAG_': [0.1, 0.1],
        'NL__DEL_PC_E2E_MAV_NO_STEPS_LAG_': [0.2, 0.2],
        'NI__DEL_PC_E2E_MAV_NO_STEPS_LAG_': [0.1, 0.1],
        'NH__DEL_PC_E2E_MAV_NO_STEPS_LAG_': [0.1, 0.0],
        'FF__DEL_PC_E2E_MAV_NO_STEPS_LAG_': [0.1, 0.1],
    }
    df = pd.DataFrame(data)

    # Preallocate columns like Loop_6_fit_signatures
    lag_list = [50]
    for lag in lag_list:
        for i in range(1, 4):
            df[f"VAR{i}_SHIFT{lag}"] = np.nan
            df[f"VAR{i}_MAGNITUDE{lag}"] = np.nan
            df[f"VAR{i}_IDENTIFIER{lag}"] = pd.Series(
                [np.nan] * len(df), dtype=object)
        df[f"ERROR_REL{lag}"] = np.nan
        df[f"ERROR_MAGNITUDE{lag}"] = np.nan
        df[f"OBS_MAGNITUDE{lag}"] = np.nan

    return df


@pytest.fixture
def xrates_df():
    """
    Minimal Xrates dictionary for testing.
    """
    df = pd.DataFrame(
        {"Sig1": [1.0, 0.0], "Sig2": [0.0, 1.0], "Norm": [1.0, 1.0]},
        index=["X1", "X2"]
    )
    return {"Cruise": df}

# =========================
# Tests
# =========================


class TestInitWorker:
    """Tests for _init_worker"""

    def test_globals_set(self, sample_df):
        _init_worker("sig_combos", ["col1"], [50], sample_df)
        import src.Loop_6_fit_signatures as mod
        assert mod._signature_combos == "sig_combos"
        assert mod._obs_mag_cols == ["col1"]
        assert mod._lag_list == [50]
        pd.testing.assert_frame_equal(mod._df_new, sample_df)


class TestProcessRow:
    """Tests for process_row"""

    def test_process_row_basic(self, sample_df):
        """
        Happy path: VAR_* columns should be updated for valid row.
        """
        # import src.loop6_fit_signatures as mod

        _init_worker(
            signature_combos=[(np.eye(10)[:, :3], np.ones(3), ["X1", "X2", "X3"])],
            obs_mag_cols=[col for col in sample_df.columns if "LAG_" in col],
            lag_list=[50],
            df_new=sample_df
        )
        idx, row_out = process_row(0)

        # Columns now exist because of preallocation
        for i in range(1, 4):
            assert f"VAR{i}_SHIFT50" in row_out
            assert f"VAR{i}_MAGNITUDE50" in row_out
        assert "OBS_MAGNITUDE50" in row_out

    def test_process_row_with_nan(self, sample_df):
        """
        If any observation column is NaN, process_row skips fitting.
        VAR_* should remain NaN.
        """
        # import src.loop6_fit_signatures as mod
        df_nan = sample_df.copy()
        df_nan.loc[0, 'PS26__DEL_PC_E2E_MAV_NO_STEPS_LAG_'] = np.nan

        _init_worker(
            signature_combos=[(np.eye(10)[:, :3], np.ones(3), ["X1", "X2", "X3"])],
            obs_mag_cols=[col for col in df_nan.columns if "LAG_" in col],
            lag_list=[50],
            df_new=df_nan
        )
        idx, row_out = process_row(0)

        assert np.isnan(row_out["VAR1_SHIFT50"])


class TestLoop6FitSignatures:
    """Tests for Loop_6_fit_signatures"""

    def test_happy_path(self, sample_df, xrates_df):
        df_out = Loop_6_fit_signatures(
            df=sample_df,
            flight_phase="cruise",
            Xrates=xrates_df,
            lag_list=[50],
            use_parallel=False,
            DebugOption=0
        )
        assert df_out.shape[0] == sample_df.shape[0]
        for i in range(1, 4):
            assert f"VAR{i}_SHIFT50" in df_out.columns
        assert "OBS_MAGNITUDE50" in df_out.columns

    def test_empty_new_flag(self, sample_df, xrates_df):
        """
        Even if no NEW_FLAG==1 rows, VAR_* columns are still preallocated.
        """
        df_empty = sample_df.copy()
        df_empty["NEW_FLAG"] = 0
        df_out = Loop_6_fit_signatures(
            df=df_empty,
            flight_phase="cruise",
            Xrates=xrates_df,
            lag_list=[50],
            use_parallel=False,
            DebugOption=0
        )
        # Check that VAR columns exist
        for i in range(1, 4):
            assert f"VAR{i}_SHIFT50" in df_out.columns

    def test_missing_columns(self, sample_df, xrates_df):
        """
        Missing observation columns should not break execution.
        VAR_* remains NaN.
        """
        df_missing = sample_df.copy()
        df_missing.drop(
            columns=["PS26__DEL_PC_E2E_MAV_NO_STEPS_LAG_"],
            inplace=True)
        df_out = Loop_6_fit_signatures(
            df=df_missing,
            flight_phase="cruise",
            Xrates=xrates_df,
            lag_list=[50],
            use_parallel=False,
            DebugOption=0
        )
        assert np.isnan(df_out["VAR1_SHIFT50"].iloc[0])

    def test_zero_vector_row(self, sample_df, xrates_df):
        """
        Observation vector with all zeros: fitting is skipped.
        VAR_* remains NaN.
        """
        df_zero = sample_df.copy()
        for col in df_zero.columns:
            if "LAG_" in col:
                df_zero[col] = 0.0
        df_out = Loop_6_fit_signatures(
            df=df_zero,
            flight_phase="cruise",
            Xrates=xrates_df,
            lag_list=[50],
            use_parallel=False,
            DebugOption=0
        )
        assert np.isnan(df_out["VAR1_SHIFT50"].iloc[0])
