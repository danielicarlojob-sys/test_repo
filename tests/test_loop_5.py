import pytest
import pandas as pd

pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', None)     # Show all rows
pd.set_option('display.max_colwidth', None) # Show full column width
pd.set_option('display.width', 0)           # Auto-adjust display width
import numpy as np
import random
from src.Loop_5_performance_trend import Loop5_performance_trend
from src.utils.log_file import debug_info



# --------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------
@pytest.fixture
def base_df():
    """Return a small DataFrame with ESN, NEW_FLAG, and E2E_MAV columns."""
    n = 4
    data = {
        "ESN": [1, 1, 1, 2],
        "reportdatetime": pd.date_range(start='2025-01-01', end='2025-01-31', periods=4),
        "NEW_FLAG": [1, 1, 0, 1],
        "PS26__DEL_PC_E2E_MAV_NO_STEPS": [round(random.random()*1000,2) for _ in range(n)],
        "T25__DEL_PC_E2E_MAV_NO_STEPS": [round(random.random()*1000,2) for _ in range(n)],
        "P30__DEL_PC_E2E_MAV_NO_STEPS": [round(random.random()*1000,2) for _ in range(n)],
        "T30__DEL_PC_E2E_MAV_NO_STEPS": [round(random.random()*1000,2) for _ in range(n)],
        "TGTU__DEL_PC_E2E_MAV_NO_STEPS": [round(random.random()*1000,2) for _ in range(n)],
        "NL__DEL_PC_E2E_MAV_NO_STEPS": [round(random.random()*1000,2) for _ in range(n)],
        "NI__DEL_PC_E2E_MAV_NO_STEPS": [round(random.random()*1000,2) for _ in range(n)],
        "NH__DEL_PC_E2E_MAV_NO_STEPS": [round(random.random()*1000,2) for _ in range(n)],
        "FF__DEL_PC_E2E_MAV_NO_STEPS": [round(random.random()*1000,2) for _ in range(n)],
        "P160__DEL_PC_E2E_MAV_NO_STEPS": [round(random.random()*1000,2) for _ in range(n)],
    }
    df = pd.DataFrame(data)
    cols = df.columns
    MAV_cols = [col for col in cols if "DEL_PC_E2E_MAV_NO_STEPS" in cols]
    return df

@pytest.fixture
def eng_change_df():
    """Return a small DataFrame with ESN, NEW_FLAG, and E2E_MAV columns."""
    n = 100
    c = 30
    data_ens1 = {
        "ESN": [75001]*n,
        "operator":["Op1"]*n,
        "ACID":["AC1"]*n,
        "ENGPOS":[1]*n,
        "reportdatetime": pd.date_range(start='2025-01-01', end='2025-01-31', periods=n),
        "NEW_FLAG": [1]*n,
        "PS26__DEL_PC_E2E_MAV_NO_STEPS": [round(random.random()*1000,2) for _ in range(n)],
        "T25__DEL_PC_E2E_MAV_NO_STEPS": [round(random.random()*1000,2) for _ in range(n)],   
    }
    data_ens2 = {
        "ESN": [75002]*c,
        "operator":["Op1"]*c,
        "ACID":["AC1"]*c,
        "ENGPOS":[2]*c,
        "reportdatetime": pd.date_range(start='2025-01-01', end='2025-01-31', periods=c),
        "NEW_FLAG": [1]*c,
        "PS26__DEL_PC_E2E_MAV_NO_STEPS": [round(random.random()*1000,2) for _ in range(c)],
        "T25__DEL_PC_E2E_MAV_NO_STEPS": [round(random.random()*1000,2) for _ in range(c)],   
    }


    data = {k: data_ens1[k] + data_ens2[k] for k in data_ens1}


    df = pd.DataFrame(data)
    cols = df.columns
    MAV_cols = [col for col in cols if "DEL_PC_E2E_MAV_NO_STEPS" in cols]
    return df


# --------------------------------------------------------------------
# Happy-path tests
# --------------------------------------------------------------------
class TestLoop5HappyPath:
    def test_creates_lagged_columns(self, base_df):
        cols = base_df.columns
        non_MAV_cols =  [col for col in cols if "DEL_PC_E2E_MAV_NO_STEPS" not in col]
        # print(f"{debug_info()} ---> cols:\n{cols}")
        
        # print(f"{debug_info()} ---> non_MAV_cols:\n{non_MAV_cols}")

        MAV_cols = [col for col in cols if "DEL_PC_E2E_MAV_NO_STEPS" in cols]
        """Check lagged columns are created correctly when history exists."""
        out = Loop5_performance_trend(base_df, flight_phase="cruise", Lag=[1], DebugOption=0)
        # print(f"{debug_info()} ---> out:\n{out[cols]}")
        # Columns should exist
        assert "PS26__DEL_PC_E2E_MAV_NO_STEPS_LAG_1" in out.columns
        assert "T25__DEL_PC_E2E_MAV_NO_STEPS_LAG_1" in out.columns
       

        # Values should be differences
        expected = [round(base_df.loc[1, "PS26__DEL_PC_E2E_MAV_NO_STEPS"] - base_df.loc[0, "PS26__DEL_PC_E2E_MAV_NO_STEPS"],2),
                    round(base_df.loc[2, "PS26__DEL_PC_E2E_MAV_NO_STEPS"] - base_df.loc[1, "PS26__DEL_PC_E2E_MAV_NO_STEPS"],2)]

        assert out.loc[[0,3], "PS26__DEL_PC_E2E_MAV_NO_STEPS_LAG_1"].isna().all()
        assert out.loc[[1,2], "PS26__DEL_PC_E2E_MAV_NO_STEPS_LAG_1"].to_list() == expected


    def test_multiple_esns_processed(self, base_df):
        """Ensure each ESN with NEW_FLAG==1 is processed independently."""
        out = Loop5_performance_trend(base_df, flight_phase="cruise", Lag=[1], DebugOption=0)

        # Row 3 belongs to ESN=E2, lag 1 has no history, so it should remain NaN
        assert np.isnan(out.loc[3, "PS26__DEL_PC_E2E_MAV_NO_STEPS_LAG_1"])

    def test_preserves_original_columns(self, base_df):
        """Original columns must not be lost after processing."""
        out = Loop5_performance_trend(base_df, flight_phase="cruise", Lag=[1], DebugOption=0)
        for col in [
            "ESN", "NEW_FLAG", "PS26__DEL_PC_E2E_MAV_NO_STEPS", "T25__DEL_PC_E2E_MAV_NO_STEPS"
        ]:
            assert col in out.columns


# --------------------------------------------------------------------
# Edge-case tests
# --------------------------------------------------------------------
class TestLoop5EdgeCases:
    def test_no_new_rows_returns_same(self, base_df):
        """If NEW_FLAG==1 never appears, DataFrame should remain unchanged."""
        df = base_df.copy()
        print(f"{debug_info()}---> df:\n{df}")
        df["NEW_FLAG"] = 0
        out = Loop5_performance_trend(df, flight_phase="climb", Lag=[1], DebugOption=0)
        print(f"{debug_info()}---> out:\n{out}")
        
        pd.testing.assert_frame_equal(out, df)

    def test_not_enough_history(self, base_df):
        """If there isn’t enough history for a lag, no difference is calculated (remains NaN)."""
        out = Loop5_performance_trend(base_df, flight_phase="cruise", Lag=[5], DebugOption=0)

        # With lag=5, no row has enough history in this fixture
        lagged_cols = [c for c in out.columns if c.endswith("_LAG_5")]
        assert all(out[c].isna().all() for c in lagged_cols)

    def test_handles_nans_in_source_columns(self, base_df):
        """If source values are NaN, differences should not be calculated."""
        df = base_df.copy()
        df.at[1, "PS26__DEL_PC_E2E_MAV_NO_STEPS"] = np.nan

        out = Loop5_performance_trend(df, flight_phase="cruise", Lag=[1], DebugOption=0)

        # Row 1 should remain NaN because of NaN in source
        assert np.isnan(out.loc[1, "PS26__DEL_PC_E2E_MAV_NO_STEPS_LAG_1"])
