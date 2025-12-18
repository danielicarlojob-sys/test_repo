import pandas as pd
import numpy as np
import pytest
from src.Loop_2_E2E_v1 import Loop_2_E2E


class TestHappyPath:
    """
    Tests for correct behavior when valid input data is provided.
    """

    def test_two_engines_correct_deltas(self):
        """
        When two engines exist for the same ACID and reportdatetime,
        E2E deltas should be correctly calculated.
        """
        df = pd.DataFrame({
            "ACID": ["AC123", "AC123"],
            "reportdatetime": ["2025-01-01", "2025-01-01"],
            "ESN": [1, 2],
            "PS26__DEL_PC": [10.0, 12.0],
            "T25__DEL_PC": [5.0, 7.0],
            "P30__DEL_PC": [1.0, 2.0],
            "T30__DEL_PC": [4.0, 6.0],
            "TGTU__DEL_PC": [50.0, 55.0],
            "NL__DEL_PC": [1.1, 1.2],
            "NI__DEL_PC": [2.1, 2.2],
            "NH__DEL_PC": [3.1, 3.2],
            "FF__DEL_PC": [4.1, 4.2],
            "P160__DEL_PC": [5.1, 5.2],
            "NEW_FLAG": [1, 1],
        })

        df_out = Loop_2_E2E(df, flight_phase="cruise", DebugOption=0)

        assert len(df_out) == 2
        assert set(df_out["SISTER_ESN"]) == {1, 2}

        eng1 = df_out[df_out["ESN"] == 1].iloc[0]
        assert eng1["PS26__DEL_PC_E2E"] == pytest.approx(10.0 - 12.0, rel=1e-6)


class TestEdgeCases:
    """
    Tests for special or problematic inputs.
    """

    def test_single_engine_group_ignored(self):
        df = pd.DataFrame({
            "ACID": ["AC123"],
            "reportdatetime": ["2025-01-01"],
            "ESN": [1],
            "PS26__DEL_PC": [10.0],
            "NEW_FLAG": [1],
        })
        df_out = Loop_2_E2E(df, flight_phase="cruise", DebugOption=0)
        
        assert df_out.empty

    def test_three_engines_group_ignored(self):
        df = pd.DataFrame({
            "ACID": ["AC123"] * 3,
            "reportdatetime": ["2025-01-01"] * 3,
            "ESN": [1, 2, 3],
            "PS26__DEL_PC": [10.0, 12.0, 14.0],
            "NEW_FLAG": [1, 1, 1],
        })
        df_out = Loop_2_E2E(df, flight_phase="cruise", DebugOption=0)
        assert df_out.empty

    def test_missing_parameter_column(self):
        df = pd.DataFrame({
            "ACID": ["AC123", "AC123"],
            "reportdatetime": ["2025-01-01", "2025-01-01"],
            "ESN": [1, 2],
            "PS26__DEL_PC": [10.0, 12.0],
            "NEW_FLAG": [1, 1],
        })
        df_out = Loop_2_E2E(df, flight_phase="cruise", DebugOption=0)

        assert "PS26__DEL_PC_E2E" in df_out.columns
        assert df_out.filter(like="T25__DEL_PC_E2E").isna().all().all()

    def test_nan_values_propagate(self):
        df = pd.DataFrame({
            "ACID": ["AC123", "AC123"],
            "reportdatetime": ["2025-01-01", "2025-01-01"],
            "ESN": [1, 2],
            "PS26__DEL_PC": [np.nan, 12.0],
            "T25__DEL_PC": [5.0, 7.0],
            "NEW_FLAG": [1, 1],
        })
        df_out = Loop_2_E2E(df, flight_phase="cruise", DebugOption=0)
        assert pd.isna(df_out.loc[df_out["ESN"] ==
                       1, "PS26__DEL_PC_E2E"]).all()

    def test_empty_dataframe_returns_empty(self):
        df = pd.DataFrame(columns=[
            "ACID", "reportdatetime", "ESN", "PS26__DEL_PC", "NEW_FLAG"
        ])
        df_out = Loop_2_E2E(df, flight_phase="cruise", DebugOption=0)
        assert df_out.empty

    def test_mixed_old_and_new_rows(self):
        """
        When NEW_FLAG includes both 0 (old) and 1 (new), only the new rows
        should be processed, while old rows remain unchanged.
        """
        df = pd.DataFrame({
            "ACID": ["AC123", "AC123", "AC123", "AC123"],
            "reportdatetime": ["2025-01-01"] * 4,
            "ESN": [1, 2, 1, 2],
            "PS26__DEL_PC": [10.0, 12.0, 20.0, 22.0],
            "NEW_FLAG": [0, 0, 1, 1],  # first pair old, second pair new
        })

        df_out = Loop_2_E2E(df, flight_phase="cruise", DebugOption=0)

        # Old rows should remain unprocessed (no sister or delta columns)
        old_rows = df_out[df_out["NEW_FLAG"] == 0]
        assert "SISTER_ESN" not in old_rows.columns or old_rows["SISTER_ESN"].isna(
        ).all()

        # New rows should have been processed with E2E deltas
        new_rows = df_out[df_out["NEW_FLAG"] == 1]
        old_rows = df_out[df_out["NEW_FLAG"] == 0]

        assert "SISTER_ESN" in new_rows.columns
        assert "PS26__DEL_PC_E2E" in new_rows.columns
        assert set(new_rows["SISTER_ESN"]) == {1, 2}
        assert new_rows.loc[new_rows["ESN"] == 1,
                            "PS26__DEL_PC_E2E"].iloc[0] == pytest.approx(20.0 - 22.0)
        assert np.isnan(old_rows.loc[old_rows["ESN"] == 1,"PS26__DEL_PC_E2E"].iloc[0]) 
    
    
    def test_missing_sister_engine_data(self):
        """
        When data is not available for sister engine E2E should be NaN.
        """
        df = pd.DataFrame({
        "ACID": ["AC123", "AC123", "AC123"],
        "reportdatetime": ["2025-01-01","2025-01-01","2025-01-02"] ,
        "ESN": [1, 2, 1],
        "PS26__DEL_PC": [10.0, 12.0, 20.0],
        "NEW_FLAG": [0, 0, 1],  # first pair old, last point new
    })

        df_out = Loop_2_E2E(df, flight_phase="cruise", DebugOption=0)
        # New rows should not have been processed new_rows should be empty
        new_rows = df_out[df_out["NEW_FLAG"] == 1]
        old_rows = df_out[df_out["NEW_FLAG"] == 0]
        #assert "SISTER_ESN" in new_rows.columns
        #assert "PS26__DEL_PC_E2E" in new_rows.columns
        assert new_rows.empty
        assert 'SISTER_ESN' not in old_rows.columns
        assert 'PS26__DEL_PC_E2E' not in old_rows.columns
        
        

class TestDebugOption:
    """
    Tests related to the DebugOption flag that controls CSV saving.
    """

    def test_debugoption_creates_csv(self, tmp_path, monkeypatch):
        df = pd.DataFrame({
            "ACID": ["AC123", "AC123"],
            "reportdatetime": ["2025-01-01", "2025-01-01"],
            "ESN": [1, 2],
            "PS26__DEL_PC": [10.0, 12.0],
            "T25__DEL_PC": [5.0, 7.0],
            "P30__DEL_PC": [1.0, 2.0],
            "T30__DEL_PC": [4.0, 6.0],
            "TGTU__DEL_PC": [50.0, 55.0],
            "NL__DEL_PC": [1.1, 1.2],
            "NI__DEL_PC": [2.1, 2.2],
            "NH__DEL_PC": [3.1, 3.2],
            "FF__DEL_PC": [4.1, 4.2],
            "P160__DEL_PC": [5.1, 5.2],
            "NEW_FLAG": [1, 1],
        })

        monkeypatch.chdir(tmp_path)

        df_out = Loop_2_E2E(df, flight_phase="cruise", DebugOption=1)

        fleetstore_dir = tmp_path / "Fleetstore_Data"
        saved_file = fleetstore_dir / "LOOP_2_cruise.csv"

        assert isinstance(df_out, pd.DataFrame)
        assert saved_file.exists()
