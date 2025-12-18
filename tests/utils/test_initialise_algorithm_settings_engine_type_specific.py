import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock

from src.utils.Initialise_Algorithm_Settings_engine_type_specific import (
    Initialise_Algorithm_Settings_engine_type_specific,
    Xrates_dic_vector_norm,
)


@pytest.fixture
def mock_xrates_reader():
    """Fixture to mock Xrates_reader returning deterministic DataFrames."""
    def _mock(d):
        # Return different dummy DataFrames depending on DSC index
        phases = ["Cruise", "Take-off", "Climb"]
        data = {
            "P26": [1 + d, 2 + d],
            "T26": [3 + d, 4 + d],
            "P30": [5 + d, 6 + d],
            "T30": [7 + d, 8 + d],
            "TGT": [9 + d, 10 + d],
            "NL": [11 + d, 12 + d],
            "NI": [13 + d, 14 + d],
            "NH": [15 + d, 16 + d],
            "WFE": [17 + d, 18 + d],
        }
        return phases[d], pd.DataFrame(data)
    return _mock


@pytest.fixture
def mock_log_message():
    """Fixture to capture log_message calls without writing files."""
    return MagicMock()


def test_initialise_happy_path(mock_xrates_reader, mock_log_message):
    """Happy path: lim_dict has expected keys, Xrates has 3 flight phases."""
    with patch(
        "src.utils.Initialise_Algorithm_Settings_engine_type_specific.Xrates_reader",
        mock_xrates_reader
    ), patch(
        "src.utils.Initialise_Algorithm_Settings_engine_type_specific.log_message",
        mock_log_message
    ):

        lim_dict, Xrates = Initialise_Algorithm_Settings_engine_type_specific()

    # Check lim_dict contains expected keys (matching the function)
    expected_keys = {
        'Lag', 'nLag', 'RelErrThresh', 'nRelErrThresh',
        'EtaThresh', 'nEtaThresh',
        'lim', 'num', 'BackDatingLimitDays',
        'NumRawREALs', 'NumDerivedREALs',
        'NumRawCELLs', 'NumDerivedCELLs',
        'AltLowLim', 'AltUppLim',
        'MnLowLim', 'MnUppLim',
        'Param', 'ParamLowLim', 'ParamUppLim'
    }
    assert expected_keys.issubset(lim_dict.keys())

    # Check Xrates dict contains 3 flight phases
    assert set(Xrates.keys()) == {"Cruise", "Take-off", "Climb"}

    # Ensure log_message was called at least 4 times (3 phases + final)
    assert mock_log_message.call_count >= 4


def test_xrates_dic_vector_norm_happy_path(
        mock_xrates_reader, mock_log_message):
    """Ensure Vector_Norm is added and computed correctly."""
    with patch(
        "src.utils.Initialise_Algorithm_Settings_engine_type_specific.Xrates_reader",
        mock_xrates_reader
    ), patch(
        "src.utils.Initialise_Algorithm_Settings_engine_type_specific.log_message",
        mock_log_message
    ):

        _, Xrates = Initialise_Algorithm_Settings_engine_type_specific()

    Xrates_normed = Xrates_dic_vector_norm(Xrates)

    for phase, df in Xrates_normed.items():
        # Check 'Vector_Norm' column exists
        assert "Vector_Norm" in df.columns
        # Recompute norm manually for first row and compare
        first_row = df.drop(columns=["Vector_Norm"]).iloc[0].values
        expected_norm = np.linalg.norm(first_row)
        assert np.isclose(df.iloc[0]["Vector_Norm"], expected_norm)
