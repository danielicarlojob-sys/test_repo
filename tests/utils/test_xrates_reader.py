import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch

from src.utils.xrates_reader import Xrates_reader

# -------------------
# Helpers
# -------------------

def get_rows_to_keep(d):
    """Return the actual rows_to_keep list depending on d (0=cruise,1=takeoff,2=climb)."""
    if d == 0:  # Cruise
        return [
            'IPC ETA', 'HPC ETA', 'HPT ETA', 'IPT ETA', 'LPT ETA',
            'HPT CAPACITY', 'IPT CAPACITY', 'DPP COMB',
            'BI8435Q (IP8 Bleed to IPT Rear)', 'BH326Q (HP3 Bleed to IPC Rear)',
            'BH342Q (HP3 Bleed to IP NGV Throat)', 'BH3435Q (HP3 Bleed to IPT Rear)',
            'BH642Q (HP6 Bleed to IP NGV Throat)', 'ESSAI switched from off to on',
            'SAS valve switch from OFF to ON', 'plus 1 DEG VSV',
            '1% increase in P20 by simulating a MN error', 'HP TCC flow',
            'IP TCC flow', 'LP TCC flow', 'TPR (representing measurment error) + 1%',
            '1% T20 (inc TPR)', 'TGT', 'P30', 'WFE +1%', 'PCNL +1%',
            'PCNI +1%', 'PCNH +1%', 'P26 +1%', 'T26 +1%', 'T30 +1%',
            'Additional shift (not IPC or HPC damage)'
        ]
    else:  # Takeoff & Climb
        return [
            'IPC ETA', 'HPC ETA', 'HPT ETA', 'IPT ETA', 'LPT ETA',
            'HPT CAPACITY', 'IPT CAPACITY', 'DPP COMB',
            'BI8435Q (IP8 Bleed to IPT Rear)', 'BH326Q (HP3 Bleed to IPC Rear)',
            'BH342Q (HP3 Bleed to IP NGV Throat)', 'BH3435Q (HP3 Bleed to IPT Rear)',
            'BH642Q (HP6 Bleed to IP NGV Throat)', 'ESSAI switched from off to on',
            'SAS valve switch from OFF to ON', 'plus 1 DEG VSV',
            '1% increase in P20 by simulating a MN error', 'HP TCC flow',
            'IP TCC flow', 'LP TCC flow', 'TPR (representing measurment error)',
            '1% T20 (inc TPR)', 'TGT', 'P30', 'WFE +1%', 'PCNL +1%',
            'PCNI +1%', 'PCNH +1%', 'P26 +1%', 'T26 +1%', 'T30 +1%',
            'Additional shift (not IPC or HPC damage)'
        ]

def make_mock_data(d, nrows=94, ncols=18, value=1.0):
    mock_df = pd.DataFrame(np.full((nrows, ncols), value))
    mock_headers = pd.DataFrame([list(range(ncols))])
    row_headers = get_rows_to_keep(d).copy()
    while len(row_headers) < nrows:
        row_headers.append(f"Row{len(row_headers)}")
    mock_row_headers = pd.DataFrame(row_headers[:nrows])
    return mock_df, mock_headers, mock_row_headers

# -------------------
# Happy Path tests
# -------------------
class TestHappyPath:
    @pytest.mark.parametrize("d,expected_phase", [
        (0, "Cruise"),
        (1, "Take-off"),
        (2, "Climb"),
    ])
    @patch("pandas.read_excel")
    def test_valid_d_values_return_expected_phase_and_df(
        self, mock_read_excel, d, expected_phase
    ):
        mock_df, mock_headers, mock_row_headers = make_mock_data(d)
        mock_read_excel.side_effect = [mock_df, mock_headers, mock_row_headers]

        phase, df = Xrates_reader(d)

        assert phase == expected_phase
        assert isinstance(df, pd.DataFrame)
        assert (df.values == 100.0).all()
        assert df.shape[1] == 15 

    @patch("pandas.read_excel")
    def test_output_dataframe_shape_matches_rows_to_keep(
            self, mock_read_excel):
        mock_df, mock_headers, mock_row_headers = make_mock_data(0)
        mock_read_excel.side_effect = [mock_df, mock_headers, mock_row_headers]

        _, df = Xrates_reader(0)
        assert df.shape[0] == len(get_rows_to_keep(0))
        assert df.shape[1] == 15  # Updated from 14 to 15

# -------------------
# Edge Case tests
# -------------------
class TestEdgeCases:
    def test_invalid_d_index_raises_indexerror(self):
        with pytest.raises(IndexError):
            Xrates_reader(5)

    @patch("pandas.read_excel")
    def test_non_numeric_data_raises_valueerror_on_cast(self, mock_read_excel):
        mock_df, mock_headers, mock_row_headers = make_mock_data(0, value="x")
        mock_read_excel.side_effect = [mock_df, mock_headers, mock_row_headers]

        with pytest.raises(ValueError):
            Xrates_reader(0)

    @patch("pandas.read_excel")
    def test_file_not_found_raises_file_error(self, mock_read_excel):
        mock_read_excel.side_effect = FileNotFoundError("File not found")

        with pytest.raises(FileNotFoundError):
            Xrates_reader(0)
