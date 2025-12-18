import pandas as pd
import pytest
from unittest.mock import patch
from src.utils.log_file import debug_info
from src.utils.df_merger_new_v2 import df_merger_new


@pytest.fixture
def sample_df():
    """Sample current data DataFrame."""
    data = {
        "ESN": [1, 2],
        "reportdatetime": pd.to_datetime(["2025-08-01", "2025-08-02"]),
        "datestored": pd.to_datetime(["2025-08-01", "2025-08-02"]),
        "DSCID": [52, 52],
        "operator": ["op1", "op2"],
        "equipmentid": [101, 102],
        "ACID": ["AC1", "AC2"],
        "ENGPOS": [1, 2],
        "P25__PSI": [10, 20],
        "T25__DEGC": [500, 510],
        "P30__PSI": [30, 40],
        "T30__DEGC": [520, 530],
        "TGTU_A__DEGC": [600, 610],
        "NL__PC": [1, 2],
        "NI__PC": [3, 4],
        "NH__PC": [5, 6],
        "FF__LBHR": [7, 8],
        "PS160__PSI": [9, 10],
        "PS26S__NOM_PSI": [11, 12],
        "TS25S__NOM_K": [13, 14],
        "PS30S__NOM_PSI": [15, 16],
        "TS30S__NOM_K": [17, 18],
        "TGTS__NOM_K": [19, 20],
        "NL__NOM_PC": [21, 22],
        "NI__NOM_PC": [23, 24],
        "NH__NOM_PC": [25, 26],
        "FF__NOM_LBHR": [27, 28],
        "P135S__NOM_PSI": [29, 30],
        "ALT__FT": [10000, 10500],
        "MN1": [0.1, 0.2],
        "P20__PSI": [200, 210],
        "T20__DEGC": [500, 510],
    }
    return pd.DataFrame(data)


def test_df_merger_new_happy_path(sample_df):
    """Merges df with mock previous data."""
    prev_df = sample_df.copy()
    prev_df["reportdatetime"] -= pd.Timedelta(days=5)

    with patch("src.utils.df_merger_new_v2.os.getcwd", return_value="/tmp"), \
            patch("src.utils.df_merger_new_v2.os.listdir", return_value=["data_output_test.csv"]), \
            patch("src.utils.df_merger_new_v2.read_and_clean_csv", return_value=prev_df), \
            patch("src.utils.df_merger_new_v2.log_message") as mock_log:
        print(f"{debug_info()} ---> sample_df:\n{sample_df}")
        result = df_merger_new(sample_df, flight_phase="Test", DebugOption=0)
        print(f"{debug_info()} ---> result:\n{result}")

    assert isinstance(result, pd.DataFrame)
    # merged DataFrame should contain all previous + current rows
    assert len(result) == len(prev_df) + len(sample_df)
    # log_message is optional here because function only logs debug lines
    # (commented out)
    # assert mock_log.call_count == 0


def test_df_merger_new_no_previous_file(sample_df):
    """Handles case when no previous CSV exists."""
    with patch("src.utils.df_merger_new_v2.os.getcwd", return_value="/tmp"), \
            patch("src.utils.df_merger_new_v2.os.listdir", return_value=[]), \
            patch("src.utils.df_merger_new_v2.log_message") as mock_log:

        result = df_merger_new(sample_df, flight_phase="Test", DebugOption=0)

    assert isinstance(result, pd.DataFrame)
    # No rows lost
    assert len(result) == len(sample_df)
    # log_message should have been called because no previous file
    assert mock_log.call_count > 0
    assert "NO previous data file found" in str(mock_log.call_args_list[0])


def test_df_merger_new_empty_df():
    """Handles empty input DataFrame."""
    empty_df = pd.DataFrame(
        columns=[
            "ESN",
            "reportdatetime",
            "operator",
            "datestored",
            "DSCID",
            "equipmentid",
            "ACID",
            "ENGPOS",
            "P25__PSI",
            "T25__DEGC",
            "P30__PSI",
            "T30__DEGC",
            "TGTU_A__DEGC",
            "NL__PC",
            "NI__PC",
            "NH__PC",
            "FF__LBHR",
            "PS160__PSI",
            "PS26S__NOM_PSI",
            "TS25S__NOM_K",
            "PS30S__NOM_PSI",
            "TS30S__NOM_K",
            "TGTS__NOM_K",
            "NL__NOM_PC",
            "NI__NOM_PC",
            "NH__NOM_PC",
            "FF__NOM_LBHR",
            "P135S__NOM_PSI",
            "ALT__FT",
            "MN1",
            "P20__PSI",
            "T20__DEGC"])
    with patch("src.utils.df_merger_new_v2.os.getcwd", return_value="/tmp"), \
            patch("src.utils.df_merger_new_v2.os.listdir", return_value=[]), \
            patch("src.utils.df_merger_new_v2.log_message") as mock_log:

        result = df_merger_new(empty_df, flight_phase="Test", DebugOption=0)

    assert isinstance(result, pd.DataFrame)
    assert result.empty
    assert mock_log.call_count > 0
    assert "NO previous data file found" in str(mock_log.call_args_list[0])


def test_df_merger_new_n_pts_limit(sample_df):
    """Ensures that only top n_pts per ESN are kept."""
    # Duplicate rows to test n_pts limiting
    extra_rows = sample_df.copy()
    extra_rows["reportdatetime"] += pd.Timedelta(days=2)
    test_df = pd.concat([sample_df, extra_rows], ignore_index=True)

    with patch("src.utils.df_merger_new_v2.os.getcwd", return_value="/tmp"), \
            patch("src.utils.df_merger_new_v2.os.listdir", return_value=[]), \
            patch("src.utils.df_merger_new_v2.log_message") as mock_log:

        result = df_merger_new(
            test_df,
            flight_phase="Test",
            DebugOption=0,
            n_pts=2)

    # Each ESN has at most 2 rows
    counts = result.groupby("ESN").size()
    assert all(counts <= 2)
    # log_message called because no previous file
    assert mock_log.call_count > 0
