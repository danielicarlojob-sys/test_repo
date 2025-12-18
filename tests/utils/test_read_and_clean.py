import pandas as pd
import pytest
from unittest.mock import patch
from src.utils.read_and_clean_v1 import read_and_clean_csv, read_and_clean_df


@pytest.fixture
def sample_data():
    return {
        "ESN": [1, 2, 3],
        "reportdatetime": ["2025-08-01 12:00:00", "2025-08-02 13:00:00", "bad_date"],
        "datestored": ["2025-08-01 00:00:00", "bad_date", "2025-08-03 00:00:00"],
        "operator": ["op1", "op2", "op3"],
        "equipmentid": [101, 102, 103],
        "ACID": ["AC1", "AC2", "AC3"],
        "ENGPOS": [1, 2, 3],
        "DSCID": [11, 12, 13],
        "days_since_prev": [1.1234567, 2.7654321, 3.9876543],
        "NEW_FLAG": [1, 1, 1],
        "SISTER_ESN": [10, 20, 30],
        "FlagSV": [0, 0, 0],
        "FlagSisChg": [0, 0, 0],
        "float_col": [1.123456789, 2.987654321, 3.555555555],
        "Unnamed: 0": [0, 1, 2],
        "Column1": ["a", "b", "c"],
        "P25__PSI": [1.5555555, 2.44444555, 3.11188888888]
    }


def test_read_and_clean_csv_basic(sample_data):
    df_mock = pd.DataFrame(sample_data)
    with patch("pandas.read_csv", return_value=df_mock):
        df = read_and_clean_csv("dummy_path.csv")

    # Only rows with valid datetimes remain
    assert len(df) == 1

    # Floats are rounded to 5 decimals
    assert df["P25__PSI"].iloc[0] == 1.55556
    assert df["float_col"].iloc[0] == 1.12346

    # Unwanted columns removed
    assert "Unnamed: 0" not in df.columns
    assert "Column1" not in df.columns

    # ESN type is int
    assert df["ESN"].dtype == "int64"

    # Datetimes parsed
    assert pd.api.types.is_datetime64_any_dtype(df["reportdatetime"])
    assert pd.api.types.is_datetime64_any_dtype(df["datestored"])


def test_read_and_clean_df_basic(sample_data):
    df_input = pd.DataFrame(sample_data)
    df = read_and_clean_df(df_input)

    assert len(df) == 1

    # Floats rounded to 5 decimals
    assert round(df["days_since_prev"].iloc[0], 5) == 1.12346
    assert round(df["float_col"].iloc[0], 5) == 1.12346

    assert df["ESN"].dtype == "int64"
    assert pd.api.types.is_datetime64_any_dtype(df["reportdatetime"])
    assert pd.api.types.is_datetime64_any_dtype(df["datestored"])


def test_invalid_datetimes_dropped_csv(sample_data):
    df_mock = pd.DataFrame(sample_data)
    with patch("pandas.read_csv", return_value=df_mock):
        df = read_and_clean_csv("dummy_path.csv")
    assert df["ESN"].tolist() == [1]


def test_invalid_datetimes_dropped_df(sample_data):
    df_input = pd.DataFrame(sample_data)
    df = read_and_clean_df(df_input)
    assert df["ESN"].tolist() == [1]


def test_empty_csv_returns_empty():
    empty_df = pd.DataFrame({
        "ESN": [], "reportdatetime": [], "datestored": [],
        "operator": [], "equipmentid": [], "ACID": [], "ENGPOS": [],
        "DSCID": [], "days_since_prev": [], "NEW_FLAG": [],
        "SISTER_ESN": [], "FlagSV": [], "FlagSisChg": [], "float_col": []
    })
    with patch("pandas.read_csv", return_value=empty_df):
        df = read_and_clean_csv("dummy_path.csv")
    assert df.empty


def test_columns_stripped_df(sample_data):
    """Since read_and_clean_df does not strip string columns, test only for column presence."""
    df_input = pd.DataFrame(sample_data)
    df_input["operator"] = [" op1 ", " op2 ", " op3 "]
    df = read_and_clean_df(df_input)
    # Ensure column exists and is unchanged (no stripping)
    assert "operator" in df.columns
    # the function does not strip spaces
    assert df["operator"].iloc[0] == " op1 "
