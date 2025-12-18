import pandas as pd
import pytest
from src.utils.log_file import debug_info
from src.utils.days_difference_v1 import days_difference
import random
pd.date_range(start='2025-01-01', end='2025-01-31', periods=7),

# --------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------
@pytest.fixture
def base_df():
    """Return a small DataFrame with ESN, ACID, ENGPOS, NEW_FLAG, and reportdatetime columns."""
    
    data = {
            "ESN": [75001, 75001, 75001],
            "ACID": ["A1", "A1", "A1"],
            "ENGPOS": [1, 1, 1],
            "NEW_FLAG": [1, 1, 1],
            "reportdatetime":pd.to_datetime(["2025-01-01 00:00:00", "2025-01-02 00:00:00", "2025-01-04 00:00:00" ]),
        }
    df = pd.DataFrame(data)

    return df
@pytest.fixture
def unsorted_df():
    """Return a small DataFrame with ESN, ACID, ENGPOS, NEW_FLAG, and reportdatetime columns."""
    
    data = {
            "ESN": [75001, 75001, 75001, 75001],
            "ACID": ["A1", "A1", "A1", "A1"],
            "ENGPOS": [1, 1, 1, 1],
            "NEW_FLAG": [1, 1, 1, 1],
            "reportdatetime":pd.to_datetime(["2025-01-05 00:00:00", "2025-01-01 00:00:00", "2025-01-03 00:00:00" , "2025-01-02 00:00:00" ]),
        }
    df = pd.DataFrame(data)

    return df

@pytest.fixture
def two_groups_unsorted_df():
    """Return a small DataFrame with ESN, ACID, ENGPOS, NEW_FLAG, and reportdatetime columns."""
    
    data = {
            "ESN": [75001, 75001, 75001, 75001] + [75002, 75002, 75002, 75002],
            "ACID": ["A1", "A1", "A1", "A1"] + ["A2", "A2", "A2", "A2"],
            "ENGPOS": [1, 1, 1, 1] + [2, 2, 2, 2],
            "NEW_FLAG": [1]*8,
            "reportdatetime":pd.to_datetime([   "2025-01-05 00:00:00", "2025-01-01 00:00:00", "2025-01-03 00:00:00" , "2025-01-02 00:00:00", 
                                                "2025-01-02 00:00:00", "2025-01-02 18:00:00", "2025-01-07 00:10:00", "2025-01-03 03:00:00",     
                                                ]),
        }
    df = pd.DataFrame(data)

    return df

class TestDaysDifference:
    def test_single_group_chronological_order(self, base_df):
        """Happy path: single ESN/ACID/ENGPOS, dates in correct order."""
        print(f"{debug_info()} ---> base_df:\n{base_df}")
        
        result = days_difference(base_df)
        print(f"{debug_info()} ---> result:\n{result}")
        assert list(result["days_since_prev"]) == [0, 1.0, 2.0]

    def test_single_group_unsorted_input(self, unsorted_df):
        """Function should sort by datetime before diffing."""
        print(f"{debug_info()} ---> unsorted_df:\n{unsorted_df}")
        result = days_difference(unsorted_df)
        print(f"{debug_info()} ---> result:\n{result}")

        # Expect 0 for first, 1 day, 2 days (chronological order enforced)
        assert list(result["days_since_prev"]) == [0, 1.0, 1.0, 2.0]

    def test_multiple_groups_independent_calculation(self, two_groups_unsorted_df):
        """Each (ESN, ACID, ENGPOS) group should be independent."""
        df = two_groups_unsorted_df

        print(f"{debug_info()} ---> df:\n{df}")
        result = days_difference(df)
        print(f"{debug_info()} ---> result:\n{result}")
        
        group1 = result[result["ESN"] == 75001]["days_since_prev"].tolist()
        group2 = result[result["ESN"] == 75002]["days_since_prev"].tolist()
        assert group1 == [0, 1.0, 1.0, 2.0]
        assert group2 == [0, 0.8, 0.4, 3.9]

    def test_same_day_entries(self):
        """Entries on the same day should yield 0-day difference."""
        df = pd.DataFrame({
            "ESN": [75001]*2,
            "ACID": ["A1"]*2,
            "ENGPOS": [1]*2,
            "NEW_FLAG": [1]*2,
            "reportdatetime": pd.to_datetime([  "2023-01-01 00:00:00",
                                                "2023-01-01 23:59:59"])
        })
        print(f"{debug_info()} ---> df:\n{df}")
        result = days_difference(df)
        print(f"{debug_info()} ---> result:\n{result}")
        assert list(result["days_since_prev"]) == [
            0, 1.0]  # 1 day after rounding

    def test_non_consecutive_days(self):
        """Large jumps in days should be correctly computed."""
        df = pd.DataFrame({
            "ESN": [1, 1],
            "ACID": ["A1", "A1"],
            "ENGPOS": [1, 1],
            "NEW_FLAG": [1, 1],
            "reportdatetime": pd.to_datetime([
                "2023-01-01 00:00:00",
                "2023-01-11 12:00:00",  # 10.5 days later
            ])
        })
        result = days_difference(df)
        assert pytest.approx(result.loc[1, "days_since_prev"], 0.1) == 10.5

    def test_empty_dataframe(self):
        """Edge case: empty DataFrame should return empty with added column."""
        df = pd.DataFrame(columns=["ESN", "ACID", "ENGPOS", "reportdatetime"])
        print(f"{debug_info()} ---> df:\n{df}")
        print(f"df.empty: {df.empty}")
        result = days_difference(df)
        print(f"{debug_info()} ---> result:\n{result}")
        print(f"result.empty: {result.empty}")

        assert "days_since_prev" in result.columns
        assert result.empty

    def test_single_row_group(self):
        """Group with only one row should get 0 as default."""
        df = pd.DataFrame({
            "ESN": [1],
            "ACID": ["A1"],
            "ENGPOS": [1],
            "NEW_FLAG": [1],
            "reportdatetime": pd.to_datetime(["2023-01-01 00:00:00"]),
        })
        result = days_difference(df)
        assert result.loc[0, "days_since_prev"] == 0.0
