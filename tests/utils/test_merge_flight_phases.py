import pytest
import pandas as pd
import numpy as np
from src.utils.log_file import debug_info
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', None)     # Show all rows
pd.set_option('display.max_colwidth', None) # Show full column width
pd.set_option('display.width', 0)           # Auto-adjust display width
from datetime import datetime, timedelta
# Adjust import path as needed
from src.utils.merge_flight_phases_v1 import merge_flight_phases, merged_data_evaluation

class TestMergeFlightPhasesHappyPath:
    """Tests for normal, expected input data."""

    @pytest.fixture
    def sample_data(self):
        # Base timestamps
        base_time = datetime(2025, 1, 1, 12, 0)
        df_takeoff = pd.DataFrame({
            "ESN": [1001],
            "operator": ["OpA"],
            "ACID": ["AC123"],
            "ENGPOS": [1],
            "reportdatetime": [base_time],
            "row_sum": [10]
        })
        df_climb = pd.DataFrame({
            "ESN": [1001],
            "operator": ["OpA"],
            "ACID": ["AC123"],
            "ENGPOS": [1],
            "reportdatetime": [base_time + timedelta(minutes=29.9)],
            "row_sum": [20]
        })
        df_cruise = pd.DataFrame({
            "ESN": [1001],
            "operator": ["OpA"],
            "ACID": ["AC123"],
            "ENGPOS": [1],
            "reportdatetime": [base_time + timedelta(minutes=54.9)],
            "row_sum": [30]
        })
        return df_takeoff, df_climb, df_cruise

    def test_merge_happy_path(self, sample_data):
        df_takeoff, df_climb, df_cruise = sample_data
        result = merge_flight_phases(df_takeoff, df_climb, df_cruise)
        assert result.shape[0] == 1
        assert result["row_sum_takeoff"].iloc[0] == 10
        assert result["row_sum_climb"].iloc[0] == 20
        assert result["row_sum_cruise"].iloc[0] == 30
        assert result["reportdatetime_climb"].iloc[0] > result["reportdatetime_takeoff"].iloc[0]
        assert result["reportdatetime_cruise"].iloc[0] > result["reportdatetime_takeoff"].iloc[0]

class TestMergeFlightPhasesEdgeCases:
    """Tests for edge cases like missing phases, no matching keys, or timeouts."""
    @pytest.mark.skip(reason="not implemented yet")
    def test_no_climb_cruise(self):
        """Climb and cruise data missing -> should return None for those columns"""
        base_time = datetime(2025, 1, 1, 12, 0)
        df_takeoff = pd.DataFrame({
            "ESN": [1001],
            "operator": ["OpA"],
            "ACID": ["AC123"],
            "ENGPOS": [1],
            "reportdatetime": [base_time],
            "row_sum": [10]
        })
        df_climb = pd.DataFrame(columns=df_takeoff.columns)
        df_cruise = pd.DataFrame(columns=df_takeoff.columns)
        print(f"{debug_info()} ---> df_takeoff:\n{df_takeoff}")
        
        print(f"{debug_info()} ---> df_climb:\n{df_climb}")

        print(f"{debug_info()} ---> df_cruise:\n{df_cruise}")

        result = merge_flight_phases(df_takeoff, df_climb, df_cruise)
        print(f"{debug_info()} ---> result:\n{result}")
        
        assert result["reportdatetime_climb"].iloc[0] is None
        assert result["reportdatetime_cruise"].iloc[0] is None

    def test_time_exceeds_limit(self):
        """Events beyond max_delta should not match"""
        base_time = datetime(2025, 1, 1, 12, 0)
        df_takeoff = pd.DataFrame({
            "ESN": [1001],
            "operator": ["OpA"],
            "ACID": ["AC123"],
            "ENGPOS": [1],
            "reportdatetime": [base_time],
            "row_sum": [10]
        })
        df_climb = pd.DataFrame({
            "ESN": [1001],
            "operator": ["OpA"],
            "ACID": ["AC123"],
            "ENGPOS": [1],
            "reportdatetime": [base_time + timedelta(hours=2)],  # exceeds 1.5h
            "row_sum": [20]
        })
        df_cruise = pd.DataFrame({
            "ESN": [1001],
            "operator": ["OpA"],
            "ACID": ["AC123"],
            "ENGPOS": [1],
            "reportdatetime": [base_time + timedelta(hours=5)],  # exceeds 4h
            "row_sum": [30]
        })
        result = merge_flight_phases(df_takeoff, df_climb, df_cruise)
        print(f"{debug_info()} ---> result:\n{result}")
        assert result["reportdatetime_takeoff"].iloc[0] is None
        assert result["reportdatetime_climb"].iloc[0] is None
        assert result["reportdatetime_cruise"].iloc[0] == pd.to_datetime("2025-01-01 17:00:00")

    def test_multiple_takeoffs(self):
        """Multiple takeoff events should each match the correct next events"""
        base_time = datetime(2025, 1, 1, 12, 0)
        df_takeoff = pd.DataFrame({
            "ESN": [1001, 1001],
            "operator": ["OpA", "OpA"],
            "ACID": ["AC123", "AC123"],
            "ENGPOS": [1, 1],
            "reportdatetime": [base_time, base_time + timedelta(hours=4)],
            "row_sum": [10, 15]
        })
        df_climb = pd.DataFrame({
            "ESN": [1001, 1001],
            "operator": ["OpA", "OpA"],
            "ACID": ["AC123", "AC123"],
            "ENGPOS": [1, 1],
            "reportdatetime": [base_time + timedelta(minutes=29),
                               base_time + timedelta(hours=4, minutes=29)],
            "row_sum": [20, 25]
        })
        df_cruise = pd.DataFrame({
            "ESN": [1001, 1001],
            "operator": ["OpA", "OpA"],
            "ACID": ["AC123", "AC123"],
            "ENGPOS": [1, 1],
            "reportdatetime": [base_time + timedelta(minutes=54),
                               base_time + timedelta(hours=4, minutes=54)],
            "row_sum": [30, 35]
        })
        result = merge_flight_phases(df_takeoff, df_climb, df_cruise)
        assert result.shape[0] == 2
        assert result["row_sum_takeoff"].tolist() == [10, 15]
        assert result["row_sum_climb"].tolist() == [20, 25]
        assert result["row_sum_cruise"].tolist() == [30, 35]

    def test_no_matching_keys(self):
        """Climb/cruise have different keys -> should return None for them"""
        base_time = datetime(2025, 1, 1, 12, 0)
        df_takeoff = pd.DataFrame({
            "ESN": [1001],
            "operator": ["OpA"],
            "ACID": ["AC123"],
            "ENGPOS": [1],
            "reportdatetime": [base_time],
            "row_sum": [10]
        })
        df_climb = pd.DataFrame({
            "ESN": [2002],  # different ESN
            "operator": ["OpB"],
            "ACID": ["AC999"],
            "ENGPOS": [2],
            "reportdatetime": [base_time + timedelta(minutes=30)],
            "row_sum": [20]
        })
        df_cruise = df_climb.copy()
        df_cruise.loc[0,"reportdatetime"] =base_time + timedelta(minutes=40)
        result = merge_flight_phases(df_takeoff, df_climb, df_cruise)
        print(f"{debug_info()} ---> result:\n{result}")
        assert result["reportdatetime_takeoff"].iloc[0] is None
        assert result["reportdatetime_climb"].iloc[0] == pd.to_datetime("2025-01-01 12:30:00")
        
        assert result["reportdatetime_cruise"].iloc[0] != pd.to_datetime("2025-01-01 12:40:00")

class TestMergedDataEvaluationHappyPath:
    """Happy-path tests for merged_data_evaluation."""

    @pytest.fixture
    def sample_df(self):
        # Construct a merged_df with two groups and multiple takeoff rows
        data = [
            # Group 1: ESN=75001, operator=OpA, ACID=A1, ENGPOS=1
            {"ESN": 75001, "operator": "OpA", "ACID": "A1", "ENGPOS": 1,
             "reportdatetime_takeoff": pd.Timestamp("2025-01-01 08:00:00"),
             "row_sum_takeoff": 10, "row_sum_climb": 20, "row_sum_cruise": 30},
            {"ESN": 75001, "operator": "OpA", "ACID": "A1", "ENGPOS": 1,
             "reportdatetime_takeoff": pd.Timestamp("2025-01-01 09:00:00"),
             "row_sum_takeoff": 15, "row_sum_climb": 25, "row_sum_cruise": 35},
            # Group 2: ESN=75002, operator=OpB, ACID=B2, ENGPOS=2
            {"ESN": 75002, "operator": "OpB", "ACID": "B2", "ENGPOS": 2,
             "reportdatetime_takeoff": pd.Timestamp("2025-01-02 10:00:00"),
             "row_sum_takeoff": 5, "row_sum_climb":  5, "row_sum_cruise":  5},
            {"ESN": 75002, "operator": "OpB", "ACID": "B2", "ENGPOS": 2,
             "reportdatetime_takeoff": pd.Timestamp("2025-01-02 11:00:00"),
             "row_sum_takeoff": 50, "row_sum_climb": 60, "row_sum_cruise": 70},
        ]
        return pd.DataFrame(data)

    def test_all_phases_above_threshold(self, sample_df):
        # threshold = 10: rows 0,1,3 qualify; row 2 does not
        annotated_df, summary_df = merged_data_evaluation(sample_df.copy(), threshold=10)

        # Check annotated_df columns
        assert "merge_sum" in annotated_df
        assert "DN_FIRE" in annotated_df
        assert "DN_FIRE_TIME" in annotated_df

        # Row‐wise expectations
        # Row 0: sum=10+20+30=60, DN_FIRE=YES
        assert annotated_df.loc[0, "merge_sum"] == 60
        assert annotated_df.loc[0, "DN_FIRE"] == "YES"
        assert annotated_df.loc[0, "DN_FIRE_TIME"] == "2025-01-01 08:00:00"

        # Row 2: sum=5+5+5 but below threshold -> NaN, NO, "NaT"
        assert np.isnan(annotated_df.loc[2, "merge_sum"])
        assert annotated_df.loc[2, "DN_FIRE"] == "NO"
        assert np.isnan(annotated_df.loc[2, "DN_FIRE_TIME"])

        # summary_df should have exactly two rows (one per group)
        assert len(summary_df) == 2

        # Group 75001 summary: two YES events, first at 08:00, last at 09:00
        g1 = summary_df.query("ESN==75001 and operator=='OpA'")
        assert g1["DN_FIRED"].iloc[0] == "YES"
        assert g1["DN_FIRES"].iloc[0] == 2
        assert g1["First DN fire"].iloc[0] == "2025-01-01 08:00:00"
        assert g1["Last DN fire"].iloc[0] == "2025-01-01 09:00:00"

        # Group 75002 summary: one YES event (the 11:00 row)
        g2 = summary_df.query("ESN==75002 and operator=='OpB'")
        assert g2["DN_FIRED"].iloc[0] == "YES"
        assert g2["DN_FIRES"].iloc[0] == 1
        assert g2["First DN fire"].iloc[0] == "2025-01-02 11:00:00"
        assert g2["Last DN fire"].iloc[0] == "2025-01-02 11:00:00"

    def test_mixed_thresholds(self, sample_df):
        
        annotated_df, summary_df = merged_data_evaluation(sample_df.copy(), threshold=15)
        print(f"annotated_df:\n{annotated_df}")
        print(f"summary_df:\n{summary_df}")
        
        # Row 0 - below threshold 
        assert np.isnan(annotated_df.loc[0, "merge_sum"])
        assert annotated_df.loc[0, "DN_FIRE"] == "NO"
        assert np.isnan(annotated_df.loc[0, "DN_FIRE_TIME"])

        # Row 1 - above threshold 
        assert annotated_df.loc[1, "merge_sum"] == 75
        assert annotated_df.loc[1, "DN_FIRE"] == "YES"
        assert annotated_df.loc[1, "DN_FIRE_TIME"] == "2025-01-01 09:00:00"

        # summary for 75001: both event above 15 -> DN_FIRED=YES, DN_FIRES=1
        g1 = summary_df.query("ESN==75001")
        print(f"line 244 ---> g1:\n{g1}")
        assert g1["DN_FIRED"].iloc[0] == "YES"
        assert g1["DN_FIRES"].iloc[0] == 1
        assert g1["First DN fire"].iloc[0] == '2025-01-01 09:00:00'
        assert g1["Last DN fire"].iloc[0] == '2025-01-01 09:00:00'

        # summary for 75002: one event above threshold
        g2 = summary_df.query("ESN==75002")
        assert g2["DN_FIRED"].iloc[0] == "YES"
        assert g2["DN_FIRES"].iloc[0] == 1

class TestMergedDataEvaluationEdgeCases:
    """Edge‐case tests for merged_data_evaluation."""

    def test_empty_input(self):
        empty_df = pd.DataFrame(columns=[
            "ESN", "operator", "ACID", "ENGPOS",
            "reportdatetime_takeoff",
            "row_sum_takeoff", "row_sum_climb", "row_sum_cruise"
        ])
        annotated_df, summary_df = merged_data_evaluation(empty_df, threshold=1)

        # Both outputs should be empty
        assert annotated_df.empty
        assert summary_df.empty

    def test_high_threshold_no_events(self):
        # one group, all sums below a very high threshold
        df = pd.DataFrame([{
            "ESN": "X", "operator": "OpX", "ACID": "AX", "ENGPOS": 1,
            "reportdatetime_takeoff": pd.Timestamp("2025-01-05 05:00:00"),
            "row_sum_takeoff": 1, "row_sum_climb": 1, "row_sum_cruise": 1
        }])
        annotated_df, summary_df = merged_data_evaluation(df, threshold=999)

        # annotated_df: DN_FIRE all NO, merge_sum NaN, DN_FIRE_TIME="NaT"
        assert annotated_df["DN_FIRE"].iloc[0] == "NO"
        assert np.isnan(annotated_df["merge_sum"].iloc[0])
        assert np.isnan(annotated_df["DN_FIRE_TIME"].iloc[0])

        # summary_df: single row flagged NO, zero fires, no timestamps
        assert len(summary_df) == 1
        row = summary_df.iloc[0]
        assert row["DN_FIRED"] == "NO"
        assert row["DN_FIRES"] == 0
        assert pd.isna(row["First DN fire"])
        assert pd.isna(row["Last DN fire"])

    def test_single_row_group(self):
        # one row exactly at threshold
        df = pd.DataFrame([{
            "ESN": 1, "operator": "OpY", "ACID": "AY", "ENGPOS": 2,
            "reportdatetime_takeoff": pd.Timestamp("2025-01-10 10:10:10"),
            "row_sum_takeoff": 5, "row_sum_climb": 5, "row_sum_cruise": 5
        }])
        annotated_df, summary_df = merged_data_evaluation(df, threshold=5)

        # exactly 5+5+5 = 15 qualifies
        assert annotated_df["DN_FIRE"].iloc[0] == "YES"
        assert annotated_df["merge_sum"].iloc[0] == 15

        # summary: one fire, first and last equal
        assert summary_df["DN_FIRED"].iloc[0] == "YES"
        assert summary_df["DN_FIRES"].iloc[0] == 1
        ts = "2025-01-10 10:10:10"
        assert summary_df["First DN fire"].iloc[0] == ts
        assert summary_df["Last DN fire"].iloc[0] == ts

