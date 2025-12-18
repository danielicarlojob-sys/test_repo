import os
import pandas as pd
from pathlib import Path

import random
import matplotlib.pyplot as plt

# show full DataFrame when printing
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)
from src.utils.log_file import debug_info
# import numpy as np
from src.Loop_4_movavg_mod_v1 import Loop_4_movavg


class TestHappyPath:
    """
    Tests for correct behavior when valid input data is provided.
    """

    def test_basic_robust_moving_average(self):
        """
        Test that the robust moving average is calculated for a simple ESN.
        """
        df = pd.DataFrame({
            "ESN": [1] * 7,
            "reportdatetime": pd.date_range(start='2025-01-01', end='2025-01-31', periods=7),
            "NEW_FLAG": [1] * 7,
            "FlagSV": [0, 0, 0, 0, 0, 0, 0],
            "FlagSisChg": [0] * 7,
            "PS26__DEL_PC_E2E": [10, 10, 10, 10, 10, 10, 10],
            "T25__DEL_PC_E2E": [1, 2, 3, 4, 5, 6, 7],
        })

        df_out = Loop_4_movavg(
            df,
            flight_phase="cruise",
            WindowSemiWidth=1,
            DebugOption=0)

        # Output columns exist
        assert "PS26__DEL_PC_E2E_MAV_NO_STEPS" in df_out.columns
        assert "T25__DEL_PC_E2E_MAV_NO_STEPS" in df_out.columns

    def test_MVG(self):
        """
        Test moving average behaviour.
        """
        n = 50
        c1 = 15
        c2 = 38
        n_w = 4
        window = (n_w * 2) + 1
        E2E_cols = ['PS26__DEL_PC_E2E', 'T25__DEL_PC_E2E']
        E2E_MAV_cols = [col+'_MAV_NO_STEPS' for col in E2E_cols]
        start_date = pd.to_datetime("2025-01-01")
        end_date = pd.to_datetime("2025-05-01")
        date_list = pd.date_range(start=start_date, end=end_date, periods=n).strftime("%Y-%m-%d %H:%M:%S")
        # P26 = [round(random.random()*1000,2) for _ in range(n)]
        P26 = [random.randint(1,100) for _ in range(n)]

        T25 = [round(random.random()*1000,2) for _ in range(n)]
        df = pd.DataFrame({
            "ESN": [75001] * n,
            "reportdatetime": date_list,
            "NEW_FLAG": [1] * n,
            "FlagSV": [0] * c1 +[1] + [0] * (n-c1-1),
            "FlagSisChg": [0] * c2 +[1] + [0] * (n-c2-1),
            "PS26__DEL_PC_E2E": P26,
            "T25__DEL_PC_E2E": T25,
        })
        #df["PS26__DEL_PC_E2E_MAV_NO_STEPS"] = df["PS26__DEL_PC_E2E"].rolling(window, min_periods=window).mean()
        df = Loop_4_movavg(df,WindowSemiWidth=n_w, DebugOption=0)
        """ 
        df[E2E_MAV_cols] = (
                            df[E2E_cols]
                            .where((df['FlagSV'] == 0) & (df['FlagSisChg'] == 0))
                            .rolling(window, min_periods=window, center=False)
                            .mean()
                            .round(2)
                        )
        """
        plot = 1
        if plot:
            # plot data
            x = df["reportdatetime"]
            y1 = df["PS26__DEL_PC_E2E"]
            y2 = df["PS26__DEL_PC_E2E_MAV_NO_STEPS"]
            y3 = df["FlagSV"]
            y4 = df["FlagSisChg"]


            # Create figure and primary axis
            fig, ax1 = plt.subplots()

            # 6 Evenly spaced ticks
            xticks = x[::len(x)//10]
            # Plot y1 and y2 as lines on primary axis
            ax1.plot(x, y1, label='PS26__DEL_PC_E2E', color='blue', marker='o', markersize=1.5, linestyle='-', linewidth=0.2)
            ax1.plot(x, y2, label='PS26__DEL_PC_E2E_MAV_NO_STEPS', color='green', marker='o', markersize=4, linestyle='-')
            ax1.set_xlabel('X-axis')
            ax1.set_ylabel('Line Series')
            ax1.legend(loc='upper left')
            ax1.set_xticks(xticks)
            ax1.set_xticklabels(ax1.get_xticklabels(), rotation=60)
            # ax1.set_xlim(ax1.get_xlim()[::-1])
            # Create secondary axis sharing the same x-axis
            ax2 = ax1.twinx()

            # Plot y3 as bars on secondary axis
            ax2.bar(x, y3, label='FlagSV', color='red', alpha=0.5)
            ax2.bar(x, y4, label='FlagSisChg', color='yellow', alpha=0.5)

            ax2.set_ylabel('Bar Series')
            ax2.legend(loc='upper right')

            # Title and layout
            plt.title('Line and Bar Plot with Dual Y-Axis')
            plt.tight_layout()
            plt.show()
        
        start_idx = c1 + 1
        end_idx = start_idx + window
        idx_range = range(start_idx, end_idx)
        idx_vec = list(idx_range)
        print('idx_vec:\n', idx_vec)
        slice_df_PS26 = df.loc[idx_range,"PS26__DEL_PC_E2E"]
        CALCULATED_AVG = round(slice_df_PS26.sum()/slice_df_PS26.size, 2)
        # print('df:\n', df)
        print('df[["reportdatetime","FlagSV", "FlagSisChg", "PS26__DEL_PC_E2E", "PS26__DEL_PC_E2E_MAV_NO_STEPS"]]:\n', df[["reportdatetime","FlagSV", "FlagSisChg", "PS26__DEL_PC_E2E", "PS26__DEL_PC_E2E_MAV_NO_STEPS"]])
        print("idx_range: ", [start_idx,end_idx], "\nlen(idx_range):", len(idx_range))
        print('slice_df_PS26:\n', slice_df_PS26)
        print('slice_df_PS26.sum():\n', slice_df_PS26.sum())
        print('slice_df_PS26.size:\n', slice_df_PS26.size)
        print("---> CALCULATED AVG:\n",CALCULATED_AVG )
        print('---> slice_df_PS26.mean():\n', slice_df_PS26.mean().round(2))
        print('---> slice_df_PS26(16-25).mean():\n', df.loc[16:25,"PS26__DEL_PC_E2E"].mean().round(2))

        print('---> df.loc[end_idx,"PS26__DEL_PC_E2E_MAV_NO_STEPS"]:\n',df.loc[end_idx-1,"PS26__DEL_PC_E2E_MAV_NO_STEPS"])
        print('df.loc[:window-2,"PS26__DEL_PC_E2E_MAV_NO_STEPS"]: \n', df.loc[:window-2,"PS26__DEL_PC_E2E_MAV_NO_STEPS"])
        print('df.loc[:window-2,"PS26__DEL_PC_E2E_MAV_NO_STEPS"].to_list():\n', df.loc[:window-2,"PS26__DEL_PC_E2E_MAV_NO_STEPS"].to_list())
        df.loc[:window-2,"PS26__DEL_PC_E2E_MAV_NO_STEPS"].isna().all()
        assert df.loc[16:24, "PS26__DEL_PC_E2E"].mean().round(5) == df.loc[24, "PS26__DEL_PC_E2E_MAV_NO_STEPS"]
        print("end_idx-1: ", end_idx-1)
        assert "PS26__DEL_PC_E2E_MAV_NO_STEPS" in df.columns


class TestEdgeCases:
    """
    Tests for special or problematic inputs.
    """

    def test_old_rows_processed_if_in_same_esn(self):
        """
        Old rows (NEW_FLAG=0) may also get processed since Loop_4_movavg works
        at ESN level. We only require that MAV columns are computed consistently
        and that at least some NEW_FLAG=1 rows have numeric values.
        """
        df = pd.DataFrame({
            "ESN": [1, 1, 1, 1, 1, 1, 1, 1, 1],
            "reportdatetime": pd.date_range(start='2025-01-01', end='2025-01-31', periods=9),
            "NEW_FLAG": [0, 0, 0, 0, 0, 0, 1, 1, 1],
            "FlagSV": [0, 0, 0, 0, 0, 0, 0, 0, 0],
            "FlagSisChg": [0, 0, 0, 0, 0, 0, 0, 0, 0],

            # Required E2E columns
            "PS26__DEL_PC_E2E": [20, 10, 10, 20, 10, 10, 20, 10, 10],
            "T25__DEL_PC_E2E": [5, 15, 15, 5, 15, 15, 5, 15, 15],
            "P30__DEL_PC_E2E": [1, 1, 1, 1, 1, 1, 1, 1, 1],
            "T30__DEL_PC_E2E": [2, 2, 2, 2, 2, 2, 2, 2, 2],
            "TGTU__DEL_PC_E2E": [3, 3, 3, 3, 3, 3, 3, 3, 3],
            "NL__DEL_PC_E2E": [4, 4, 4, 4, 4, 4, 4, 4, 4],
            "NI__DEL_PC_E2E": [5, 5, 5, 5, 5, 5, 5, 5, 5],
            "NH__DEL_PC_E2E": [6, 6, 6, 6, 6, 6, 6, 6, 6],
            "FF__DEL_PC_E2E": [7, 7, 7, 7, 7, 7, 7, 7, 7],
            "P160__DEL_PC_E2E": [8, 8, 8, 8, 8, 8, 8, 8, 8],
        })
        cols_mock_df = df.columns
        non_parameters_cols = [col for col in cols_mock_df if "DEL" not in col]
        parameters_cols = [col for col in cols_mock_df if "DEL" in col]
        MAV_cols = [col+"_MAV_NO_STEPS" for col in parameters_cols]
        cols_to_print = non_parameters_cols + parameters_cols[:1] + MAV_cols[:1]

        # print(f"{debug_info()} ---> cols_mock_df:\n{cols_mock_df}")
        # print(f"{debug_info()} ---> non_parameters_cols:\n{non_parameters_cols}")
        # print(f"{debug_info()} ---> parameters_cols:\n{parameters_cols}")
        # print(f"{debug_info()} ---> MAV_cols:\n{MAV_cols}")

        df_out = Loop_4_movavg(
            df,
            flight_phase="cruise",
            WindowSemiWidth=1,
            DebugOption=0)
        print(f"{debug_info()} ---> df_out:\n{df_out[cols_to_print]}")
        # The first row (old row) can also get a computed value
        index_NANs = df_out[df_out["NEW_FLAG"] == 0].index
        index_not_NANs = df_out[df_out["NEW_FLAG"] == 1].index
        print(f"{debug_info()} df_out.loc[index_NANs, MAV_cols]:\n {df_out.loc[index_NANs, cols_to_print]}")
        print(f"{debug_info()} df_out.loc[index_not_NANs, MAV_cols]:\n {df_out.loc[index_not_NANs, cols_to_print]}")

        assert not pd.isna(df_out.loc[index_not_NANs, MAV_cols].all().all())
        assert pd.isna(df_out.loc[index_NANs, MAV_cols]).all().all()

        # At least one of the new rows should have numeric MAV values
        new_rows = df_out[df_out["NEW_FLAG"] == 1]
        ps26_mav_numeric = new_rows["PS26__DEL_PC_E2E_MAV_NO_STEPS"].dropna()
        t25_mav_numeric = new_rows["T25__DEL_PC_E2E_MAV_NO_STEPS"].dropna()
        print(f"{debug_info()} ---> ps26_mav_numeric:\n{ps26_mav_numeric}")

        assert len(ps26_mav_numeric) > 0
        assert len(t25_mav_numeric) > 0

    def test_empty_dataframe_returns_empty(self):
        """
        If input DataFrame is empty, output should be empty as well.
        """
        df = pd.DataFrame(columns=[
            "ESN", "reportdatetime", "NEW_FLAG", "FlagSV", "FlagSisChg",
            "PS26__DEL_PC_E2E", "T25__DEL_PC_E2E"
        ])
        df_out = Loop_4_movavg(
            df,
            flight_phase="cruise",
            WindowSemiWidth=1,
            DebugOption=0)
        assert df_out.empty


class TestDebugOption:
    """
    Tests related to the DebugOption flag that controls CSV saving.
    """

    def test_debugoption_creates_csv(self, tmp_path, monkeypatch):
        """
        DebugOption=1 should save a CSV file to Fleetstore_Data inside tmp_path.
        """
        monkeypatch.chdir(tmp_path)

        # Ensure directory exists to avoid OSError
        fleetstore_dir = os.path.join(tmp_path, "Fleetstore_Data")
        os.makedirs(fleetstore_dir, exist_ok=True)

        df = pd.DataFrame({
            "ESN": [1, 1],
            "reportdatetime": pd.date_range(start='2025-01-01', end='2025-01-31', periods=2),
            "NEW_FLAG": [1, 1],
            "FlagSV": [0, 0],
            "FlagSisChg": [0, 0],
            "PS26__DEL_PC_E2E": [10, 10],
            "T25__DEL_PC_E2E": [1, 1]
        })

        df_out = Loop_4_movavg(
            df,
            flight_phase="cruise",
            WindowSemiWidth=1,
            DebugOption=1)

        
        file_name = [fn for fn in os.listdir(fleetstore_dir) if "LOOP_4_cruise" in fn][0]
        saved_file_path = Path(fleetstore_dir) / file_name

        print(f"{debug_info()} ---> file_name:\n{file_name}")
        print(f"{debug_info()} ---> saved_file_path:\n{saved_file_path}")

        assert isinstance(df_out, pd.DataFrame)
        assert saved_file_path.exists()
