import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import Dict, Tuple
from src.utils.log_file import log_message, f_lineno as line

def find_next_event(grouped, key, takeoff_time, max_delta):
    """Find the next event timestamp and row_sum after takeoff_time within max_delta."""
    if key not in grouped.groups:
        return None, None
    group = grouped.get_group(key).sort_values("reportdatetime")
    next_time = group.loc[group["reportdatetime"] > takeoff_time, "reportdatetime"].min()
    if pd.isna(next_time):
        return None, None
    if (next_time - takeoff_time) < max_delta:
        row = group.loc[group["reportdatetime"] == next_time].iloc[0]
        return row["reportdatetime"], row["row_sum"]
    return None, None

def find_previous_event(grouped, key, cruise_time, max_delta):
    """Find previous event timestamp and row_sum before cruise_time within max_delta."""
    if key not in grouped.groups:
        return None, None
    group = grouped.get_group(key).sort_values("reportdatetime")
    prev_time = group.loc[group["reportdatetime"] < cruise_time, "reportdatetime"].max()
    if pd.isna(prev_time):
        return None, None
    if (cruise_time - prev_time) < max_delta:
        row = group.loc[group["reportdatetime"] == prev_time].iloc[0]
        return row["reportdatetime"], row["row_sum"]
    return None, None

def merge_flight_phases(
    df_takeoff: pd.DataFrame,
    df_climb: pd.DataFrame,
    df_cruise: pd.DataFrame,
) -> pd.DataFrame:
    """Merge takeoff, climb, and cruise flight phases into a single DataFrame.

    Groups rows by (ESN, operator, ACID, ENGPOS), then for each takeoff event:
    - Finds the corresponding climb event within 30 minutes after takeoff.
    - Finds the corresponding cruise event within 55 minutes after takeoff.

    Args:
        df_takeoff (pd.DataFrame): Takeoff phase records with columns:
            ['ESN','operator','ACID','ENGPOS','reportdatetime','row_sum'].
        df_climb (pd.DataFrame): Climb phase records, same columns.
        df_cruise (pd.DataFrame): Cruise phase records, same columns.

    Returns:
        pd.DataFrame: Merged DataFrame with columns:
            ['ESN','operator','ACID','ENGPOS',
             'reportdatetime_takeoff','reportdatetime_climb','reportdatetime_cruise',
             'row_sum_takeoff','row_sum_climb','row_sum_cruise']
    """
    # Group by keys of interest
    keys_to_group = ["ESN", "operator", "ACID", "ENGPOS"]
    grouped_takeoff = df_takeoff.groupby(keys_to_group, group_keys=False)
    grouped_climb = df_climb.groupby(keys_to_group, group_keys=False)
    grouped_cruise = df_cruise.groupby(keys_to_group, group_keys=False)

    rows = []  # collect dicts here instead of pre-building DataFrame


    # Iterate over takeoff groups
    for key, group_tko in tqdm(grouped_takeoff, desc="Merging flight phases", unit="installation level"):
        group_tko = group_tko.sort_values("reportdatetime")
        for _, row_tko in group_tko.iterrows():
            takeoff_time = row_tko["reportdatetime"]

            climb_time, climb_sum = find_next_event(grouped_climb, key, takeoff_time, pd.Timedelta(minutes=30))
            cruise_time, cruise_sum = find_next_event(grouped_cruise, key, takeoff_time, pd.Timedelta(minutes=55))

            rows.append({
                "ESN": row_tko["ESN"],
                "operator": row_tko["operator"],
                "ACID": row_tko["ACID"],
                "ENGPOS": row_tko["ENGPOS"],
                "reportdatetime_takeoff": takeoff_time,
                "reportdatetime_climb": climb_time,
                "reportdatetime_cruise": cruise_time,
                "row_sum_takeoff": row_tko["row_sum"],
                "row_sum_climb": climb_sum,
                "row_sum_cruise": cruise_sum,
            })
    merged_df = pd.DataFrame(rows)
    
    return merged_df



def merged_data_evaluation(
    merged_df: pd.DataFrame,
    threshold: int
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Evaluate merged flight-phase DataFrame and produce a summary of DN_FIRE events.

    This function annotates the input `merged_df` with:
      - a boolean mask of whether all three row_sum columns meet or exceed `threshold`
      - a combined `merge_sum` of those row_sums when the mask is True
      - a DN_FIRE flag ("YES"/"NO")
      - a DN_FIRE_TIME string formatted as '%Y-%m-%d %H:%M:%S'

    It then groups the annotated DataFrame by the flight key columns (ESN, operator, ACID, ENGPOS)
    and for each group builds a summary row capturing:
      - whether any DN_FIRE occurred in that group
      - how many DN_FIRE occurrences there were
      - the timestamp of the first DN_FIRE
      - the timestamp of the last DN_FIRE

    Parameters
    ----------
    merged_df : pd.DataFrame
        DataFrame produced by `merge_flight_phases()`. Must contain columns:
          ['ESN',
           'operator',
           'ACID',
           'ENGPOS',
           'reportdatetime_takeoff',
           'row_sum_takeoff',
           'row_sum_climb',
           'row_sum_cruise']
    threshold : int
        Minimum required value for each of the row_sum columns to qualify as a DN_FIRE event.

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame]
        - annotated_df : pd.DataFrame
            The original `merged_df` augmented with:
              ['merge_sum', 'DN_FIRE', 'DN_FIRE_TIME'].
        - summary_df : pd.DataFrame
            One summary row per (ESN, operator, ACID, ENGPOS) capturing:
              ['operator',
               'ESN',
               'ACID',
               'ENGPOS',
               'DN_FIRED',
               'DN_FIRES',
               'First DN fire',
               'Last DN fire'].

    Notes
    -----
    - DN_FIRE_TIME is formatted as a string for downstream reporting.
    - Any exceptions during processing are logged via `log_message()`.
    """
    summary_rows = []

    try:
        # Step 1: Identify rows where all three phases exceed the threshold
        mask = (
            (merged_df["row_sum_takeoff"] >= threshold) &
            (merged_df["row_sum_climb"] >= threshold) &
            (merged_df["row_sum_cruise"] >= threshold)
        )

        # Step 2: Compute total row_sum only for valid DN_FIRE rows
        merged_df["merge_sum"] = np.where(
            mask,
            merged_df["row_sum_takeoff"]
            + merged_df["row_sum_climb"]
            + merged_df["row_sum_cruise"],
            np.nan
        )

        # Step 3: Flag each row as DN_FIRE = "YES" or "NO"
        merged_df["DN_FIRE"] = np.where(mask, "YES", "NO")

        # Step 4: Capture the takeoff timestamp when DN_FIRE is True
        merged_df["DN_FIRE_TIME"] = np.where(
            mask,
            merged_df["reportdatetime_takeoff"],
            pd.NaT
        )

        # Ensure DN_FIRE_TIME is a datetime and then format as string
        merged_df["DN_FIRE_TIME"] = pd.to_datetime(
            merged_df["DN_FIRE_TIME"], errors="coerce"
        ).dt.strftime("%Y-%m-%d %H:%M:%S")

        # Step 5: Build summary per flight key
        key_cols = ["ESN", "operator", "ACID", "ENGPOS"]
        grouped = merged_df.groupby(key_cols, group_keys=False)

        for _, group in tqdm(
            grouped,
            desc="DN output reordering",
            unit="installation level"
        ):
            # Sort events by takeoff timestamp
            group = group.sort_values("reportdatetime_takeoff")

            # Extract the unique flight identifiers (one per group)
            esn     = group["ESN"].iat[0]
            operator= group["operator"].iat[0]
            acid    = group["ACID"].iat[0]
            engpos  = group["ENGPOS"].iat[0]

            # Filter only DN_FIRE == "YES" rows within this group
            dn_events = group.loc[group["DN_FIRE"] == "YES", :]
            count_dn  = len(dn_events)
            fired_flag = "YES" if count_dn > 0 else "NO"

            # Determine first and last DN_FIRE takeoff timestamps
            first_fire_ts = (
                dn_events["reportdatetime_takeoff"].min()
                if fired_flag == "YES"
                else pd.NaT
            )
            last_fire_ts = (
                dn_events["reportdatetime_takeoff"].max()
                if fired_flag == "YES"
                else pd.NaT
            )

            # Format timestamps or set None if missing
            first_fire_str = (
                first_fire_ts.strftime("%Y-%m-%d %H:%M:%S")
                if pd.notna(first_fire_ts)
                else None
            )
            last_fire_str = (
                last_fire_ts.strftime("%Y-%m-%d %H:%M:%S")
                if pd.notna(last_fire_ts)
                else None
            )

            summary_rows.append({
                "operator": operator,
                "ESN": esn,
                "ACID": acid,
                "ENGPOS": engpos,
                "DN_FIRED": fired_flag,
                "DN_FIRES": count_dn,
                "First DN fire": first_fire_str,
                "Last DN fire": last_fire_str
            })

        summary_df = pd.DataFrame(summary_rows)

    except Exception as exc:
        # Log any exception with function context and re-raise if needed
        log_message(f"Error in merged_data_evaluation: {exc}")
        raise

    return merged_df, summary_df




 