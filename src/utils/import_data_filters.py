import pandas as pd
import numpy as np


def drop_nans(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans DataFrame removing NaN and -5555

    Parameters
    ----------
    Args:
        - df (pd.DataFrame): input DataFrame to be filtered for NaN and -5555

    Returns
        - df (pd.DataFrame): DataFrame filtered
    """
    Param = [
        'P25__PSI',
        'T25__DEGC',
        'P30__PSI',
        'T30__DEGC',
        'TGTU_A__DEGC',
        'NL__PC',
        'NI__PC',
        'NH__PC',
        'FF__LBHR',
        'ALT__FT',
        'MN1',
        'PS26S__NOM_PSI',
        'TS25S__NOM_K',
        'PS30S__NOM_PSI',
        'TS30S__NOM_K',
        'TGTS__NOM_K',
        'NL__NOM_PC',
        'NI__NOM_PC',
        'NH__NOM_PC',
        'FF__NOM_LBHR',
        'P135S__NOM_PSI'
    ]
    # Drop rows with any NaN
    df.dropna(subset=Param, inplace=True)
    # Replace -5555 with NaN so we can treat both uniformly
    df = df.replace(-5555, np.nan).dropna(subset=Param)
    return df


def filter_parameters(df: pd.DataFrame, flight_phase: str) -> pd.DataFrame:
    """
    Filters input parameters extracted from SQL queries applying upper and lower limits flight phase specific.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with data to be filtered.
    flight_phase : str
        Input string containing flight phase.

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame
    """
    # Define parameters and limits
    Param = [
        'P25__PSI', 'T25__DEGC', 'P30__PSI', 'T30__DEGC', 'TGTU_A__DEGC',
        'NL__PC', 'NI__PC', 'NH__PC', 'FF__LBHR', 'ALT__FT', 'MN1'
    ]

    rows_names = [
        'Take-off low limits', 'Take-off high limits',
        'Climb low limits', 'Climb high limits',
        'Cruise low limits', 'Cruise high limits'
    ]

    lims = [
        [50, 200, 400, 500, 650, 60, 75, 75, 10000, -1000, 0.175],
        [200, 400, 800, 800, 1000, 120, 120, 120, 25000, 9000, 0.320],
        [50, 200, 250, 450, 650, 60, 75, 75, 8000, 15000, 0.500],
        [100, 350, 500, 700, 900, 120, 120, 120, 16000, 25000, 0.770],
        [25, 170, 130, 330, 380, 60, 75, 75, 3000, 25000, 0.700],
        [60, 280, 270, 590, 900, 120, 120, 120, 8500, 43000, 0.880]
    ]

    limits_DSCs = pd.DataFrame(lims, columns=Param, index=rows_names)

    # Normalize flight_phase for matching
    flight_phase = flight_phase.lower()
    matching_rows = [
        row for row in limits_DSCs.index if row.lower().startswith(flight_phase)]

    if not matching_rows or len(matching_rows) != 2:
        raise ValueError(
            f"No valid limits found for flight phase: {flight_phase}")

    limits_flight_phase = limits_DSCs.loc[matching_rows]

    # Apply filtering
    for parameter in limits_flight_phase.columns:
        try:
            lower_limit, upper_limit = limits_flight_phase[parameter].tolist()
        except ValueError:
            raise ValueError(
                f"Expected two limits for parameter '{parameter}', got {limits_flight_phase[parameter].tolist()}")

        if parameter in df.columns:
            df = df[(df[parameter] >= lower_limit) &
                    (df[parameter] <= upper_limit)]

    return df
