import pandas as pd

def filter_top_n_by_esn(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    df = df.copy()

    # Parse datetime safely
    df['reportdatetime'] = pd.to_datetime(
        df['reportdatetime'],
        format="%Y-%m-%d %H:%M:%S",
        errors='coerce'
    )

    # Drop rows with NaN in ESN or reportdatetime
    df = df.dropna(subset=['ESN', 'reportdatetime'])

    # Drop exact duplicate rows
    df = df.drop_duplicates()

    # Sort by ESN, then by date (latest first) to pick top_n
    df = df.sort_values(['ESN', 'reportdatetime'], ascending=[True, False])

    # Keep top_n latest rows per ESN
    df = df.groupby('ESN').head(top_n)

    # Final sort: earliest → latest
    df = df.sort_values('reportdatetime', ascending=True).reset_index(drop=True)

    return df
