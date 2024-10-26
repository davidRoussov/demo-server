import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor

# Any values we know we can exclude from inference
KNOWN_ERRONEOUS_ENTRIES = {'Not Available', 'unknown'}

BOOLEAN_ALIASES_TRUE = {'yes', 't', 'enabled', 'on'}
BOOLEAN_ALIASES_FALSE = {'no', 'f', 'disabled', 'off'}

def clean_series(series):
    return series[~series.isin(KNOWN_ERRONEOUS_ENTRIES)].dropna()

def infer_boolean_column(series):
    def to_bool(val):
        if isinstance(val, bool):
            return val
        if isinstance(val, str):
            val_lower = val.lower()
            if val_lower in BOOLEAN_ALIASES_TRUE:
                return True
            elif val_lower in BOOLEAN_ALIASES_FALSE:
                return False
        if isinstance(val, (int, float)) and val in [0, 1]:
            return bool(val)
        return val
    
    df_converted = series.apply(to_bool)

    if not df_converted.isna().all():
        return df_converted.astype(pd.BooleanDtype())

    return None

def infer_column(df, col):
    series = clean_series(df[col])

    try:
        # Attempt to convert to boolean
        df_converted = infer_boolean_column(series)
        if df_converted is not None:
            return col, df_converted

        # Attempt to convert to numeric first
        df_converted = pd.to_numeric(series, errors='coerce')
        if not df_converted.isna().all():
            return col, df_converted

        # Attempt to convert to complex numbers
        df_converted = series.apply(lambda x: complex(x.replace(' ', '') if isinstance(x, str) else np.nan))
        if not df_converted.apply(np.isnan).all():
            return col, df_converted

        # Attempt to convert to datetime
        df_converted = pd.to_datetime(series, errors='coerce')
        if not df_converted.isna().all():
            return col, df_converted

        # Check if the column should be categorical
        if len(series.unique()) / len(series) < 0.5:
            return col, pd.Categorical(series)

    except (ValueError, TypeError):
        pass

    return col, series

def infer_and_convert_data_types(dataset):
    df = dataset.dataframe

    with ThreadPoolExecutor() as executor:
        future_to_col = {executor.submit(infer_column, df, col): col for col in df.columns}
        for future in future_to_col:
            col, converted_col = future.result()
            df[col] = converted_col

    print("\nData types after inference:")
    print(df.dtypes)

    return df.dtypes
