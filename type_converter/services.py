import warnings
import logging
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

# Any values we know we can exclude from inference
KNOWN_ERRONEOUS_ENTRIES = {'Not Available', 'unknown'}

BOOLEAN_ALIASES_TRUE = {'yes', 't', 'enabled', 'on', 'true'}
BOOLEAN_ALIASES_FALSE = {'no', 'f', 'disabled', 'off', 'false'}

def clean_series(series):
    return series[~series.isin(KNOWN_ERRONEOUS_ENTRIES)].dropna()

def infer_complex_column(series):
    try:
        df_converted = series.apply(lambda x: complex(x.replace(' ', '') if isinstance(x, str) else np.nan))
        if not df_converted.apply(np.isnan).all():
            return df_converted
    except Exception:
        pass

    return None

def infer_datetime_column(series):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)

        # Heuristic for determining whether an arbitrary numeric value actually represents a timestamp:
        # Ten-digit positive integers represent epoch seconds as it would represent time from 2001 to 2286
        # Similarly, 13-digit integers represent milliseconds since epoch
        numeric_series = pd.to_numeric(series, errors='coerce')
        clean_series = numeric_series.dropna()
        if len(clean_series) > 0:
            if (clean_series == clean_series.astype(int)).all() and (clean_series > 0).all():
                string_series = clean_series.astype(str)
                if all(string_series.str.replace('.0', '').str.len() == 13):
                    logger.info("Detected epoch milliseconds series")
                    return pd.to_datetime(clean_series, unit='ms', errors='coerce')
                elif all(string_series.str.replace('.0', '').str.len() == 10):
                    logger.info("Detected epoch seconds series")
                    return pd.to_datetime(clean_series, unit='s', errors='coerce')
                else:
                    return None
            else:
                return None

        try:
            df_converted = pd.to_datetime(series, errors='coerce')

            if not df_converted.isna().all():
                return df_converted
        except Exception as e:
            pass

    return None

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
    
    try:
        df_converted = series.apply(to_bool)

        if not df_converted.isna().all():
            return df_converted.astype(pd.BooleanDtype())
    except Exception:
        pass

    return None

def infer_column(df, col):
    series = clean_series(df[col])

    # Attempt to convert to boolean
    df_converted = infer_boolean_column(series)
    if df_converted is not None:
        return col, df_converted.reindex(df.index)

    # Attempt to convert to datetime
    df_converted = infer_datetime_column(series)
    if df_converted is not None:
        return col, df_converted.reindex(df.index)

    # Attempt to convert to numeric first
    df_converted = pd.to_numeric(series, errors='coerce')
    if not df_converted.isna().all():
        return col, df_converted.reindex(df.index)

    # Attempt to convert to complex numbers
    df_converted = infer_complex_column(series)
    if df_converted is not None:
        return col, df_converted.reindex(df.index)

    # Check if the column should be categorical
    if len(series.unique()) / len(series) < 0.5:
        categorical_series = pd.Series(pd.Categorical(series), index=series.index)
        return col, categorical_series.reindex(df.index)

    return col, series.reindex(df.index)

def infer_and_convert_data_types(dataset):
    df = dataset.dataframe

    print("\nData types before inference:")
    print(df.dtypes)

    with ThreadPoolExecutor() as executor:
        future_to_col = {executor.submit(infer_column, df, col): col for col in df.columns}
        for future in future_to_col:
            col, converted_col = future.result()
            df[col] = converted_col

    print("\nData types after inference:")
    print(df.dtypes)

    return df.dtypes
