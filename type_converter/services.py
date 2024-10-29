import warnings
import logging
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import re

logger = logging.getLogger(__name__)

# Any values we know we can exclude from inference
KNOWN_ERRONEOUS_ENTRIES = {'Not Available', 'unknown'}

BOOLEAN_ALIASES_TRUE = {'yes', 't', 'enabled', 'on', 'true'}
BOOLEAN_ALIASES_FALSE = {'no', 'f', 'disabled', 'off', 'false'}

KNOWN_DURATIONS = {
    "second": 1,
    "seconds": 1,
    "minute": 60,
    "minutes": 60,
    "hour": 3600,
    "hours": 3600,
    "day": 86400,
    "days": 86400,
    "week": 604800,
    "weeks": 604800,
    "month": 2592000,
    "months": 2592000,
    "year": 31536000,
    "years": 31536000
}

def clean_series(series):
    return series[~series.isin(KNOWN_ERRONEOUS_ENTRIES)].dropna()

def infer_duration_series(series):
    def to_timedelta(val):
        if isinstance(val, pd.Timedelta):
            return val
        if isinstance(val, str):
            match = re.match(r"(\d+\.?\d*)\s*(\w+)", val.strip().lower())
            if match:
                num, unit = match.groups()
                num = float(num)

                if unit in KNOWN_DURATIONS:
                    total_seconds = num * KNOWN_DURATIONS[unit]
                    try:
                        return pd.to_timedelta(total_seconds, unit='seconds')
                    except (ValueError, OverflowError):
                        pass
        return pd.NaT

    try:
        series_converted = series.apply(to_timedelta)

        if not series_converted.isna().all():
            return series_converted
    except Exception:
        pass

    return None

def infer_complex_series(series):
    try:
        df_converted = series.apply(lambda x: complex(x.replace(' ', '') if isinstance(x, str) else np.nan))
        if not df_converted.apply(np.isnan).all():
            return df_converted
    except Exception:
        pass

    return None

def infer_datetime_series(series):
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

def infer_boolean_series(series):
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

def infer_series(df, col):
    series = clean_series(df[col])

    # Attempt to convert to duration
    df_converted = infer_duration_series(series)
    if df_converted is not None:
        return col, df_converted.reindex(df.index)

    # Attempt to convert to boolean
    df_converted = infer_boolean_series(series)
    if df_converted is not None:
        return col, df_converted.reindex(df.index)

    # Attempt to convert to datetime
    df_converted = infer_datetime_series(series)
    if df_converted is not None:
        return col, df_converted.reindex(df.index)

    # Attempt to convert to numeric first
    df_converted = pd.to_numeric(series, errors='coerce')
    if not df_converted.isna().all():
        return col, df_converted.reindex(df.index)

    # Attempt to convert to complex numbers
    df_converted = infer_complex_series(series)
    if df_converted is not None:
        return col, df_converted.reindex(df.index)

    # Check if the column should be categorical
    if len(series.unique()) / len(series) < 0.5:
        categorical_series = pd.Series(pd.Categorical(series), index=series.index)
        return col, categorical_series.reindex(df.index)

    return col, series.reindex(df.index)

def convert_column_type(series, type_hint):
    match type_hint:
        case "object":
            return series.astype('object')
        case "int64":
            return series.astype('int64')
        case "int32":
            return series.astype('int32')
        case "int16":
            return series.astype('int16')
        case "int8":
            return series.astype('int8')
        case "float64":
            return series.astype('float64')
        case "float32":
            return series.astype('float32')
        case "bool":
            return series.astype('bool')
        case "datetime64[ns]":
            return pd.to_datetime(series, errors='coerce')
        case "timedelta":
            return pd.to_timedelta(series, errors='coerce')
        case "category":
            return series.astype('category')
        case "complex128":
            return series.astype('complex128')
        case _:
            logger.info(f"Unexpected type hint: {type_hint}")
            return series

def infer_and_convert_data_types(dataset):
    df = dataset.dataframe

    print("\nData types before inference:")
    print(df.dtypes)

    type_hints_dict = {col: dtype for col, dtype in dataset.type_hints}
    print(f"type_hints_dict: {type_hints_dict}")

    with ThreadPoolExecutor() as executor:
        future_to_col = {}
        for col in df.columns:
            if col in type_hints_dict:
                df[col] = convert_column_type(df[col], type_hints_dict[col])
            else:
                future_to_col[executor.submit(infer_series, df, col)] = col

        for future in future_to_col:
            col, converted_col = future.result()
            df[col] = converted_col

    print("\nData types after inference:")
    print(df.dtypes)

    return df.dtypes
