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

def infer_numeric_series(series):
    df_converted = pd.to_numeric(series, errors='coerce')
    if not df_converted.isna().all():
        max_value = df_converted.max()
        min_value = df_converted.min()

        is_decimal = (df_converted % 1 != 0).any()

        if is_decimal:
            if df_converted.abs().max() <= 3.4e38:
                return df_converted.astype('float32')
            else:
                return df_converted.astype('float64')
        else:
            if min_value >= -128 and max_value <= 127:
                return df_converted.astype('int8')
            elif min_value >= -32768 and max_value <= 32767:
                return df_converted.astype('int16')
            elif min_value >= -2147483648 and max_value <= 2147483647:
                return df_converted.astype('int32')
            elif min_value >= -9223372036854775808 and max_value <= 9223372036854775807:
                return df_converted.astype('int64')
            else:
                # If the range is outside standard int64 bounds, use float
                return df_converted.astype('float64')

    return None

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
    logger.debug("Starting type inference for column '%s'", col)

    series = clean_series(df[col])
    logger.debug("Cleaned series for column '%s'", col)

    # Attempt to convert to duration
    df_converted = infer_duration_series(series)
    if df_converted is not None:
        logger.info("Column '%s' inferred as 'duration'", col)
        return col, df_converted.reindex(df.index)

    # Attempt to convert to boolean
    df_converted = infer_boolean_series(series)
    if df_converted is not None:
        logger.info("Column '%s' inferred as 'boolean'", col)
        return col, df_converted.reindex(df.index)

    # Attempt to convert to datetime
    df_converted = infer_datetime_series(series)
    if df_converted is not None:
        logger.info("Column '%s' inferred as 'datetime'", col)
        return col, df_converted.reindex(df.index)

    # Attempt to convert to a numeric series
    df_converted = infer_numeric_series(series)
    if df_converted is not None:
        logger.info("Column '%s' inferred as 'numeric'", col)
        return col, df_converted.reindex(df.index)

    # Attempt to convert to complex numbers
    df_converted = infer_complex_series(series)
    if df_converted is not None:
        logger.info("Column '%s' inferred as 'complex'", col)
        return col, df_converted.reindex(df.index)

    # Check if the column should be categorical
    unique_ratio = len(series.unique()) / len(series)
    if unique_ratio < 0.5:
        logger.info("Column '%s' inferred as 'categorical'", col)
        categorical_series = pd.Series(pd.Categorical(series), index=series.index)
        return col, categorical_series.reindex(df.index)

    logger.info("Column '%s' remains as 'object'", col)
    return col, series.reindex(df.index)

def convert_column_type(series, type_hint):
    if type_hint == "object":
        return series.astype('object')
    elif type_hint == "int64":
        return series.astype('int64')
    elif type_hint == "int32":
        return series.astype('int32')
    elif type_hint == "int16":
        return series.astype('int16')
    elif type_hint == "int8":
        return series.astype('int8')
    elif type_hint == "float64":
        return series.astype('float64')
    elif type_hint == "float32":
        return series.astype('float32')
    elif type_hint == "bool":
        return series.astype('bool')
    elif type_hint == "datetime64[ns]":
        return pd.to_datetime(series, errors='coerce')
    elif type_hint == "timedelta":
        return pd.to_timedelta(series, errors='coerce')
    elif type_hint == "category":
        return series.astype('category')
    elif type_hint == "complex128":
        return series.astype('complex128')
    else:
        logger.info(f"Unexpected type hint: {type_hint}")
        return series

def infer_and_convert_data_types(dataset):
    df = dataset.dataframe

    logger.info("Data types before inference:\n%s", df.dtypes)

    type_hints_dict = {col: dtype for col, dtype in dataset.type_hints}
    logger.debug("Type hints dict: %s", type_hints_dict)

    with ThreadPoolExecutor() as executor:
        future_to_col = {}
        for col in df.columns:
            if col in type_hints_dict:
                logger.debug("Converting column '%s' to '%s'", col, type_hints_dict[col])
                df[col] = convert_column_type(df[col], type_hints_dict[col])
            else:
                logger.debug("Inferring type for column '%s'", col)
                future_to_col[executor.submit(infer_series, df, col)] = col

        for future in future_to_col:
            col, converted_col = future.result()
            logger.debug("Completed inference for column '%s'", col)
            df[col] = converted_col

    logger.info("Data types after inference:\n%s", df.dtypes)

    return df.dtypes

