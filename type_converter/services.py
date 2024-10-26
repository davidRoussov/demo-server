import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor

def infer_column(df, col):
    try:
        # Attempt to convert to numeric first
        df_converted = pd.to_numeric(df[col], errors='coerce')
        if not df_converted.isna().all():
            return col, df_converted

        # Attempt to convert to complex numbers
        df_converted = df[col].apply(lambda x: complex(x.replace(' ', '') if isinstance(x, str) else np.nan))
        if not df_converted.apply(np.isnan).all():
            return col, df_converted

        # Attempt to convert to datetime
        df_converted = pd.to_datetime(df[col], errors='coerce')
        if not df_converted.isna().all():
            return col, df_converted

        # Check if the column should be categorical
        if len(df[col].unique()) / len(df[col]) < 0.5:
            return col, pd.Categorical(df[col])

    except (ValueError, TypeError):
        pass

    return col, df[col]

def infer_and_convert_data_types(dataset):
    df = dataset.dataframe

    with ThreadPoolExecutor() as executor:
        future_to_col = {executor.submit(infer_column, df, col): col for col in df.columns}
        for future in future_to_col:
            col, converted_col = future.result()
            df[col] = converted_col

    print("\nData types after inference:")
    print(df.dtypes)
