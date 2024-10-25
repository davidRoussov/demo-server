import pandas as pd
import numpy as np

class Dataset:
    def __init__(self, name, dataframe):
        self.name = name
        self.dataframe = dataframe

    def size(self):
        return self.dataframe.shape

    def infer_and_convert_data_types(self):
        df = self.dataframe

        for col in df.columns:
            # Attempt to convert to numeric first
            df_converted = pd.to_numeric(df[col], errors='coerce')
            if not df_converted.isna().all():
                df[col] = df_converted
                continue

            # Attempt to convert to complex numbers
            try:
                df_converted = df[col].apply(lambda x: complex(x.replace(' ', '') if isinstance(x, str) else np.nan))
                if not df_converted.apply(np.isnan).all():
                    df[col] = df_converted
                    continue
            except (ValueError, TypeError):
                pass

            # Attempt to convert to datetime
            try:
                df[col] = pd.to_datetime(df[col])
                continue
            except (ValueError, TypeError):
                pass

            # Check if the column should be categorical
            if len(df[col].unique()) / len(df[col]) < 0.5:
                df[col] = pd.Categorical(df[col])

        print("\nData types after inference:")
        print(df.dtypes)
