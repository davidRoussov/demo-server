import pandas
from django.test import TestCase
from type_converter.services import infer_and_convert_data_types
from type_converter.models import Dataset

class InferenceTests(TestCase):
    def test_floats(self):
        dataframe = pandas.read_csv('./datasets/sample_floats.csv')
        dataset = Dataset(name="sample_data", dataframe=dataframe, type_hints=[])
        dtypes = infer_and_convert_data_types(dataset)
        self.assertEqual(dtypes['float64_col'], 'float64')
        self.assertEqual(dtypes['float32_col'], 'float32')
        self.assertEqual(dtypes['float64_col_2'], 'float64')
        self.assertEqual(dtypes['float32_col_2'], 'float32')
    def test_booleans(self):
        dataframe = pandas.read_csv('./datasets/sample_booleans.csv')
        dataset = Dataset(name="sample_data", dataframe=dataframe, type_hints=[])
        dtypes = infer_and_convert_data_types(dataset)
        self.assertEqual(dtypes['bool_col_tf'], 'boolean')
        self.assertEqual(dtypes['bool_col_numeric'], 'boolean')
        self.assertEqual(dtypes['bool_col_yesno'], 'boolean')
        self.assertEqual(dtypes['bool_col_onoff'], 'boolean')
        self.assertEqual(dtypes['bool_col_tf_short'], 'boolean')
        self.assertEqual(dtypes['bool_col_enable'], 'boolean')
    def test_dates(self):
        dataframe = pandas.read_csv('./datasets/sample_dates.csv')
        dataset = Dataset(name="sample_data", dataframe=dataframe, type_hints=[])
        dtypes = infer_and_convert_data_types(dataset)
        self.assertEqual(dtypes['date_col_ymd'], 'datetime64[ns]')
        self.assertEqual(dtypes['date_col_dmy'], 'datetime64[ns]')
        self.assertEqual(dtypes['date_col_mdy'], 'datetime64[ns]')
        self.assertEqual(dtypes['date_col_iso'], 'datetime64[ns]')
        self.assertEqual(dtypes['date_col_full'], 'datetime64[ns]')
        self.assertEqual(dtypes['date_col_short'], 'datetime64[ns]')
        self.assertEqual(dtypes['epoch_seconds'], 'datetime64[ns]')
        self.assertEqual(dtypes['epoch_milliseconds'], 'datetime64[ns]')
    def test_categorical(self):
        dataframe = pandas.read_csv('./datasets/sample_categorical.csv')
        dataset = Dataset(name="sample_data", dataframe=dataframe, type_hints=[])
        infer_and_convert_data_types(dataset)
    def test_complex(self):
        dataframe = pandas.read_csv('./datasets/sample_complex.csv')
        dataset = Dataset(name="sample_data", dataframe=dataframe, type_hints=[])
        infer_and_convert_data_types(dataset)
    def test_duration(self):
        dataframe = pandas.read_csv('./datasets/sample_durations.csv')
        dataset = Dataset(name="sample_data", dataframe=dataframe, type_hints=[])
        dtypes = infer_and_convert_data_types(dataset)
        self.assertEqual(dtypes['duration_seconds'], 'timedelta64[ns]')
        self.assertEqual(dtypes['duration_minutes'], 'timedelta64[ns]')
        self.assertEqual(dtypes['duration_hours'], 'timedelta64[ns]')
        self.assertEqual(dtypes['duration_days'], 'timedelta64[ns]')
        self.assertEqual(dtypes['duration_weeks'], 'timedelta64[ns]')
        self.assertEqual(dtypes['duration_months'], 'timedelta64[ns]')
        self.assertEqual(dtypes['duration_years'], 'timedelta64[ns]')
        self.assertEqual(dtypes['combined_duration'], 'timedelta64[ns]')
    def test_integers(self):
        dataframe = pandas.read_csv('./datasets/sample_integers.csv')
        dataset = Dataset(name="sample_data", dataframe=dataframe, type_hints=[])
        dtypes = infer_and_convert_data_types(dataset)
        self.assertEqual(dtypes['int64_col'], 'int64')
        self.assertEqual(dtypes['int32_col'], 'int32')
        self.assertEqual(dtypes['int16_col'], 'int16')
        self.assertEqual(dtypes['int8_col'], 'int8')
