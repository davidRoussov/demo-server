import pandas
from django.test import TestCase
from type_converter.services import infer_and_convert_data_types
from type_converter.models import Dataset

class InferenceTests(TestCase):
    def test_integers(self):
        dataframe = pandas.read_csv('./datasets/sample_integers.csv')
        dataset = Dataset(name="sample_data", dataframe=dataframe)
        infer_and_convert_data_types(dataset)
    def test_floats(self):
        dataframe = pandas.read_csv('./datasets/sample_floats.csv')
        dataset = Dataset(name="sample_data", dataframe=dataframe)
        infer_and_convert_data_types(dataset)
    def test_booleans(self):
        dataframe = pandas.read_csv('./datasets/sample_booleans.csv')
        dataset = Dataset(name="sample_data", dataframe=dataframe)
        infer_and_convert_data_types(dataset)
    def test_dates(self):
        dataframe = pandas.read_csv('./datasets/sample_dates.csv')
        dataset = Dataset(name="sample_data", dataframe=dataframe)
        infer_and_convert_data_types(dataset)
    def test_categorical(self):
        dataframe = pandas.read_csv('./datasets/sample_categorical.csv')
        dataset = Dataset(name="sample_data", dataframe=dataframe)
        infer_and_convert_data_types(dataset)
    def test_complex(self):
        dataframe = pandas.read_csv('./datasets/sample_complex.csv')
        dataset = Dataset(name="sample_data", dataframe=dataframe)
        infer_and_convert_data_types(dataset)
