class Dataset:
    def __init__(self, name, dataframe):
        self.name = name
        self.dataframe = dataframe

    def size(self):
        return self.dataframe.shape
