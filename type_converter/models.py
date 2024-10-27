class Dataset:
    def __init__(self, name, dataframe, type_hints):
        self.name = name
        self.dataframe = dataframe
        self.type_hints = type_hints if type_hints is not None else []

    def size(self):
        return self.dataframe.shape
