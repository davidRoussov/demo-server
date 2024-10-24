class Dataset:
    def __init__(self, name, content):
        self.name = name
        self.content = content

    def size(self):
        return len(self.content)
