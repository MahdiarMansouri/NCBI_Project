import json


class Gene:
    def __init__(self, id, name, file_path):
        self.id = id
        self.name = name
        self.file_path = file_path

    def __repr__(self):
        return json.dumps(self.__dict__)
