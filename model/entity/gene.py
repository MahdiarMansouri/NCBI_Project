import json


class Gene:
    def __init__(self, id, name, file_path, file_name):
        self.id = id
        self.name = name
        self.file_path = file_path
        self.file_name = file_name

    def __repr__(self):
        return json.dumps(self.__dict__)
