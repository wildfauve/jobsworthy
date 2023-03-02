from dataclasses import dataclass


@dataclass
class DataClassAbstract:
    def replace(self, key, value):
        setattr(self, key, value)
        return self
