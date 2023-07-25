from __future__ import annotations


class Marker:
    def __init__(self, name, details):
        self.name = name
        self.details = details

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.name!r} details={self.details!r}>"
