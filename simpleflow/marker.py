from __future__ import annotations


class Marker:
    def __init__(self, name, details):
        self.name = name
        self.details = details

    def __repr__(self):
        return "<{klass} {name!r} details={details!r}>".format(
            klass=self.__class__.__name__,
            name=self.name,
            details=self.details,
        )
