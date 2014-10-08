from enum import Enum


class SortOrder(Enum):
    ASC = 'asc'
    DESC = 'desc'


class Sort(object):
    """A class to represent a sort statement"""
    def __init__(self, order, field):
        """Constructor
        :param order: the sort order
        :type order: SortOrder
        :param field: the field concern by the sort
        :type field: str
        """
        self.order = order
        self.field = field

    def to_dict(self):
        """Return a dict representation of the object
        :returns: dict"""
        return {self.order.value: self.field}


class AscendingSort(Sort):
    """A class to represent an ascending sort statement"""
    def __init__(self, field):
        super(self.__class__, self).__init__(SortOrder.ASC, field)


class DescendingSort(Sort):
    """A class to represent a descending sort statement"""
    def __init__(self, field):
        super(self.__class__, self).__init__(SortOrder.DESC, field)
