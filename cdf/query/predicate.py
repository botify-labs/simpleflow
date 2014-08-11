class Predicate(object):
    """A class to represent a predicate in a query.
    The use of a dedicated class
    - saves code
    - document what are the expected fields
    - document the query language
    For instance instead of writing
    {
        "field": "http_code",
        "predicate": "eq",
        "value": 200
    }
    to build a query
    you can now write
      Predicate("http_code", "eq", 200).to_dict()
    Thanks to the class you know that a field, an operator and a value are
    expected.
    And you don't have to remember that the expected key for the field is "field".
    """

    def __init__(self, field, operator, value):
        """Constructor
        :param field: the field concerned by the predicate
        :type field: str
        :param operator: the predicate operate (for instance: "eq", "lt", "between")
        :type operator: str
        :param value: the value associated with the operator
                      for instance 300 if predicate is "eq"
                      or [300, 399] if predicate is "between"
        :type value: object
        """
        self.field = field
        self.operator = operator
        self.value = value

    def to_dict(self):
        """Return a dict version of the predicate.
        Usually, this representation is used to build filters for queries.
        :returns: dict
        """
        return {
            "field": self.field,
            "predicate": self.operator,
            "value": self.value
        }
