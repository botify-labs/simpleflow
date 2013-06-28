from cdf.collections.properties_stats.constants import CROSS_PROPERTIES_COLUMNS


class CounterRequest(object):

    def __init__(self, df):
        self.df = df

    def df_from_filters(self, filters):
        df = self.df.copy()
        for key, value in filters.iteritems():
            if callable(value):
                df = df[value(df[key])]
            else:
                df = df[df[key] == value]
        return df

    """
    Return the total sum from a list of `fields`

    :param fields : a list of fields
    :param filters : a dictionnary where key is in DISTRIBUTION_COLUMNS and value is a string or a function to apply on this dataframe's field

    Return a dict with the `field` as key and sum as value
    """
    def fields_sum(self, fields, filters=None):
        if filters and any(f not in self.DISTRIBUTION_COLUMNS for f in filters.keys()):
            raise Exception('One of the submitted filters is not allowed')
        df = self.df_from_filters(filters) if filters else self.df
        return {f: int(df[f].sum()) for f in fields}

    """
    Return the total sum from a list of `fields` aggregated by cross-property

    :param fields : a list of files
    :param filters : a dictionnary where key in DISTRIBUTION_COLUMNS and value is a string or a function to apply on this dataframe's field
    :params merge : if None, return all properties combinations possible. If a tuple of properties, return all combination matching those fields

    Return a list of dictionaries with two keys "properties" and "counters".

    Ex :

    [
        {
            "properties": {
                "host": "www.site.com",
                "content_type": "text/html"
            },
            "counters": {
                "pages_nb": 10,
            }
        },
        {
            "properties": {
                "host": "subdomain.site.com",
                "content_type": "text/html"
            },
            "counters": {
                "pages_nb": 20
            }
        }
    ]
    """
    def fields_sum_by_property(self, fields, filters=None, merge=False):
        results = {}
        if not merge:
            merge = self.DISTRIBUTION_COLUMNS
        df = self.df_from_filters(filters) if filters else self.df
        df = df.groupby(merge).reset_index()
        results = []
        for i, n in enumerate(df.values):
            result = {
                'properties': {field_: df[field_][i] for field_ in merge},
                'counters': {field_: df[field_][i] for field_ in fields}
            }
            results.append(result)
        return results


class PropertiesStatsRequest(CounterRequest):

    DISTRIBUTION_COLUMNS = CROSS_PROPERTIES_COLUMNS

    def __init__(self, df):
        super(PropertiesStatsRequest, self).__init__(df)
